open Effect
open Effect.Deep

type 'a task = unit -> 'a

module P = Priority

type state =
  Running of P.priority
| Queued of P.priority * work
| Blocked of P.priority
| ChangeTo of P.priority
and task_slot =
  Active of state Atomic.t
| Cancelled
and work = {
    work   : unit -> unit;
    state  : task_slot Atomic.t
  }

type task_state = state Atomic.t



             
type message =
| Work of work
  (* Invariant: the Work function does not need to run under the 'step' handler,
     it installs its own handler or re-invokes a deep-handler continuation. *)
| Quit



type deque_pools = message Dpool.t array

type pool_data = {
  domains      : unit Domain.t array;
  deque_pools  : deque_pools;
  name         : string option;
  current_prio : P.priority array;
  current_task : task_state option array;
  dls          : int Domain.DLS.key;
  work_tracker : P.work_tracker;
  io_waiting   : (unit -> bool) list Atomic.t array
}

type pool = pool_data option Atomic.t

type 'a promise_state =
  Returned of 'a
| Raised of exn * Printexc.raw_backtrace
| Pending of (('a, unit) continuation * task_state * pool_data) list

type 'a promise =
  { promise : 'a promise_state Atomic.t;
    state   : task_state
  }

type _ t += Wait : 'a promise * task_state * pool_data -> 'a t
type _ t += Io : (unit -> 'a option) * task_state * pool_data -> 'a t
type _ t += Yield : task_state * pool_data -> unit t
type _ t += Suspend : (('a, unit) continuation -> unit) -> 'a t

let next_id = Atomic.make 0

let make_dls () =
  Domain.DLS.new_key (fun () -> Atomic.fetch_and_add next_id 1)

let my_id pd = Domain.DLS.get pd.dls

let get_pool_data p =
  match Atomic.get p with
  | None -> invalid_arg "pool already torn down"
  | Some p -> p

let my_prio pd =
  let id = my_id pd in
  pd.current_prio.(id)

let set_my_prio pd p =
  let id = my_id pd in
  pd.current_prio.(id) <- p

(*
let queued_ts () =
  let new_slot = Atomic.make Cancelled in
  Atomic.make (Queued new_slot)
 *)
  
let make_queued p f =
  let new_slot = Atomic.make Cancelled in
  let work =
    { work = f;
      state = new_slot
    }
  in
  Atomic.set new_slot (Active (Atomic.make (Queued (p, work))));
  Work work

let make_queued_with_promise r p f =
  let new_slot = Atomic.make Cancelled in
  let ts = Atomic.make (Running r) in
  let promise = { promise = p; state = ts } in
  let work =
    { work = (fun _ -> f promise);
      state = new_slot
    }
  in
  Atomic.set ts (Queued (r, work));
  Atomic.set new_slot (Active ts);
  (Work work, promise)

  (*
let make_running f =
  Work { work = f;
         state = Atomic.make (Active (Atomic.make Running))
    }
   *)

let make_work_with ts f =
  { work = f;
    state = Atomic.make (Active ts)
  }
let make_with ts f =
  Work (make_work_with ts f)

let prio_of_ts ts =
  match Atomic.get ts with
  | Running p | Queued (p, _) | ChangeTo p | Blocked p -> p
  
let rec try_to_queue work ts =
  let old = Atomic.get ts in
  match old with
  | ChangeTo p' ->
     if Atomic.compare_and_set ts old (Queued (p', work)) then p'
     else try_to_queue work ts
  | Running p | Queued (p, _) | Blocked p ->
     if Atomic.compare_and_set ts old (Queued (p, work)) then p
     else try_to_queue work ts

let rec try_to_block ts =
  let old = Atomic.get ts in
  match old with
  | ChangeTo p' ->
     if Atomic.compare_and_set ts old (Blocked p') then p'
     else try_to_block ts
  | Running p | Queued (p, _) | Blocked p ->
     if Atomic.compare_and_set ts old (Blocked p) then p
     else try_to_block ts
  
let cont v (k, ts, pd) =
  let work = make_work_with ts (fun _ -> continue k v) in
  let p = try_to_queue work ts in
  P.set_work pd.work_tracker p;
  Dpool.push_local pd.deque_pools.(P.toInt p) (my_id pd) (Work work)
let discont e bt (k, ts, pd) =
  let work = make_work_with ts (fun _ -> discontinue_with_backtrace k e bt) in
  let p = try_to_queue work ts in
  P.set_work pd.work_tracker p;
  Dpool.push_local pd.deque_pools.(P.toInt p) (my_id pd) (Work work)

exception TaskCancelled
exception MustChange of P.priority
  
let rec set_task_state slot new_state =
  let old_slot = Atomic.get slot in
  match old_slot with
  | Active ts ->
     (let old = Atomic.get ts in
      match (old, new_state) with
      | (Running _, _) | (Queued _, _) | (Blocked _, _) ->
         (if Atomic.compare_and_set ts old new_state then ts
          else set_task_state slot new_state)
      | (ChangeTo p, Running _) | (ChangeTo p, Queued _)
        | (ChangeTo p, Blocked _) -> raise (MustChange p)
      | (ChangeTo p1, ChangeTo p2) ->
         let new_state = ChangeTo (P.join p1 p2) in
         (if Atomic.compare_and_set ts old new_state then ts
          else set_task_state slot new_state)
     )         
  | Cancelled -> raise TaskCancelled
  
let check_io (pd: pool_data) (proc: int) : unit =
  (* Printf.printf "check_io on %d\n%!" proc; *)
  Atomic.set
    (Array.unsafe_get pd.io_waiting proc)
    (
      List.fold_left
        (fun l f -> if f () then l else f::l)
        []
        (Atomic.get (Array.unsafe_get pd.io_waiting proc))
    )

let do_task (type a) (f : unit -> a) (p : a promise) : unit =
  let action, result =
    try
      let v = f () in
      cont v, Returned v
    with e ->
      let bt = Printexc.get_raw_backtrace () in
      discont e bt, Raised (e, bt)
  in
  match Atomic.exchange p.promise result with
  | Pending l -> List.iter action l
  |  _ -> failwith "Task.do_task: impossible, can only set result of task once"

let await pool promise =
  let pd = get_pool_data pool in
  let proc = my_id pd in
  let ts =
    match pd.current_task.(proc) with
    | None -> failwith "Task.await: impossible, should have a running task"
    | Some ts -> ts
  in
  match Atomic.get promise.promise with
  | Returned v -> v
  | Raised (e, bt) -> Printexc.raise_with_backtrace e bt
  | Pending _ -> perform (Wait (promise, ts, pd))

let poll _ promise =
  match Atomic.get promise.promise with
  | Returned v -> Some v
  | Raised (e, bt) -> Printexc.raise_with_backtrace e bt
  | Pending _ -> None

let input_line pool c =
  let pd = get_pool_data pool in
  let proc = my_id pd in
  let ts =
    match pd.current_task.(proc) with
    | None -> failwith "Task.await: impossible, should have a running task"
    | Some ts -> ts
  in
  let poll () =
    match Unix.select [ (Unix.descr_of_in_channel c) ] [] [] 0.01 with
    | ([], [], []) -> None
    | _ -> Some (input_line c)
  in
  perform (Io (poll, ts, pd))

let handle_io
      (pd: pool_data)
      (k: ('a, _) continuation)
      (poll: unit -> 'a option)
      (p: P.priority)
      (ts: task_state)
      () =
  (* Printf.printf "Handle io\n%!"; *)
  match poll () with
  | Some res ->
     P.set_work pd.work_tracker p;
     Dpool.push_local
       pd.deque_pools.(P.toInt p)
       (my_id pd)
       (make_with ts (fun _ -> continue k res))
    ; true
  | None -> false
          
let step (type a) (f : a -> unit) (v : a) : unit =
  try_with f v
  { effc = fun (type a) (e : a t) ->
      match e with
      | Wait (p, ts, c) -> Some (fun (k : (a, _) continuation) ->
          let rec loop () =
            let old = Atomic.get p.promise in
            let _ = try_to_block ts in
            match old with
            | Pending l ->
                if Atomic.compare_and_set p.promise old (Pending ((k,ts,c)::l)) then ()
                else (Domain.cpu_relax (); loop ())
            | Returned v -> continue k v
            | Raised (e,bt) -> discontinue_with_backtrace k e bt
          in
          loop ())
      | Io (poll, ts, pd) -> Some (fun (k : (a, _) continuation) ->
          let rec loop () =
            match poll () with
            | Some v -> continue k v
            | None ->
               let iow = Array.unsafe_get pd.io_waiting (my_id pd) in
               let old = Atomic.get iow in
               let r = try_to_block ts in
               let handler = handle_io pd k poll r ts in
               if Atomic.compare_and_set iow old (handler::old) then ()
               else (Domain.cpu_relax (); loop ())
          in loop ())
      | Yield (ts, pd) -> Some (fun (k : (a, _) continuation) ->
                                 let work = make_work_with ts (fun _ -> continue k ())
                                 in
                                 let p = try_to_queue work ts in
                                 P.set_work pd.work_tracker p;
                                 Dpool.push_local
                                   pd.deque_pools.(P.toInt p)
                                   (my_id pd)
                                   (Work work)
                               )
      | Suspend f -> Some (fun (k : (a, _) continuation) -> f k)
      | _ -> None }

let async pool ?(prio=(my_prio (get_pool_data pool))) f =
  let pd = get_pool_data pool in
  let p = Atomic.make (Pending []) in
  let (work, p) = make_queued_with_promise prio p (fun p -> step (do_task f) p) in
  P.set_work pd.work_tracker prio;
  Dpool.push_local pd.deque_pools.(P.toInt prio) (my_id pd) work;
  p

let prepare_for_await pd () =
  let promise = Atomic.make (Pending []) in
  let release () =
    match Atomic.get promise with
    | (Returned _ | Raised _) -> ()
    | Pending _ ->
      match Atomic.exchange promise (Returned ()) with
      | Pending ks ->
        ks
        |> List.iter @@ fun (k, ts, pd) ->
                        (let work = make_work_with ts (fun _ -> continue k ())
                         in
                         let r = try_to_queue work ts in
                          P.set_work pd.work_tracker r;
                         Dpool.push_global pd.deque_pools.(P.toInt r) (Work work))
      | _ -> ()
  and await () =
    let proc = my_id pd in
    let ts =
      match pd.current_task.(proc) with
      | None -> failwith "Task.await: impossible, should have a running task"
      | Some ts -> ts
    in
    match Atomic.get promise with
    | (Returned _ | Raised _) -> ()
    | Pending _ -> perform (Wait ({promise = promise; state = ts}, ts, pd))
  in
  Domain_local_await.{ release; await }

let rec worker pd =
  let _ = check_io pd (my_id pd) in
  let prio = P.highest_with_work pd.work_tracker in
  (*
  let _ = Printf.printf "%d (w) looking at %d\n%!" (my_id pd) (P.toInt prio)
  in
   *)
  try
    match Dpool.pop pd.deque_pools.(P.toInt prio) (my_id pd)
    with
    | Quit -> ()
    | Work w ->
       (if P.plt (my_prio pd) prio then
          begin
            set_my_prio pd prio;
            Dpool.push_deque_to_mug
              pd.deque_pools.(P.toInt (my_prio pd))
              (my_id pd)
          end
       );
       (try
          pd.current_task.(my_id pd) <- Some (set_task_state w.state (Running prio))
        with TaskCancelled -> worker pd
           | MustChange newp ->
              P.set_work pd.work_tracker newp;
              Dpool.push_local pd.deque_pools.(P.toInt newp) (my_id pd)
                (make_queued prio w.work);
              worker pd
       );
       w.work ();
       pd.current_task.(my_id pd) <- None;
       worker pd
  with Exit ->
    (P.clear_work pd.work_tracker prio;
     Domain.cpu_relax ();
     worker pd)

let worker pd =
  Domain_local_await.using
    ~prepare_for_await:(prepare_for_await pd)
    ~while_running:(fun () -> worker pd)

let run (type a) pool (f : unit -> a) : a =
  let pd = get_pool_data pool in
  let p = Atomic.make (Pending []) in
  let ts = Atomic.make (Running P.bot) in
  pd.current_task.(my_id pd) <- Some ts;
  step (fun _ -> do_task f {promise = p; state = ts}) ();
  let rec loop () : a =
    let _ = check_io pd (my_id pd) in
    match Atomic.get p with
    | Pending _ ->
       begin
         let prio = P.highest_with_work pd.work_tracker in
         (*
         let _ = Printf.printf "%d (r) looking at %d\n%!" (my_id pd) (P.toInt prio)
         in
          *)
         try 
           match Dpool.pop pd.deque_pools.(P.toInt prio) (my_id pd)
           with
           | Work w ->
              (if P.plt (my_prio pd) prio then
                 begin
                   set_my_prio pd prio;
                   Dpool.push_deque_to_mug
                     pd.deque_pools.(P.toInt (my_prio pd))
                     (my_id pd)
                 end
              );
              (try
                 pd.current_task.(my_id pd) <- Some (set_task_state w.state (Running prio))
               with MustChange newp ->
                     P.set_work pd.work_tracker newp;
                     Dpool.push_local pd.deque_pools.(P.toInt newp) (my_id pd)
                       (make_queued prio w.work);
                     raise TaskCancelled
              );
              w.work ();
              pd.current_task.(my_id pd) <- None
           | Quit -> failwith "Task.run: tasks are active on pool"
         with Exit ->
           (P.clear_work pd.work_tracker prio;
            Domain.cpu_relax ())
            | TaskCancelled -> ()
       end;
       loop ()
   | Returned v -> v
   | Raised (e, bt) -> Printexc.raise_with_backtrace e bt
  in
  loop ()

let run pool f =
  Domain_local_await.using
    ~prepare_for_await:(prepare_for_await (get_pool_data pool))
    ~while_running:(fun () -> run pool f)

let yield pool =
  let pd = get_pool_data pool in
  let proc = my_id pd in
  let ts =
    match pd.current_task.(proc) with
    | None -> failwith "Task.await: impossible, should have a running task"
    | Some ts -> ts
  in
  perform (Yield (ts, pd))

let change pool ~prio =
  let rec change_ts ts p =
    let old = Atomic.get ts in
    let news =
      match old with
      | ChangeTo p' -> ChangeTo (P.join p p')
      | Running _ -> ChangeTo p
      | Queued _ | Blocked _ -> failwith "Task.change: task shouldn't be queued or blocked"
    in
    if Atomic.compare_and_set ts old news then ()
    else change_ts ts p
  in
  let pd = get_pool_data pool in
  let proc = my_id pd in
  let ts =
    match pd.current_task.(proc) with
    | None -> failwith "Task.await: impossible, should have a running task"
    | Some ts -> ts
  in
  change_ts ts prio;
  perform (Yield (ts, pd))

let named_pools = Hashtbl.create 8
let named_pools_mutex = Mutex.create ()

let rec pre_worker p =
  match Atomic.get p with
  | None -> Domain.cpu_relax (); pre_worker p
  | Some pd -> worker pd

let setup_pool ?name ~num_domains () =
  if num_domains < 0 then
    invalid_arg "Task.setup_pool: num_domains must be at least 0"
  else
  let p = Atomic.make None in
  let deque_pools = Array.init (P.count ())
                      (fun _ -> Dpool.make (num_domains+1))
  in
  let domains = Array.init num_domains (fun _ ->
    Domain.spawn (fun _ -> pre_worker p))
  in
  let current_prio = Array.make (num_domains + 1) P.bot in
  let current_task = Array.make (num_domains + 1) None in
  let dls = make_dls () in
  let work_tracker = P.make_work_tracker () in
  let io_waiting = Array.init (num_domains + 1) (fun _ -> Atomic.make []) in
  let _ =
    Atomic.set
      p
      (Some {domains; deque_pools; name; current_prio; current_task; work_tracker; dls; io_waiting})
  in
  begin match name with
    | None -> ()
    | Some x ->
        Mutex.lock named_pools_mutex;
        Hashtbl.add named_pools x p;
        Mutex.unlock named_pools_mutex
  end;
  p

let teardown_pool pool =
  let pd = get_pool_data pool in
  for _i=1 to Array.length pd.domains do
    for p=0 to P.count () - 1 do
      Dpool.push_global pd.deque_pools.(p) Quit
    done
  done;
  (* Multi_channel.clear_local_state pd.task_chan; *)
  Array.iter Domain.join pd.domains;
  (* Remove the pool from the table *)
  begin match pd.name with
  | None -> ()
  | Some n ->
      Mutex.lock named_pools_mutex;
      Hashtbl.remove named_pools n;
      Mutex.unlock named_pools_mutex
  end;
  Atomic.set pool None

let lookup_pool name =
  Mutex.lock named_pools_mutex;
  let p = Hashtbl.find_opt named_pools name in
  Mutex.unlock named_pools_mutex;
  p

let get_num_domains pool =
  let pd = get_pool_data pool in
  Array.length pd.domains + 1

let parallel_for_reduce ?(chunk_size=0) ~start ~finish ~body pool reduce_fun init =
  let pd = get_pool_data pool in
  let chunk_size = if chunk_size > 0 then chunk_size
      else begin
        let n_domains = (Array.length pd.domains) + 1 in
        let n_tasks = finish - start + 1 in
        if n_domains = 1 then n_tasks
        else max 1 (n_tasks/(8*n_domains))
      end
  in
  let rec work s e =
    if e - s < chunk_size then
      let rec loop i acc =
        if i > e then acc
        else loop (i+1) (reduce_fun acc (body i))
      in
      loop (s+1) (body s)
    else begin
      let d = s + ((e - s) / 2) in
      let p = async pool (fun _ -> work s d) in
      let right = work (d+1) e in
      let left = await pool p in
      reduce_fun left right
    end
  in
  if finish < start
  then init
  else reduce_fun init (work start finish)

let parallel_for ?(chunk_size=0) ~start ~finish ~body pool =
  let pd = get_pool_data pool in
  let chunk_size = if chunk_size > 0 then chunk_size
      else begin
        let n_domains = (Array.length pd.domains) + 1 in
        let n_tasks = finish - start + 1 in
        if n_domains = 1 then n_tasks
        else max 1 (n_tasks/(8*n_domains))
      end
  in
  let rec work pool fn s e =
    if e - s < chunk_size then
      for i = s to e do fn i done
    else begin
      let d = s + ((e - s) / 2) in
      let left = async pool (fun _ -> work pool fn s d) in
      work pool fn (d+1) e;
      await pool left
    end
  in
  work pool body start finish

let parallel_scan pool op elements =
  let pd = get_pool_data pool in
  let n = Array.length elements in
  let p = min (n - 1) ((Array.length pd.domains) + 1) in
  let prefix_s = Array.copy elements in
  let scan_part op elements prefix_sum start finish =
    assert (Array.length elements > (finish - start));
    for i = (start + 1) to finish do
      prefix_sum.(i) <- op prefix_sum.(i - 1) elements.(i)
    done
  in
  if p < 2 then begin
    (* Do a sequential scan when number of domains or array's length is less
    than 2 *)
    scan_part op elements prefix_s 0 (n - 1);
    prefix_s
  end
  else begin
  let add_offset op prefix_sum offset start finish =
    assert (Array.length prefix_sum > (finish - start));
    for i = start to finish do
      prefix_sum.(i) <- op offset prefix_sum.(i)
    done
  in

  parallel_for pool ~chunk_size:1 ~start:0 ~finish:(p - 1)
  ~body:(fun i ->
    let s = (i * n) / (p ) in
    let e = (i + 1) * n / (p ) - 1 in
    scan_part op elements prefix_s s e);

  let x = ref prefix_s.(n/p - 1) in
  for i = 2 to p do
      let ind = i * n / p - 1 in
      x := op !x prefix_s.(ind);
      prefix_s.(ind) <- !x
  done;

  parallel_for pool ~chunk_size:1 ~start:1 ~finish:(p - 1)
  ~body:( fun i ->
    let s = i * n / (p) in
    let e = (i + 1) * n / (p) - 2 in
    let offset = prefix_s.(s - 1) in
      add_offset op prefix_s offset s e
    );

  prefix_s
  end

let parallel_find (type a) ?(chunk_size=0) ~start ~finish ~body pool =
  let pd = get_pool_data pool in
  let found : a option Atomic.t = Atomic.make None in
  let chunk_size = if chunk_size > 0 then chunk_size
      else begin
        let n_domains = (Array.length pd.domains) + 1 in
        let n_tasks = finish - start + 1 in
        if n_domains = 1 then n_tasks
        else max 1 (n_tasks/(8*n_domains))
      end
  in
  let rec work pool fn s e =
    if e - s < chunk_size then
      let i = ref s in
      while !i <= e && Option.is_none (Atomic.get found) do
        begin match fn !i with
          | None -> ()
          | Some _ as some -> Atomic.set found some
        end;
        incr i;
      done
    else if Option.is_some (Atomic.get found) then ()
    else begin
      let d = s + ((e - s) / 2) in
      let left = async pool (fun _ -> work pool fn s d) in
      work pool fn (d+1) e;
      await pool left
    end
  in
  work pool body start finish;
  Atomic.get found

module type MUTEX =
  sig
    type t
    val create : unit -> t
    val lock : pool -> t -> unit
    val unlock: pool -> t -> unit
  end
  
module Mutex : MUTEX =
  struct

    type locked_state =
      { waiting: ((unit, unit) continuation * task_state * pool_data) list;
        prio   : P.priority;
        original_prio : P.priority;
        holder : task_state
      }
    
    type mut_state =
      Locked of locked_state
    | Unlocked

    type t = mut_state Atomic.t

    let create () = Atomic.make Unlocked

                  (*
    let print_lock = Mutex.create ()
    let print f = Mutex.lock print_lock; f (); Mutex.unlock print_lock
                   *)

    let add_to_waiting curr_waiting (k, ts, pd) =
      curr_waiting @ [(k, ts, pd)]
                       (*
      let p = prio_of_ts ts in
      let rec add curr_waiting = 
        (* Add the new task at the last slot such that it's not behind a task
         * of lower priority *)
        match curr_waiting with
        | [] -> [(k, ts, pd)]
        | (_, ts', _)::t ->
           if P.plt (prio_of_ts ts') p then
             (k, ts, pd)::curr_waiting
           else
             add t
      in
      add curr_waiting
                        *)
                  
    let rec lock pool (m: t) =
      let pd = get_pool_data pool in
      let proc = my_id pd in
      let p = pd.current_prio.(proc) in
      let ts =
        match pd.current_task.(proc) with
        | None -> failwith "Task.await: impossible, should have a running task"
        | Some ts -> ts
      in
      let new_state =
        { waiting = [];
          prio = p;
          original_prio = p;
          holder = ts
        }
      in
      match Atomic.get m with
      | Unlocked ->
         
         if Atomic.compare_and_set m Unlocked (Locked new_state)
         then ()
         else (Domain.cpu_relax (); lock pool m)
      | Locked _ as old ->
         perform (Suspend
                    (fun k ->
                      let rec loop old =
                        match old with
                        | Unlocked ->
                           if Atomic.compare_and_set m old (Locked new_state)
                           then
                             (* It's unlocked so we want to just quickly
                              * return, but we're already in the handler
                              * so it's too late to avoid going back to
                              * the scheduler. *)
                             cont () (k, ts, pd)
                           else (Domain.cpu_relax (); loop (Atomic.get m))
                        | Locked l ->
                           let new_state =
                             if P.plt l.prio p then
                               ((
                                 (* Locking would create a priority inversion *)
                                 let old_holder = Atomic.get l.holder in
                                 match old_holder with
                                 | Running _ ->
                                    if Atomic.compare_and_set l.holder old_holder (ChangeTo p) then
                                      ()
                                    else (Domain.cpu_relax (); loop (Atomic.get m))
                                 | Blocked _ ->
                                    if Atomic.compare_and_set l.holder old_holder (Blocked p) then
                                      ()
                                    else (Domain.cpu_relax (); loop (Atomic.get m))
                                 | Queued (_, work) ->
                                    (Atomic.set work.state Cancelled;
                                     P.set_work pd.work_tracker p;
                                     Dpool.push_local pd.deque_pools.(P.toInt p) (my_id pd)
                                       (make_with l.holder work.work)
                                    )
                                 | ChangeTo p' ->
                                    if Atomic.compare_and_set l.holder old_holder (ChangeTo (P.join p p')) then
                                      ()
                                    else (Domain.cpu_relax (); loop (Atomic.get m))
                               );
                                { l with waiting = add_to_waiting l.waiting (k,ts,pd);
                                         prio = p
                                }
                               )
                             else
                               { l with waiting = add_to_waiting l.waiting (k,ts,pd) }
                           in
                           if Atomic.compare_and_set m old (Locked new_state)
                           then ()
                           else (Domain.cpu_relax (); loop (Atomic.get m))
                      in loop old
           ))

    let rec unlock pool (m: t) =
      match Atomic.get m with
      | Unlocked -> failwith "Mutex.unlocked: mutex is already unlocked"
      | Locked ({ waiting = []; _ } as l) as old ->
         if Atomic.compare_and_set m old Unlocked
         then
           (* We might need to change our priority back *)
           if P.peq l.prio l.original_prio then ()
           else change pool ~prio:l.original_prio 
         else unlock pool m
      | Locked ({ waiting = ((k, ts, pd)::waiting); _ } as l) as old ->
         let new_state =
           { l with waiting = waiting;
                    prio = prio_of_ts ts }
         in
         if Atomic.compare_and_set m old (Locked new_state)
         then
           begin
             cont () (k, ts, pd);
             (* We might need to change our priority back *)
             if P.peq l.prio l.original_prio then ()
             else change pool ~prio:l.original_prio
           end
         else unlock pool m
         
  end
