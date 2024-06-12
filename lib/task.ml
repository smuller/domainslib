open Effect
open Effect.Deep

type 'a task = unit -> 'a

type message =
| Work of (unit -> unit)
  (* Invariant: the Work function does not need to run under the 'step' handler,
     it installs its own handler or re-invokes a deep-handler continuation. *)
| Quit

module P = Priority

type deque_pools = message Dpool.t array

type pool_data = {
  domains      : unit Domain.t array;
  deque_pools  : deque_pools;
  name         : string option;
  current_prio : P.priority array;
  dls          : int Domain.DLS.key;
  work_tracker : P.work_tracker;
  io_waiting   : (unit -> bool) list Atomic.t array
}

type pool = pool_data option Atomic.t

type 'a promise_state =
  Returned of 'a
| Raised of exn * Printexc.raw_backtrace
| Pending of (('a, unit) continuation * P.priority * pool_data) list

type 'a promise = 'a promise_state Atomic.t

type _ t += Wait : 'a promise * P.priority * pool_data -> 'a t
type _ t += Io : (unit -> 'a option) * P.priority * pool_data -> 'a t
type _ t += Yield : P.priority * pool_data -> unit t
type _ t += YieldAndContinue  : P.priority * pool_data * ((unit, unit) continuation * P.priority * pool_data) -> unit t
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

let current_priority pool =
  let pd = get_pool_data pool in
  my_prio pd

let set_my_prio pd p =
  let id = my_id pd in
  pd.current_prio.(id) <- p

let cont v (k, p, pd) =
  Dpool.push_local pd.deque_pools.(P.toInt p) (my_id pd) (Work (fun _ -> continue k v));
  P.set_work pd.work_tracker p

let discont e bt (k, p, pd) =
  Dpool.push_local pd.deque_pools.(P.toInt p) (my_id pd)
    (Work (fun _ -> discontinue_with_backtrace k e bt));
  P.set_work pd.work_tracker p

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
  match Atomic.exchange p result with
  | Pending l -> List.iter action l
  |  _ -> failwith "Task.do_task: impossible, can only set result of task once"

let await pool promise =
  let pd = get_pool_data pool in
  let proc = my_id pd in
  let p = pd.current_prio.(proc) in
  match Atomic.get promise with
  | Returned v -> v
  | Raised (e, bt) -> Printexc.raise_with_backtrace e bt
  | Pending _ -> perform (Wait (promise, p, pd))

let poll _ promise =
  match Atomic.get promise with
  | Returned v -> Some v
  | Raised (e, bt) -> Printexc.raise_with_backtrace e bt
  | Pending _ -> None

let input_line pool c =
  let pd = get_pool_data pool in
  let poll () =
    match Unix.select [ (Unix.descr_of_in_channel c) ] [] [] 0.01 with
    | ([], [], []) -> None
    | _ -> Some (input_line c)
  in
  perform (Io (poll, my_prio pd, pd))

let handle_io
      (pd: pool_data)
      (k: ('a, _) continuation)
      (poll: unit -> 'a option)
      (p: P.priority) () =
  (* Printf.printf "Handle io\n%!"; *)
  match poll () with
  | Some res ->
     Dpool.push_local
       pd.deque_pools.(P.toInt p)
       (my_id pd)
       (Work (fun _ -> continue k res))
    ; P.set_work pd.work_tracker p
    ; true
  | None -> false

let step (type a) (f : a -> unit) (v : a) : unit =
  try_with f v
  { effc = fun (type a) (e : a t) ->
      match e with
      | Wait (p, r, c) -> Some (fun (k : (a, _) continuation) ->
          let rec loop () =
            let old = Atomic.get p in
            match old with
            | Pending l ->
                if Atomic.compare_and_set p old (Pending ((k,r, c)::l)) then ()
                else (Domain.cpu_relax (); loop ())
            | Returned v -> continue k v
            | Raised (e,bt) -> discontinue_with_backtrace k e bt
          in
          loop ())
      | Io (poll, r, pd) -> Some (fun (k : (a, _) continuation) ->
          let rec loop () =
            match poll () with
            | Some v -> continue k v
            | None ->
               let iow = Array.unsafe_get pd.io_waiting (my_id pd) in
               let old = Atomic.get iow in
               let handler = handle_io pd k poll r in
               if Atomic.compare_and_set iow old (handler::old) then ()
               else (Domain.cpu_relax (); loop ())
          in loop ())
      | Yield (p, pd) -> Some (fun (k : (a, _) continuation) ->
         (*Printf.printf "%d pushing at %d\n%!" (my_id pd) (P.toInt p); *)
         Dpool.push_local
           pd.deque_pools.(P.toInt p)
           (my_id pd)
           (Work (fun _ -> continue k ()));
         P.set_work pd.work_tracker p
                           )
      | YieldAndContinue (p, pd, t) -> Some (fun (k : (a, _) continuation) ->
         (*Printf.printf "%d pushing at %d\n%!" (my_id pd) (P.toInt p); *)
         Dpool.push_local
           pd.deque_pools.(P.toInt p)
           (my_id pd)
           (Work (fun _ -> continue k ()));
         P.set_work pd.work_tracker p;
         cont () t)
      | Suspend f -> Some (fun (k : (a, _) continuation) -> f k)
      | _ -> None }

let async pool ?(prio=(my_prio (get_pool_data pool))) f =
  let pd = get_pool_data pool in
  let p = Atomic.make (Pending []) in
  (* Printf.printf "%d pushing at %d\n%!" (my_id pd) (P.toInt prio); *)
  Dpool.push_local pd.deque_pools.(P.toInt prio) (my_id pd)
    (Work (fun _ -> step (do_task f) p));
  P.set_work pd.work_tracker prio;
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
        |> List.iter @@ fun (k, r, pd) ->
                        (Dpool.push_global pd.deque_pools.(P.toInt r)
                           (Work (fun _ -> continue k ()));
                         P.set_work pd.work_tracker r)
      | _ -> ()
  and await () =
    match Atomic.get promise with
    | (Returned _ | Raised _) -> ()
    | Pending _ -> perform (Wait (promise, my_prio pd, pd))
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
    | Work f ->
       (if P.plt (my_prio pd) prio then
          begin
            Dpool.push_deque_to_mug
              pd.deque_pools.(P.toInt (my_prio pd))
              (my_id pd)
          end
       );
       set_my_prio pd prio;
       f ();
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
  step (fun _ -> do_task f p) ();
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
           | Work f -> set_my_prio pd prio; f ()
           | Quit -> failwith "Task.run: tasks are active on pool"
         with Exit ->
           (P.clear_work pd.work_tracker prio;
              Domain.cpu_relax ())
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
  let p = my_prio pd in
  (* let _ = Printf.printf "Yield at %d\n%!" (P.toInt p) in *)
  perform (Yield (p, pd))

let change pool ~prio = perform (Yield (prio, get_pool_data pool))
  

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
  let dls = make_dls () in
  let work_tracker = P.make_work_tracker () in
  let io_waiting = Array.init (num_domains + 1) (fun _ -> Atomic.make []) in
  let _ =
    Atomic.set
      p
      (Some {domains; deque_pools; name; current_prio; work_tracker; dls; io_waiting})
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
    exception CeilingViolated
    val create : ?ceil:Priority.priority -> unit -> t
    val lock : pool -> t -> unit
    val unlock: pool -> t -> unit
  end

(* Mutexes with priority protection/priority ceiling emulation *)
module Mutex : MUTEX =
  struct

    type state =
      Locked of ((unit, unit) continuation * P.priority * pool_data) list
    | Unlocked

    type t =
      { state: state Atomic.t;
        ceiling: P.priority;
        (* If currently running at a higher priority, we will return to this one
         * upon unlocking
         *)
        mutable old_prio: P.priority option
      }

    exception CeilingViolated

    let create ?ceil () =
      let ceil =
        match ceil with
        | None -> P.top ()
        | Some c -> c
      in
      { state = Atomic.make Unlocked;
        ceiling = ceil;
        old_prio = None
      }

                  (*
    let print_lock = Mutex.create ()
    let print f = Mutex.lock print_lock; f (); Mutex.unlock print_lock
                   *)
                  
    let rec lock pool (m: t) =
      let _ = Printf.printf "lock\n%!" in
      let _ =
        let my_p = current_priority pool in
        if not (P.ple my_p m.ceiling) then
           raise CeilingViolated
      in
      let maybe_promote_me () =
        let my_p = current_priority pool in
        if P.plt my_p m.ceiling then
          (m.old_prio <- Some my_p;
           change pool ~prio:m.ceiling;
          )
      in
      match Atomic.get m.state with
      | Unlocked ->
         if Atomic.compare_and_set m.state Unlocked (Locked [])
         then maybe_promote_me ()
         else (Domain.cpu_relax (); lock pool m)
      | Locked _ as old ->
         let _ = Printf.printf "contention\n%!" in
         let pd = get_pool_data pool in
         let proc = my_id pd in
         let p = pd.current_prio.(proc) in
         begin
           perform (Suspend
                    (fun k ->
                      let rec loop old =
                        match old with
                        | Unlocked ->
                           if Atomic.compare_and_set m.state old (Locked [])
                           then
                             (* It's unlocked so we want to just quickly
                              * return, but we're already in the handler
                              * so it's too late to avoid going back to
                              * the scheduler. *)
                             (Printf.printf "Just kidding\n%!";
                              cont () (k, p, pd))
                           else (Domain.cpu_relax (); loop (Atomic.get m.state))
                        | Locked l ->
                           if Atomic.compare_and_set m.state old (Locked (l @ [(k,p,pd)]))
                           then (Printf.printf "added me\n%!"; ())
                           else (Domain.cpu_relax (); loop (Atomic.get m.state))
                      in loop old
             ));
           (* Make sure we can still get the lock *)
           (* lock pool m; *)
           maybe_promote_me ()
         end

    let unlock pool (m: t) =
      let old_p = m.old_prio in
      let _ = m.old_prio <- None in
      let maybe_demote_me () =
        match old_p with
         | None -> ()
         | Some p -> change pool~prio:p
      in
      let rec unlock_loop () =
        let _ = Printf.printf "unlock\n%!" in
        match Atomic.get m.state with
        | Unlocked -> failwith "Mutex.unlocked: mutex is already unlocked"
        | Locked [] as old ->
           Printf.printf "unlocking: no waiters\n%!";
           if Atomic.compare_and_set m.state old Unlocked
           then maybe_demote_me ()
           else unlock_loop ()
        | Locked (t::waiting) as old ->
           if Atomic.compare_and_set m.state old (Locked waiting)
           then
             (match old_p with
              | None -> Printf.printf "unlocking: no demotion\n%!";()
              | Some p ->
                 (* If we change to a lower priority, we might get
                  * unscheduled before we call cont on t, so we need to
                  * do both at the same time *)
                 Printf.printf "unlocking\n%!";
                 perform (YieldAndContinue (p, get_pool_data pool, t))
             )
           else unlock_loop ()
      in
      unlock_loop ()
         
  end
