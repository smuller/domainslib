
module Q = Saturn.Queue

module type DEQUE =
  sig

    type deque_state = Active of int | Suspended | Resumable

    type 'a deque
    
    val new_deque : deque_state -> 'a deque
    val is_mine : 'a deque -> int -> bool
    val push: 'a deque -> 'a -> unit
    val pop : 'a deque -> 'a
    val steal : 'a deque -> 'a

    val count : 'a deque -> int
    val state : 'a deque -> deque_state
    val set_state : 'a deque -> deque_state -> unit
    val cas_state : 'a deque -> deque_state -> deque_state -> bool
    val id : 'a deque -> int

  end

module D : DEQUE =
  struct

    module D = Saturn.Work_stealing_deque.M
    type deque_state = Active of int | Suspended | Resumable
         
    type 'a deque = {
        q             : 'a D.t;
        state         : deque_state Atomic.t;
        count         : int Atomic.t;
        id            : int
      }

    let next_id = ref 0

    let new_deque state =
      let id = !next_id in
      next_id := id + 1;
      { q = D.create ();
        state = Atomic.make state;
        count = Atomic.make 0;
        id = id}

    let is_mine d proc =
      match Atomic.get d.state with
      | Active proc' -> proc = proc'
      | _ -> false
    
    let push q v =
      D.push q.q v;
      Atomic.incr q.count


    let rec pop q =
      try
        let res = D.pop q.q in
        Atomic.decr q.count;
        res
      with Exit ->
            if Atomic.get (q.count) > 0 then pop q
            else raise Exit

    let rec steal q =
      try
        let res = D.steal q.q in
        Atomic.decr q.count;
        res
      with Exit ->
        if Atomic.get (q.count) > 0 then steal q
        else raise Exit

    let count q = Atomic.get q.count
    let id q = q.id

    let state q = Atomic.get q.state

    let set_state q s = Atomic.set q.state s

    let cas_state q old newv = Atomic.compare_and_set q.state old newv

  end

type 'a deque = 'a D.deque
         
type 'a t = {
    active  : 'a deque Array.t;
    regular : 'a deque Q.t;
    mugging : 'a deque Q.t
  }

let check_actives dp =
  let print_state s =
    match s with
    | D.Active p -> "Active " ^ (string_of_int p)
    | D.Resumable -> "Resumable"
    | D.Suspended -> "Suspended"
  in
  let (_, b) =
    (Array.fold_left
       (fun (i, b) d ->
         let is_mine = D.is_mine d i in
         if not is_mine then
           Printf.printf "ERROR: %d's deque state is %s!\n%!" i (print_state (D.state d))
         ;
         (i + 1, b && is_mine))
       (0, true)
       dp.active
    )
  in
  b

let make num_domains =
  { active = Array.init num_domains (fun p -> D.new_deque (Active p));
    regular = Q.create ();
    mugging = Q.create () }

let push_if_needed (dp: 'a t) (d: 'a deque) : unit =
  if D.count d > 0 then
    Q.push dp.regular d
             
let rec steal (dp: 'a t) (proc: int) : 'a =
  try
    let d = Q.pop dp.regular in
    if D.is_mine d proc then
      (assert (D.count d = 0);
       steal dp proc)
    else
      let a =
        if D.cas_state d Resumable (Active proc) then
          (* We now own the deque; it may be empty but that's OK *)
          (dp.active.(proc) <- d;
           assert (D.is_mine d proc);
           D.pop d
          )
        else D.steal d
      in
      push_if_needed dp d;
      a
  with Exit -> steal dp proc
     | Q.Empty -> raise Exit (* steal dp proc *)
  
let rec mug (dp: 'a t) (proc: int) : 'a =
  try
    let d = Q.pop dp.mugging in
    if D.cas_state d Resumable (Active proc) then
      (* We now own the deque; it may be empty but that's OK *)
      (dp.active.(proc) <- d;
       (* assert (check_actives dp); *)
       assert (D.is_mine d proc);
       let a = D.pop d in
       a)
    else
  (* Deque was already resumed; just steal from it.*)
      (* mug dp proc *)
      D.steal d
  with Exit -> mug dp proc
     | Q.Empty -> steal dp proc

let pop (dp: 'a t) (proc: int) : 'a =
  let d = Array.unsafe_get dp.active proc in
  (* assert (check_actives dp); *)
  assert (D.is_mine d proc);
  try D.pop d
  with Exit -> mug dp proc

let push_local (dp: 'a t) (proc: int) (v: 'a) : unit =
  let d = Array.unsafe_get dp.active proc in
  (* assert (check_actives dp); *)
  D.push d v;
  if D.count d = 1 then push_if_needed dp d

let push_global (dp: 'a t) (v: 'a) : unit =
  let d = D.new_deque Resumable in
  D.push d v;
  Q.push dp.mugging d

let push_deque_to_mug (dp: 'a t) (proc: int) : unit =
  let d = Array.unsafe_get dp.active proc in
  if D.count d > 0 then
    (
      dp.active.(proc) <- D.new_deque (Active proc);
      D.set_state d Resumable;
      Q.push dp.mugging d
    )
(*
  ;
    assert (check_actives dp)
 *)
