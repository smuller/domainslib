
module Q = Saturn.Queue


module D =
  struct

    module D = Saturn.Work_stealing_deque.M
    type deque_state = Active | Suspended | Resumable
         
    type 'a deque = {
        q             : 'a D.t;
        mutable state : deque_state;
        count         : int Atomic.t;
        id            : int
      }

    let next_id = ref 0

    let new_deque state _ =
      let id = !next_id in
      next_id := id + 1;
      { q = D.create ();
        state = state;
        count = Atomic.make 0;
        id = id}

    let push q v =
      D.push q.q v;
      Atomic.incr q.count;
      Printf.printf "to %d\n%!" q.id;
      Printf.printf "after push: %d\n%!" (Atomic.get (q.count))


    let pop q =
      try
        Printf.printf "from %d\n%!" q.id;
        Printf.printf "before pop: %d\n%!" (Atomic.get (q.count));
        let res = D.pop q.q in
        Atomic.decr q.count;
        Printf.printf "after pop: %d\n%!" (Atomic.get (q.count));
        res
      with Exit -> raise Exit

    let steal q =
      try
        Printf.printf "steal from %d\n%!" q.id;
        Printf.printf "before pop: %d\n%!" (Atomic.get (q.count));
        let res = D.steal q.q in
        Atomic.decr q.count;
        Printf.printf "after steal: %d\n%!" (Atomic.get (q.count));
        res
      with Exit -> raise Exit

    let count q = Atomic.get q.count

    let state q = q.state

    let set_state q s = q.state <- s

  end

type 'a deque = 'a D.deque
         
type 'a t = {
    active  : 'a deque Array.t;
    regular : 'a deque Q.t;
    mugging : 'a deque Q.t
  }



let make num_domains =
  { active = Array.init num_domains (D.new_deque Active);
    regular = Q.create ();
    mugging = Q.create () }

let push_if_needed (dp: 'a t) (d: 'a deque) : unit =
  if D.count d > 0 then
    Q.push dp.regular d
             
let rec steal (dp: 'a t) (proc: int) : 'a =
  try
    let d = Q.pop dp.regular in
    let a =
      match D.state d with
      | Resumable ->
         let a = D.pop d in
         dp.active.(proc) <- d;
         D.set_state d Active;
         a
      | _ ->
         D.steal d;
    in
    push_if_needed dp d;
    a
  with Exit -> steal dp proc
     | Q.Empty -> raise Exit (* steal dp proc *)
  
let rec mug (dp: 'a t) (proc: int) : 'a =
  try
    let d = Q.pop dp.mugging in
    let a =
      match d.state with
      | Resumable ->
         let a = D.pop d in
         dp.active.(proc) <- d;
         a
      | _ -> failwith "non-resumable deque in mugging queue"
                      (*D.steal d; *)
    in
    push_if_needed dp d;
    a
  with Exit -> mug dp proc
     | Q.Empty -> steal dp proc
          
let pop (dp: 'a t) (proc: int) : 'a =
  let d = Array.unsafe_get dp.active proc in
  try D.pop d
  with Exit -> mug dp proc

let push_local (dp: 'a t) (proc: int) (v: 'a) : unit =
  let d = Array.unsafe_get dp.active proc in
  D.push d v;
  if D.count d = 1 then push_if_needed dp d

let push_global (dp: 'a t) (v: 'a) : unit =
  let d = D.new_deque Resumable () in
  D.push d v;
  Q.push dp.mugging d

let push_deque_to_mug (dp: 'a t) (proc: int) : unit =
  let d = Array.unsafe_get dp.active proc in
  if D.count d > 0 then
    (
      D.set_state d Resumable;
      Q.push dp.mugging d
    )
