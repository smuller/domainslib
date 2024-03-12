module D = Saturn.Work_stealing_deque.M
module Q = Saturn.Queue

type deque_state = Active | Suspended | Resumable
         
type 'a deque = {
    q     : 'a D.t;
    mutable state : deque_state;
  }
         
type 'a t = {
    active  : 'a deque Array.t;
    regular : 'a deque Q.t;
    mugging : 'a deque Q.t
  }

let my_id () = 0 (* XXX *)

let push_if_needed (dp: 'a t) (d: 'a deque) : unit =
  Q.push dp.regular d
             
let rec steal (dp: 'a t) (proc: int) : 'a =
  try
    let d = Q.pop dp.regular in
    let a =
      match d.state with
      | Resumable ->
         let a = D.pop d.q in
         dp.active.(proc) <- d;
         a
      | _ ->
         D.steal d.q;
    in
    push_if_needed dp d;
    a
  with Exit | Q.Empty -> steal dp proc
  
let mug (dp: 'a t) (proc: int) : 'a =
  try
    let d = Q.pop dp.mugging in
    let a =
      match d.state with
      | Resumable ->
         let a = D.pop d.q in
         dp.active.(proc) <- d;
         a
      | _ ->
         D.steal d.q;
    in
    push_if_needed dp d;
    a
  with Exit | Q.Empty -> steal dp proc
          
let pop (dp: 'a t) : 'a =
  let proc = my_id () in
  let d = Array.unsafe_get dp.active proc in
  try D.pop d.q
  with Exit -> mug dp proc

let push_local (dp: 'a t) (v: 'a) : unit =
  let proc = my_id () in
  let d = Array.unsafe_get dp.active proc in
  D.push d.q v

let push_global (dp: 'a t) (v: 'a) : unit =
  let d = {q = D.create (); state = Resumable} in
  let _ = D.push d.q v in
  Q.push dp.mugging d
