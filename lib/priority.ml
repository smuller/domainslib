type priority = int

exception PriorityError of string

let bot = 0

let numberOfPrios = ref 1

let toInt p = p

let count () = !numberOfPrios

let top () = count ()

let plt = (<)
let ple = (<=)

(* XXX currently assumes priorities are declared in order from low to high *)
        
let new_priority () =
  if count () >= Sys.int_size then
    raise (PriorityError "exceeded max number of priorities")
  else
    (numberOfPrios := (!numberOfPrios) + 1;
     count () - 1
    )

let new_lessthan _ _ = ()

type work_tracker = int Atomic.t (* Domain.DLS.key *)

let make_work_tracker () =
  Atomic.make 0
    (*
  Domain.DLS.new_key (fun _ -> 0)
     *)
  
let prio_mask p =
  Int.shift_left 1 p

let get_work work_bitfield p =
  let work = Atomic.get work_bitfield in
  (Int.logand work (prio_mask p)) <> 0

let rec set_work work_bitfield p =
  let work = Atomic.get work_bitfield in
  if (Int.logand work (prio_mask p)) = 0 then
    let new_work = Int.logor work (prio_mask p) in
    if Atomic.compare_and_set work_bitfield work new_work then
      ()
    else set_work work_bitfield p
  else
    (* Bit is already set *)
    ()

let rec clear_work work_bitfield p =
  let work = Atomic.get work_bitfield in
  if (Int.logand work (prio_mask p)) <> 0 then
    let new_work = Int.logxor work (prio_mask p) in
    if Atomic.compare_and_set work_bitfield work new_work then
      ()
    else clear_work work_bitfield p
  else
    (* Bit is already cleared *)
    ()

let rec lin_scan work p =
  if p = 0 then 0
  else
    if (Int.logand work (prio_mask p)) <> 0 then p
    else lin_scan work (p - 1)
  
let highest_with_work work_bitfield =
  let work = Atomic.get work_bitfield in
  lin_scan work (count ())
  
