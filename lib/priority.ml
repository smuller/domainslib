type priority = int

let bot = 1

let numberOfPrios = ref 1

let toInt p = p

let initialized = ref false

let count () = !numberOfPrios

let top () = count ()

let plt = (<)
let ple = (<=)

(* XXX currently assumes priorities are declared in order from low to high *)
        
let new_priority () =
  numberOfPrios := (!numberOfPrios) + 1;
  count ()

let new_lessthan () = ()
