let num_domains = 2
                                               
module T = Domainslib.Task
module M = T.Mutex
module P = Domainslib.Priority

let low = P.bot
let medium = P.new_priority ()
let high = P.new_priority ()

let mut = M.create ()

let rec fib_no_yield n =
  if n < 2 then 1
  else
    (fib_no_yield (n-1) + fib_no_yield (n-2))

let rec fib pool n =
  if n < 2 then 1
  else
    ((if n > 35 then T.yield pool);
     fib pool (n-1) + fib pool (n-2))

let low_thread pool () =
  Printf.printf "Starting low!\n%!";
  M.lock pool mut;
  Printf.printf "Locked\n%!";
  ignore (fib pool 39);
  Printf.printf "low done with mutex!\n%!";
  M.unlock pool mut;
  Printf.printf "Unlocked\n%!";
  T.change pool ~prio:low;
  ignore (fib pool 41);
  Printf.printf "Done with low!\n%!"
  


let med_thread pool () =
  let rec loop n m =
    let n' = if n = 0 then (T.yield pool; m) else n - 1
    in
    loop n' m
  in
  Printf.printf "Starting medium!\n%!";
  (* ignore (fib pool 42); *)
  ignore (loop 10000 10000);
  Printf.printf "Done with medium!\n%!"

let high_thread pool () =
  Printf.printf "Starting high!\n%!";
  M.lock pool mut;
  ignore (fib pool 10);
  Printf.printf "Done with high!\n%!";
  M.unlock pool mut


let all pool () =
  let rec loop () = loop () in
  let _ = T.async pool ~prio:low (low_thread pool) in
  let _ = fib_no_yield 36 in
  let _ = T.async pool ~prio:medium (med_thread pool) in
  let _ = Printf.printf "Started medium\n%!" in
  let _ = fib_no_yield 36 in
  let _ = Printf.printf "Starting high\n%!" in
  let _ = T.async pool ~prio:high (high_thread pool) in
  let _ = Printf.printf "Started high\n%!" in
  loop ()
    (*
  ignore (fib_no_yield 43);
  T.await pool h;
  T.await pool l
     *)

let main =
  let pool = T.setup_pool ~num_domains:(num_domains - 1) () in
  let _ = T.run pool (all pool) in
  T.teardown_pool pool

let () = main
