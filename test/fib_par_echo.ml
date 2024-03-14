let num_domains = try int_of_string Sys.argv.(1) with _ -> 1
let n = try int_of_string Sys.argv.(2) with _ -> 43

module T = Domainslib.Task
module P = Domainslib.Priority

let fib_prio = P.bot
let echo_prio = P.new_priority ()

let rec fib pool n =
  if n < 2 then 1
  else
    ((if n > 35 then T.yield pool);
      fib pool (n-1) + fib pool (n-2))

let rec fib_par pool n =
  if n <= 40 then fib pool n
  else
    let a = T.async pool ~prio:fib_prio (fun _ -> fib_par pool (n-1)) in
    let b = T.async pool ~prio:fib_prio (fun _ -> fib_par pool (n-2)) in
    let res = T.await pool a + T.await pool b in
    Printf.printf "fib(%d) = %d\n" n res;
    res

let rec echo pool () =
  Printf.printf "What's your name? %!";
  try
    let n = T.input_line pool stdin in
    (* if n = "" then ()
    else *)
      (Printf.printf "Hi, %s!\n%!" n;
       echo pool ())
  with End_of_file -> ()

let start pool n =
  let f = T.async pool ~prio:fib_prio (fun _ -> fib_par pool n) in
  let _ = T.async pool ~prio:echo_prio (echo pool) in
  T.await pool f
  

let main =
  let pool = T.setup_pool ~num_domains:(num_domains - 1) () in
  let res = T.run pool (fun _ -> start pool n) in
  T.teardown_pool pool;
  Printf.printf "fib(%d) = %d\n" n res

let () = main
