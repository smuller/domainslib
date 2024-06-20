let num_domains = try int_of_string Sys.argv.(1) with _ -> 1
let t = try int_of_string Sys.argv.(2) with _ -> 43
let per_thread = 100000

module P = Domainslib.Priority                                          
module T = Domainslib.Task
module M = T.Mutex

let p2 = P.new_priority ()

let mut = M.create ()
let r = ref 0


        
let rec thread pool n =
  if n <= 0 then Printf.printf "done\n%!"
  else
    (M.lock pool mut;
     Printf.printf "%d\t%d\n%!" (!r) n;
     r := (!r) + 1;
     M.unlock pool mut;
     thread pool (n - 1))

let rec all pool threads =
  if threads <= 0 then ()
  else
    let _ = Printf.printf "all %d\n%!" threads in
    let a = T.async pool ~prio:(if threads mod 2 = 0 then p2 else P.bot)
              (fun _ -> thread pool per_thread)
    in
    all pool (threads - 1);
    T.await pool a

let main =
  let pool = T.setup_pool ~num_domains:(num_domains - 1) () in
  let _ = T.run pool (fun _ -> all pool t) in
  T.teardown_pool pool;
  Printf.printf "result = %d\nexpected = %d\n" (!r) (per_thread * t)

let () = main
