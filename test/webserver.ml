exception Invalid_argument of string

module P = Domainslib.Priority
module T = Domainslib.Task

open Unix

let num_domains = try int_of_string Sys.argv.(1) with _ -> 3

let class_p = P.new_priority ()
let accept_p = P.new_priority ()
(* let high_hash_p = P.new_priority () *)
let serve_p = P.new_priority ()

let pool = T.setup_pool ~num_domains:(num_domains - 1) ()

type host = string
   
let host_compare = String.compare

let host_of_sockaddr h =
  match h with
    ADDR_UNIX s -> s
  | ADDR_INET (a, _) -> string_of_inet_addr a

module TSHashtbl = Hashtbl
module UHashtbl = Hashtbl

type ts_list_hashtbl = (host, float) TSHashtbl.t
type unit_hashtbl = (host, unit) UHashtbl.t

let string_of_host h = h

type request =
     {page : string;
      host : host;
      ts   : float}

let socks = ref []
let conns = Atomic.make 0
let reqs : request list ref = ref []
let conns_hashtbl : ts_list_hashtbl = Hashtbl.create 100
let bad_hashtbl : unit_hashtbl = Hashtbl.create 100

(* Parameters *)
(*let conns_cutoff = 10 (* Number of connections for switching priority *) *)
let req_threshold = 5 (* A host is bad if it has more than this many reqs *)
let time_window = 5.0 (* over this many seconds *)
let hosts_par_threshold = 10 (* Hosts to process sequentially for tracking *)

let request_mut = T.Mutex.create ~ceil:serve_p ()
let bad_mut = T.Mutex.create ~ceil:accept_p ()

let close sock = (Unix.shutdown sock) Unix.SHUTDOWN_ALL

let addtosocks s =
    socks := (fun () -> close s)::(!socks)

let open_conn _ =
    let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0
    in
    begin
      Unix.setsockopt sock Unix.SO_REUSEADDR true;
      Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_loopback, 8000));
      Unix.listen sock 32;
      addtosocks sock;
      sock
    end

let _ = at_exit (fun () -> List.iter (fun f -> f ()) (!socks))

let parse_request s =
    let tokens = String.split_on_char ' ' s in
    let url = List.nth tokens 1 in
    let urltokens = String.split_on_char '/' url in
	Some (List.nth urltokens (List.length urltokens - 1))

	  
let format_date tm =
    (match (tm.tm_wday) with
	 0 -> "Sun"
       | 1 -> "Mon"
       | 2 -> "Tue"
       | 3 -> "Wed"
       | 4 -> "Thur"
       | 5 -> "Fri"
       | 6 -> "Sat"
       | _ -> raise (Invalid_argument "format_date")
    )
    ^ ", "
    ^ (string_of_int (tm.tm_mday))
    ^ " "
    ^
    (match tm.tm_mon with
	 0 -> "January"
       | 1 -> "February"
       | 2 -> "March"
       | 3 -> "April"
       | 4 -> "May"
       | 5 -> "June"
       | 6 -> "July"
       | 7 -> "August"
       | 8 -> "September"
       | 9 -> "October"
       | 10 -> "November"
       | 11 -> "December"
       | _ -> raise (Invalid_argument "format_date")
    )
    ^ " " ^ (string_of_int ((tm.tm_year) + 1900))
    ^ " " ^ (string_of_int (tm.tm_hour))
    ^ ":" ^ (string_of_int (tm.tm_min))
    ^ ":" ^ (string_of_int (tm.tm_sec))

let build_success s =
  [  "HTTP/1.0 200 OK"
   ; ("Date: " ^
      (format_date (Unix.gmtime (Unix.time ()))))
   ; "Content-Type: text/html"
   ; ("Content-Length: " ^ (string_of_int (String.length s)))
   ; "Connection: Keep-Alive"
   ; ""
   ; s
  ]
		
let build_inv_req () =
    ["HTTP/1.0 400 Bad Request"]

let build_404 () =
    ["HTTP/1.0 404 Not Found"
    ; ("Date: " ^
	(format_date (Unix.gmtime (Unix.time ()))))
    ; "Content-Type: text/html"
    ; "Content-Length: 0"
    ; ""
    ]

let readfile f =
  try
    let chan = open_in f in
    let rec readloop s =
      try
        (readloop s ^ (input_line chan))
      with End_of_file -> s
    in
    let s = readloop ""
    in
    close_in chan;
    Some s
  with _ -> None


let sendLine sock s =
    let rec sendLineRec (sock, s, n, len) =
	    if n = len then ()
	    else
		let n' = Unix.send_substring sock s n len []
			     (*
		    val _ = Time.print ((string_of_int n) ^ "\n")
		    val _ = Time.print ((string_of_int n') ^ "\n")
*)
		in
		    sendLineRec (sock, s, n', len)
    in
    let s = s ^ "\r\n"
		(*
	val _ = Time.print s
	val _ = Printf.printf "%!"
*)
    in	
    sendLineRec (sock, s, 0, String.length s)

let recv sock =
  let n = 1024 in
  let bytes = Bytes.create n in
  let n' = Unix.recv sock bytes 0 n [] in
  let bytes' = ((Bytes.sub bytes) 0) n' in
  let s = Bytes.to_string bytes' in
  s

let rec inploop addr sock =
  let req = recv sock in
  let _ = Printf.printf "inploop\n%!" in
  if String.length req = 0 then
    (close sock; Atomic.decr conns
    )
  else
    let response =
      match parse_request req with
	Some filename ->
          (T.Mutex.lock pool request_mut;
           reqs := {page = filename;
		    host = addr;
		    ts = Unix.time ()}::(!reqs);
           T.Mutex.unlock pool request_mut;
           match readfile ("www/" ^ filename) with
             Some s -> build_success s
           | None -> build_404 ())
      | None -> build_inv_req ()
    in
    List.iter (sendLine sock) response;
    Printf.printf "sent\n%!";
    inploop addr sock

let rec acceptloop sock =
  let (s, a) = Unix.accept sock in
  let h = host_of_sockaddr a
  in
  let bad =
    T.Mutex.lock pool bad_mut;
    let b = Hashtbl.mem bad_hashtbl h in
    T.Mutex.unlock pool bad_mut;
    b
  in
  if bad then
    acceptloop sock
  else
    (Atomic.incr conns;
     Printf.printf "accepted: ";
     Printf.printf "%d" (Atomic.get conns);
     Printf.printf " connections\n%!";
     addtosocks s;
     ignore (T.async pool ~prio:serve_p (fun () -> inploop h s));
     acceptloop sock
    )

let take n l =
  let rec take_rec a n l =
    (if n <= 0 then (List.rev a, l)
     else
       (match l with
	  [] -> (List.rev a, [])
        | h::t -> take_rec (h::a) (n - 1) t
       )
    )
  in
  take_rec [] n l

let process_one_host host =
    let _ = Printf.printf "processing " in
    let _ = Printf.printf "%s\n%!" (string_of_host host) in
    let reqs = Hashtbl.find_all conns_hashtbl host in
    let now = Unix.time () in
    let recent = List.filter
		   (fun ts' -> (now -. ts') < time_window)
		   reqs
    in
    let _ = Printf.printf "%d\n%!" (List.length recent) in
    if List.length recent > req_threshold then
      (Printf.printf "Adding %s\n%!" (string_of_host host);
       [host])
    else
      []

let process_hosts hosts =
    let rec touch_all l futs =
      match futs with
	[] -> l
      | f::t ->
         let v = T.await pool f in
         touch_all (v @ l) t
    in
    let rec spawn_futs futs hosts =
      match take hosts_par_threshold hosts with
	([], _) -> touch_all [] futs
      | (l, rest) ->
         let t = T.async pool
                   ~prio:class_p
                   (fun () -> List.flatten (List.map process_one_host l))
         in
	 spawn_futs (t::futs) rest
    in
    spawn_futs [] hosts

let rec trackloop _ =
  (*
  let p = if Atomic.get conns > conns_cutoff
          then high_hash_p else low_hash_p
  in
   *)
  T.change pool ~prio:class_p;
  (* ret (Printf.printf "checking\n%!"); *)
  let new_reqs =
    T.Mutex.lock pool request_mut;
    let nr = !reqs in
    reqs := [];
    T.Mutex.unlock pool request_mut;
    nr
  in
  (* Add new requests to hash table *)
  List.iter
    (fun {host; ts; _} -> Hashtbl.add conns_hashtbl host ts)
    new_reqs;
  let hosts = List.sort_uniq
		host_compare
		(List.map (fun {host; _} -> host) new_reqs)
  in
  (* Process new hosts in parallel.
   * NOTE: This results in concurrent hash table *reads*, which is
   * safe as of the current code. *)
  let bad_hosts = process_hosts hosts in
  T.Mutex.lock pool bad_mut;
  List.iter (fun h -> Hashtbl.add bad_hashtbl h ()) bad_hosts;
  T.Mutex.unlock pool bad_mut;
  trackloop ()

let main () =
  let sock = open_conn () in
  Printf.printf "opened\n%!";
  let _ = T.async pool ~prio:class_p trackloop in
  Printf.printf "listening...\n%!";
  let l = T.async pool ~prio:accept_p (fun () -> acceptloop sock) in
  T.await pool l

let _ = Printf.printf "Hi\n%!"

let _ = T.run pool main
