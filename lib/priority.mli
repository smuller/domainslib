type priority

exception PriorityError of string

val top : unit -> priority
(** Returns the top priority currently in the system **)

val bot : priority
(** A designated priority lower than all others. This is the priority of the
    main thread. **)

val count : unit -> int
(** Returns the number of declared priorities. **)

val toInt : priority -> int
  
val new_priority : unit -> priority
(** Create a new priority higher than all existing priorities.
     Must be called before [setup_pool]. **)

val plt : priority -> priority -> bool
(** [plt] p1 p2 returns true if and only if p1 < p2 in the priority order. **)
                             
val ple : priority -> priority -> bool
(** [ple] p1 p2 returns true if and only if p1 <= p2 in the priority order. **)
  
val peq : priority -> priority -> bool
(** [peq] p1 p2 returns true if and only if p1 = p2 in the priority order. **)

val join : priority -> priority -> priority
  
type work_tracker
val make_work_tracker : unit -> work_tracker
val get_work : work_tracker -> priority -> bool
val set_work : work_tracker -> priority -> unit
val clear_work : work_tracker -> priority -> unit
val highest_with_work : work_tracker -> priority
