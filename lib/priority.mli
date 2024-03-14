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
(** Create a new priority. Must be called before [setup_pool]. **)

val new_lessthan : priority -> priority -> unit
(** [new_lessthan p1 p2] declares p1 to be lower priority than p2.
    Raises [CyclicPriorities] if this creates a cycle in the priority order.
    Must be called before [setup_pool]. **)

val plt : priority -> priority -> bool
(** [plt] p1 p2 returns true if and only if p1 < p2 in the priority order. **)
                             
val ple : priority -> priority -> bool
(** [ple] p1 p2 returns true if and only if p1 <= p2 in the priority order. **)

val get_work : priority -> bool
val set_work : priority -> unit
val clear_work : priority -> unit
val highest_with_work : unit -> priority
