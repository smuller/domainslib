type 'a task = unit -> 'a
(** Type of task *)

type !'a promise
(** Type of promises *)

type pool
(** Type of task pool *)

val setup_pool : ?name:string -> num_domains:int -> unit -> pool
(** Sets up a task execution pool with [num_domains] new domains. If [name] is
    provided, the pool is mapped to [name] which can be looked up later with
    [lookup_pool name].

    When [num_domains] is 0, the new pool will be empty, and when an empty
    pool is in use, every function in this module will run effectively
    sequentially, using the calling domain as the only available domain.

    Raises {!Invalid_argument} when [num_domains] is less than 0. *)

val teardown_pool : pool -> unit
(** Tears down the task execution pool. *)

val lookup_pool : string -> pool option
(** [lookup_pool name] returns [Some pool] if [pool] is associated to [name] or
    returns [None] if no value is associated to it. *)

val get_num_domains : pool -> int
(** [get_num_domains pool] returns the total number of domains in [pool]
    including the parent domain. *)

val run : pool -> 'a task -> 'a
(** [run p t] runs the task [t] synchronously with the calling domain and the
    domains in the pool [p]. If the task [t] blocks on a promise, then tasks
    from the pool [p] are executed until the promise blocking [t] is resolved.

    This function should be used at the top level to enclose the calls to other
    functions that may await on promises. This includes {!await},
    {!parallel_for} and its variants. Otherwise, those functions will raise
    [Unhandled] exception. *)

val async : pool -> ?prio:Priority.priority -> 'a task -> 'a promise
(** [async p r t] runs the task [t] asynchronously in the pool [p] at
    priority [r]. The function  returns a promise [r] in which the result of
    the task [t] will be stored. *)

val await : pool -> 'a promise -> 'a
(** [await p r] waits for the promise [r] to be resolved. During the resolution,
    other tasks in the pool [p] might be run using the calling domain and/or the
    domains in the pool [p]. If the task associated with the promise has
    completed successfully, then the result of the task will be returned. If the
    task has raised an exception, then [await] raises the same exception.

    Must be called with a call to {!run} in the dynamic scope to handle the
    internal algebraic effects for task synchronization. *)

val poll : pool -> 'a promise -> 'a option
(** [poll p r] checks the status of the promise [r] and returns immediately.
    If the task associated with the promise has
    completed successfully, then the result of the task will be returned. If the
    task has raised an exception, then [poll] raises the same exception.
    If the task is still executiong, [poll] returns [None].

    Must be called with a call to {!run} in the dynamic scope to handle the
    internal algebraic effects for task synchronization. *)

val yield : pool -> unit

val change : pool -> prio:Priority.priority -> unit
(** [change p r] changes the priority of the calling task to r. This also
    returns to the schedule, which may result in the current task being
    un-scheduled if there is a higher-priority task (whether the priority
    of the current task is raised or lowered). *)

val current_priority : pool -> Priority.priority
(** Returns the current priority of the calling task. *)

val parallel_for : ?chunk_size:int -> start:int -> finish:int ->
                   body:(int -> unit) -> pool -> unit
(** [parallel_for c s f b p] behaves similar to [for i=s to f do b i done], but
    runs the for loop in parallel with the calling domain and/or the domains in
    the pool [p]. The chunk size [c] determines the number of body applications
    done in one task; this will default to [max(1, (finish-start + 1) / (8 *
    num_domains))]. Individual iterations may be run in any order. Tasks are
    distributed to the participating domains using a divide-and-conquer scheme.

    Must be called with a call to {!run} in the dynamic scope to handle the
    internal algebraic effects for task synchronization. *)

val parallel_for_reduce : ?chunk_size:int -> start:int -> finish:int ->
                body:(int -> 'a) -> pool -> ('a -> 'a -> 'a) -> 'a -> 'a
(** [parallel_for_reduce c s f b p r i] is similar to [parallel_for] except
    that the result returned by each iteration is reduced with [r] with initial
    value [i]. The reduce operations are performed in an arbitrary order and
    the reduce function needs to be associative in order to obtain a
    deterministic result.

    Must be called with a call to {!run} in the dynamic scope to handle the
    internal algebraic effects for task synchronization. *)

val parallel_scan : pool -> ('a -> 'a -> 'a) -> 'a array -> 'a array
(** [parallel_scan p op a] computes the scan of the array [a] in parallel with
    binary operator [op] and returns the result array, using the calling domain
    and/or the domains in the pool [p]. Scan is similar to [Array.fold_left]
    but returns an array of reduced intermediate values. The reduce operations
    are performed in an arbitrary order and the reduce function needs to be
    associative in order to obtain a deterministic result.

    Must be called with a call to {!run} in the dynamic scope to handle the
    internal algebraic effects for task synchronization. *)

val parallel_find : ?chunk_size:int -> start:int -> finish:int ->
  body:(int -> 'a option) -> pool -> 'a option
(** [parallel_find ~start ~finish ~body pool] calls [body] in parallel
    on the indices from [start] to [finish], in any order, until at
    least one of them returns [Some v].

    Search stops when a value is found, but there is no guarantee that
    it stops as early as possible, other calls to [body] may happen in
    parallel or afterwards.

    See {!parallel_for} for the description of the [chunk_size]
    parameter and the scheduling strategy.

    Must be called with a call to {!run} in the dynamic scope to
    handle the internal algebraic effects for task synchronization.
*)

                                        (** I/O functions **)
val input_line : pool -> in_channel -> string

module type MUTEX =
  sig
    type t
    exception CeilingViolated
    val create : ?ceil:Priority.priority -> unit -> t
    (** Creates a new mutex object. Mutexes implement the "priority protection"
        or "priority ceiling emulation" protocol. The optional [ceil] argument
        is the priority ceiling of the mutex, and must be greater than or
        equal to any priority with which the mutex might be locked.
        If unspecified, [ceil] defaults to Priority.top. *)
    
    val lock : pool -> t -> unit
    (** Locks the given mutex. If the mutex's priority ceiling is greater
        than the current priority, the calling thread is temporarily promoted
        to the priority ceiling. *)

    val unlock: pool -> t -> unit
    (** Unlocks the mutex. If the thread was promoted to a higher priority
        for its critical section, it now regains the priority it had upon
        calling [lock]. *)
  end
  
module Mutex : MUTEX
