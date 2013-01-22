# beanstalktc: An async beanstalkd client for Tornado

## About beanstalkd

[Beanstalk](http://kr.github.com/beanstalkd/) is a simple, fast work queue.

Its interface is generic, but was originally designed for reducing the latency of page views in high-volume web applications by running time-consuming tasks asynchronously.

This module contains a beanstalkd client for [Tornado](http://www.tornadoweb.org) implemented using the asynchronous (non-blocking) [IOStream](http://www.tornadoweb.org/documentation/iostream.html) socket wrapper.

The code and documentation is licensed under the Apache Licence, Version 2.0 ([http://www.apache.org/licenses/LICENSE-2.0.html]()).

## Example client usage

This simple example involves the most basic operations of putting a job in the queue, reserving and deleting it (the code are in `demo.py`):

    import tornado.ioloop
    import beanstalktc

    def show(msg, value, cb):
      print msg % value
      cb()
    
    def stop():
      client.close(ioloop.stop)
    
    def put():
      client.put("A job to work on", callback=lambda s: show(
          "Queued a job with jid %d", s, reserve))
    
    def reserve():
      client.reserve(callback=lambda s: show(
          "Reserved job %s", s, lambda: delete(s["jid"])))
    
    def delete(jid):
      client.delete(jid, callback=lambda: show(
          "Deleted job with jid %d", jid, stop))

    client = beanstalktc.Client()
    client.connect(put)
    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.start()

Executing the script (`python demo.py`) with beanstalkd running produces the following output:

    Queued a job with jid 1
    Reserved job {"body": "A job to work on", "jid": 1}
    Deleted job with jid 1

Where `jid` is the job id that beanstalkd has given the job when putting in on the queue.

The client will attempt to automatically re-connect if the socket connection to beanstalkd is closed unexpectedly. In other cases where an error occur, an exception will be passed to the callback function.

## The job lifecycle

A job in beanstalk gets created by a client with the put command. During its life it can be in one of four states:

**`ready`**  
The job waits in the ready queue until a worker sends the "reserve" command.  

**`reserved`**  
The job is reserved for a worker. The worker will execute the job, when it is finished the worker will send a "delete" command, removing the job from the queue.  

**`delayed`**  
The job is waiting a requested amount of time before it will be transitioned to the `ready` state.  

**`buried`**  
The job is in a FIFO linked list that will not be touched by the server until a client kicks them with the "kick" command  

## Tubes

The system has one or more tubes. Each tube consists of a ready queue and a delay queue. Each job spends its entire life in one tube. Consumers can show interest in tubes by sending the `watch` command; they can show disinterest by sending the `ignore` command. This set of interesting tubes is said to be a consumer’s `watch` list. When a client reserves a job, it may come from any of the tubes in its watch list.

When a client connects, its watch list is initially just the tube named `default`. If it submits jobs without having sent a use command, they will live in the tube named `default`.

Tubes are created on demand whenever they are referenced. If a tube is empty (that is, it contains no `ready`, `delayed`, or `buried` jobs) and no client refers to it, it will be deleted.

## Reference for the client module

The complete spec for the beanstalkd protocol is available in the repository.

**`beanstalktc.Client(host='localhost', port=11300, connect_timeout=socket.getdefaulttimeout(), io_loop=None)`**  
Creates a client object with methods for all beanstalkd commands as of version 1.8. The methods are described in the following.

### Connection methods

**`connect(callback=None)`**  
Establish the client's connection to beanstalkd. Calls back when connection has been established. After first attempt to connect, the client will automatically attempt to re-connect (with 1 second intervals) if the socket is closed unexpectedly.

**`close(callback=None)`**  
Close the client's connection to beanstalkd. Calls back when connection has been closed.

**`closed()`**
Return True if the connection is established, otherwise returns False.

If the connection is down (also while re-connecting), any attempt to communicate with beanstalkd, using methods in the following sections, will likely raise an IOError exception.

### Producer methods

**`put(body, priority=DEFAULT_PRIORITY, delay=0, ttr=120, callback=None)`**  
This method is for any process that wants to insert a job (body, a string) into the current tube. The job can be delayed a number of seconds, before it is put in the ready queue, default is no delay. The job is assigned a Time To Run (tar, in seconds), the minimum is 1 sec., default ttr=120 sec. Calls back with job id when inserted.

**`use(name, callback=None)`**  
This method is for producers. Subsequent put commands will put jobs into the tube specified by this command. If no use command has been issued, jobs will be put into the tube named `default`. Calls back with the name of the tube now being used.

### Worker methods

**`reserve(timeout=None, callback=None)`**  
Reserve a job from one of the watched tubes, with optional timeout
in seconds. Calls back with a newly-reserved job.

If no timeout is given, and no job is available to be reserved, beanstalkd will wait to send a response until one becomes available. Commands issued while waiting for the `reserve` callback will be queued and sent in FIFO order, when communication is resumed.

A timeout value of 0 will cause the server to immediately return either a response or TIMED_OUT. A positive value of timeout will limit the amount of time the client will hold communication until a job becomes available.

Once a job is reserved for the client, the client has limited time to run (TTR) the job before the job times out. When the job times out, the server will put the job back into the ready queue. Both the TTR and the actual time left can be found in response to the `stats-job` command.

**`delete(jid, callback=None)`**  
Removes a job from the server entirely. It is normally used by the client when the job has successfully run to completion. A client can delete jobs that it has `reserved`, `ready` jobs, `delayed` jobs, and jobs that are `buried`.

**`release(jid, priority=DEFAULT_PRIORITY, delay=0, callback=None)`**  
Puts a reserved job back into the ready queue (and marks its state as ready) to be run by any client. It is normally used when the job fails because of a transitory error.

**`bury(jid, priority=DEFAULT_PRIORITY, callback=None)`**  
The `bury` command puts a job into the "buried" state. Buried jobs are put into a FIFO linked list and will not be touched by the server again until a client kicks them with the `kick` command.

**`touch(jid, callback=None)`**  
The `touch` command allows a worker to request more time to work on a job. This is useful for jobs that potentially take a long time, but you still want the benefits of a TTR pulling a job away from an unresponsive worker. A worker may periodically tell the server that it’s still alive and processing a job (e.g. it may do this on `DEADLINE_SOON`).

**`watch(name, callback=None)`**  
The `watch` command adds the named tube to the watch list for the current connection. A reserve command will take a job from any of the tubes in the watch list. For each new connection, the watch list initially consists of one tube, named `default`.

**`ignore(name, callback=None)`**  
The `ignore` command is for consumers. It removes the named tube from the watch list for the current connection.

## Other commands

**`peek(jid, callback=None)`**  
**`peek_ready(callback=None)`**  
**`peek_delayed(callback=None)`**  
**`peek_buried(callback=None)`**  
The `peek` commands let the client inspect a job in the system. There are four variations. All but the first operate only on the currently used tube.

**`kick(bound=1, callback=None)`**  
The `kick` command applies only to the currently used tube. It moves jobs into the ready queue. If there are any buried jobs, it will only kick buried jobs.

**`kick_job(jid, callback=None)`**  
The `kick_job` command is a variant of kick that operates with a single job identified by its job id. If the given job id exists and is in a buried or delayed state, it will be moved to the ready queue of the the same tube where it currently belongs.

**`stats_job(jid, callback=None)`**  
The `stats_job` command gives statistical information about the specified job if it exists. The callback gets a Python `dict` containing these keys:

* `id` is the job id (mid)
* `tube` is the name of the tube that contains this job
* `state` is `ready`, `delayed`, `reserved` or `buried`
* `pri` is the priority value set by the `put`, `release`, or `bury` commands.
* `age` is the time in seconds since the `put` command that created this job.
* `time-left` is the number of seconds left until the server puts this job into the ready queue. This number is only meaningful if the job is reserved or delayed. If the job is reserved and this amount of time elapses before its state changes, it is considered to have timed out.
* `file` is the number of the earliest bin log file containing this job. If `-b` flag wasn’t used, this will be 0.
* `reserves` is the number of times this job has been reserved.
* `timeouts` is the number of times this job has timed out during a reservation.
* `releases` is the number of times a client has released this job from a reservation.
* `buries` is the number of times this job has been buried.
* `kicks` is the number of times this job has been kicked.

**`stats_tube(name, callback=None)`**  
The stats-tube command gives statistical information about the specified tube if it exists. The callback gets a Python `dict` containing these keys:

* `name` is the tube’s name.
* `current-jobs-urgent` is the number of ready jobs with priority < 1024 in this tube.
* `current-jobs-ready` is the number of jobs in the ready queue in this tube.
* `current-jobs-reserved` is the number of jobs reserved by all clients in this tube.
* `current-jobs-delayed` is the number of delayed jobs in this tube.
* `current-jobs-buried` is the number of buried jobs in this tube.
* `total-jobs` is the cumulative count of jobs created in this tube in the current beanstalkd process.
* `current-using` is the number of open connections that are currently using this tube.
* `current-waiting` is the number of open connections that have issued a reserve command while watching this tube but not yet received a response.
* `current-watching` is the number of open connections that are currently watching this tube.
* `pause` is the number of seconds the tube has been paused for.
* `cmd-delete` is the cumulative number of delete commands for this tube
* `cmd-pause-tube` is the cumulative number of pause-tube commands for this tube.
* `pause-time-left` is the number of seconds until the tube is un-paused.

Entries described as "cumulative" are reset when the beanstalkd process starts; they are not stored on disk with the `-b` flag.


**`stats(callback=None)`**  
The stats command gives statistical information about the system as a whole. The callback gets a Python `dict` containing these keys:

* `current-jobs-urgent` is the number of ready jobs with priority < 1024.
* `current-jobs-ready` is the number of jobs in the ready queue.
* `current-jobs-reserved` is the number of jobs reserved by all clients.
* `current-jobs-delayed` is the number of delayed jobs.
* `current-jobs-buried` is the number of buried jobs.
* `cmd-put` is the cumulative number of put commands.
* `cmd-peek` is the cumulative number of peek commands.
* `cmd-peek-ready` is the cumulative number of peek-ready commands.
* `cmd-peek-delayed` is the cumulative number of peek-delayed commands.
* `cmd-peek-buried` is the cumulative number of peek-buried commands.
* `cmd-reserve` is the cumulative number of reserve commands.
* `cmd-use` is the cumulative number of use commands.
* `cmd-watch` is the cumulative number of watch commands.
* `cmd-ignore` is the cumulative number of ignore commands.
* `cmd-delete` is the cumulative number of delete commands.
* `cmd-release` is the cumulative number of release commands.
* `cmd-bury` is the cumulative number of bury commands.
* `cmd-kick`` is the cumulative number of kick commands.
* `cmd-stats` is the cumulative number of stats commands.
* `cmd-stats-job` is the cumulative number of stats-job commands.
* `cmd-stats-tube` is the cumulative number of stats-tube commands.
* `cmd-list-tubes` is the cumulative number of list-tubes commands.
* `cmd-list-tube-used` is the cumulative number of list-tube-used commands.
* `cmd-list-tubes-watched` is the cumulative number of list-tubes-watched commands.
* `cmd-pause-tube` is the cumulative number of pause-tube commands
* `job-timeouts` is the cumulative count of times a job has timed out.
* `total-jobs` is the cumulative count of jobs created.
* `max-job-size` is the maximum number of bytes in a job.
* `current-tubes` is the number of currently-existing tubes.
* `current-connections` is the number of currently open connections.
* `current-producers` is the number of open connections that have each issued at least one put command.
* `current-workers` is the number of open connections that have each issued at least one reserve command.
* `current-waiting` is the number of open connections that have issued a reserve command but not yet received a response.
* `total-connections` is the cumulative count of connections.
* `pid is` the process id of the server.
* `version` is the version string of the server.
* `rusage-utime` is the cumulative user CPU time of this process in seconds and microseconds.
* `rusage-stime` is the cumulative system CPU time of this process in seconds and microseconds.
* `uptime` is the number of seconds since this server process started running.
* `binlog-oldest-index` is the index of the oldest bin log file needed to store the current jobs
* `binlog-current-index` is the index of the current bin log file being written to. If bin log is not active this value will be 0
* `binlog-max-size` is the maximum size in bytes a bin log file is allowed to get before a new bin log file is opened
* `binlog-records-written` is the cumulative number of records written to the bin log
* `binlog-records-migrated` is the cumulative number of records written as part of compaction

Entries described as "cumulative" are reset when the beanstalkd process starts; they are not stored on disk with the `-b` flag.

**`list_tubes(callback=None)`**  
The `list_tubes` command calls back with a list of all existing tubes.

**`list_tube_used(callback=None)`**  
The `list_tube_used` command calls back with the name of the tube currently being used by the client.

**`list_tubes_watched(self, callback=None)`**  
The `list_tubes_watched` command calls back with a list of tubes currently being watched by the client.

**`pause_tube(name, delay, callback=None)`**  
The `pause_tube` command can delay any new job being reserved for a given time.

## Implementation notes

Tests are contained in `btc_test.py` and all tests cases can be run by `python btc_test.py`. 

The beanstalkd protocol uses YAML for communicating the various stats and lists. The client has a crude YAML parser, suitable only for parsing simple lists and dicts, which eliminates the dependency of a YAML parser.
