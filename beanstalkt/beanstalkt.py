#!/usr/bin/env python
"""beanstalkt - An async beanstalkd client for Tornado"""

__license__ = """
Copyright (C) 2012-2014 Nephics AB
Copyright (C) 2008-2012 Andreas Bolka  (beanstalkc project)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

__version__ = '0.7.0'

import socket
import time

from collections import deque

from tornado.gen import coroutine, Task, Return, Wait, Callback
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado import stack_context
from tornado import version as tornado_version
from tornado.util import ObjectDict


DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120  # Time (in seconds) To Run a job, min. 1 sec.
RECONNECT_TIMEOUT = 1  # Time (in seconds) between re-connection attempts


class Bunch:
    """Create a bunch to group a few variables.
    Undefined attributes have the default value of None.
    """
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __getattr__(self, name):
        return None


class BeanstalkException(Bunch, Exception):
    def __str__(self):
        return '{}: {} in reply to command {}'.format(
            self.__class__.__name__, self.status, self.request.cmd)


class UnexpectedResponse(BeanstalkException): pass
class CommandFailed(BeanstalkException): pass
class Buried(BeanstalkException): pass
class DeadlineSoon(BeanstalkException): pass
class TimedOut(BeanstalkException): pass


class Client(object):

    def __init__(self, host='localhost', port=11300,
                 connect_timeout=socket.getdefaulttimeout(), io_loop=None):
        self._connect_timeout = connect_timeout
        self.host = host
        self.port = port
        self.io_loop = io_loop or IOLoop.instance()
        self._stream = None
        self._using = 'default'  # current tube
        self._watching = set(['default'])   # set of watched tubes
        self._queue = deque()
        self._talking = False
        self._reconnect_cb = None

    def _reconnect(self):
        # wait some time before trying to re-connect
        self.io_loop.add_timeout(time.time() + RECONNECT_TIMEOUT,
                lambda: self.connect(self._reconnected))

    def _reconnected(self):
        # re-establish the used tube and tubes being watched
        watch = self._watching.difference(['default'])
        # ignore "default", if it is not in the client's watch list
        ignore = set(['default']).difference(self._watching)

        def do_next(_=None):
            try:
                if watch:
                    self.watch(watch.pop(), do_next)
                elif ignore:
                    self.ignore(ignore.pop(), do_next)
                elif self._using != 'default':
                    # change the tube used, and callback to user
                    self.use(self._using, self._reconnect_cb)
                elif self._reconnect_cb:
                    # callback to user
                    self._reconnect_cb()
            except:
                # ignored, as next re-connect will retry the operation
                pass

        do_next()

    @coroutine
    def connect(self):
        """Connect to beanstalkd server."""
        if not self.closed():
            return
        self._talking = False
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM,
                socket.IPPROTO_TCP)
        if tornado_version >= '5.0':
            self._stream = IOStream(self._socket)
        else:
            self._stream = IOStream(self._socket, io_loop=self.io_loop)
        self._stream.set_close_callback(self._reconnect)
        yield Task(self._stream.connect, (self.host, self.port))

    def set_reconnect_callback(self, callback):
        """Set callback to be called if connection has been lost and
        re-established again.

        If the connection is closed unexpectedly, the client will automatically
        attempt to re-connect with 1 second intervals. After re-connecting, the
        client will attempt to re-establish the used tube and watched tubes.
        """
        self._reconnect_cb = callback

    @coroutine
    def close(self):
        """Close connection to server."""
        key = object()
        if self._stream:
            self._stream.set_close_callback((yield Callback(key)))
        if not self.closed():
            yield Task(self._stream.write, b'quit\r\n')
            self._stream.close()
            yield Wait(key)

    def closed(self):
        """"Returns True if the connection is closed."""
        return not self._stream or self._stream.closed()

    def _interact(self, request, callback):
        # put the interaction request into the FIFO queue
        cb = stack_context.wrap(callback)
        self._queue.append((request, cb))
        self._process_queue()

    def _process_queue(self):
        if self._talking or not self._queue:
            return
        # pop a request of the queue and perform the send-receive interaction
        self._talking = True
        with stack_context.NullContext():
            req, cb = self._queue.popleft()
            command = req.cmd + b'\r\n'
            if req.body:
                command += req.body + b'\r\n'

            # write command and body to socket stream
            self._stream.write(command,
                    # when command is written: read line from socket stream
                    lambda: self._stream.read_until(b'\r\n',
                    # when a line has been read: return status and results
                    lambda data: self._recv(req, data, cb)))

    def _recv(self, req, data, cb):
        # parse the data received as server response
        spl = data.decode('utf8').split()
        status, values = spl[0], spl[1:]

        error = None
        err_args = ObjectDict(request=req, status=status, values=values)

        if req.ok and status in req.ok:
            # avoid raising a Buried exception when using the bury command
            pass
        elif status == 'BURIED':
            error = Buried(**err_args)
        elif status == 'TIMED_OUT':
            error = TimedOut(**err_args)
        elif status == 'DEADLINE_SOON':
            error = DeadlineSoon(**err_args)
        elif req.err and status in req.err:
            error = CommandFailed(**err_args)
        else:
            error = UnexpectedResponse(**err_args)

        resp = Bunch(req=req, status=status, values=values, error=error)

        if error or not req.read_body:
            # end the request and callback with results
            self._do_callback(cb, resp)
        else:
            # read the body including the terminating two bytes of crlf
            if len(values) == 2:
                job_id, size = int(values[0]), int(values[1])
                resp.job_id = int(job_id)
            else:
                size = int(values[0])
            self._stream.read_bytes(size + 2,
                    lambda data: self._recv_body(data[:-2], resp, cb))

    def _recv_body(self, data, resp, cb):
        if resp.req.parse_yaml:
            # parse the yaml encoded body
            self._parse_yaml(data, resp, cb)
        else:
            # don't parse body, it is a job!
            # end the request and callback with results
            resp.body = ObjectDict(id=resp.job_id, body=data)
            self._do_callback(cb, resp)

    def _parse_yaml(self, data, resp, cb):
        # dirty parsing of yaml data
        # (assumes that data is a yaml encoded list or dict)
        spl = data.decode('utf8').split('\n')[1:-1]
        if spl[0].startswith('- '):
            # it is a list
            resp.body = [s[2:] for s in spl]
        else:
            # it is a dict
            conv = lambda v: ((float(v) if '.' in v else int(v))
                if v.replace('.', '', 1).isdigit() else v)
            resp.body = ObjectDict((k, conv(v.strip())) for k, v in
                    (s.split(':') for s in spl))
        self._do_callback(cb, resp)

    def _do_callback(self, cb, resp):
        # end the request and process next item in the queue
        # and callback with results
        self._talking = False
        self.io_loop.add_callback(self._process_queue)

        if not cb:
            return

        # default is to callback with error state (None or exception)
        obj = None
        req = resp.req

        if resp.error:
            obj = resp.error

        elif req.read_value:
            # callback with an integer value or a string
            if resp.values[0].isdigit():
                obj = int(resp.values[0])
            else:
                obj = resp.values[0]

        elif req.read_body:
            # callback with the body (job or parsed yaml)
            obj = resp.body

        self.io_loop.add_callback(lambda: cb(obj))

    #
    #  Producer commands
    #

    @coroutine
    def put(self, body, priority=DEFAULT_PRIORITY, delay=0, ttr=120):
        """Put a job body (a byte string) into the current tube.

        The job can be delayed a number of seconds, before it is put in the
        ready queue, default is no delay.

        The job is assigned a Time To Run (ttr, in seconds), the mininum is
        1 sec., default is ttr=120 sec.

        Calls back with id when job is inserted. If an error occured,
        the callback gets a Buried or CommandFailed exception. The job is
        buried when either the body is too big, so server ran out of memory,
        or when the server is in draining mode.
        """
        cmd = 'put {} {} {} {}'.format(priority, delay, ttr,
            len(body)).encode('utf8')
        assert isinstance(body, bytes)
        request = Bunch(cmd=cmd, ok=['INSERTED'], err=['BURIED', 'JOB_TOO_BIG',
                'DRAINING'], body=body, read_value=True)
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def use(self, name):
        """Use the tube with given name.

        Calls back with the name of the tube now being used.
        """
        cmd = 'use {}'.format(name).encode('utf8')
        request = Bunch(cmd=cmd, ok=['USING'],
                read_value=True)
        resp = yield Task(self._interact, request)
        if not isinstance(resp, Exception):
            self._using = resp
        raise Return(resp)

    #
    #  Worker commands
    #

    @coroutine
    def reserve(self, timeout=None):
        """Reserve a job from one of the watched tubes, with optional timeout
        in seconds.

        Not specifying a timeout (timeout=None, the default) will make the
        client put the communication with beanstalkd on hold, until either a
        job is reserved, or a already reserved job is approaching it's TTR
        deadline. Commands issued while waiting for the "reserve" callback will
        be queued and sent in FIFO order, when communication is resumed.

        A timeout value of 0 will cause the server to immediately return either
        a response or TIMED_OUT. A positive value of timeout will limit the
        amount of time the client will will hold communication until a job
        becomes available.

        Calls back with a job dict (keys id and body). If the request timed out,
        the callback gets a TimedOut exception. If a reserved job has deadline
        within the next second, the callback gets a DeadlineSoon exception.
        """
        if timeout is not None:
            cmd = 'reserve-with-timeout {}'.format(timeout).encode('utf8')
        else:
            cmd = b'reserve'
        request = Bunch(cmd=cmd, ok=['RESERVED'], err=['DEADLINE_SOON',
                'TIMED_OUT'], read_body=True)
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def delete(self, job_id):
        """Delete job with given id.

        Calls back when job is deleted. If the job does not exist, or it is not
        neither reserved by the client, ready or buried; the callback gets a
        CommandFailed exception.
        """
        cmd = 'delete {}'.format(job_id).encode('utf8')
        request = Bunch(cmd=cmd, ok=['DELETED'], err=['NOT_FOUND'])
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def release(self, job_id, priority=DEFAULT_PRIORITY, delay=0):
        """Release a reserved job back into the ready queue.

        A new priority can be assigned to the job.

        It is also possible to specify a delay (in seconds) to wait before
        putting the job in the ready queue. The job will be in the "delayed"
        state during this time.

        Calls back when job is released. If the job was buried, the callback
        gets a Buried exception. If the job does not exist, or it is not
        reserved by the client, the callback gets a CommandFailed exception.
        """
        cmd = 'release {} {} {}'.format(job_id, priority, delay).encode('utf8')
        request = Bunch(cmd=cmd, ok=['RELEASED'], err=['BURIED', 'NOT_FOUND'])
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def bury(self, job_id, priority=DEFAULT_PRIORITY):
        """Bury job with given id.

        A new priority can be assigned to the job.

        Calls back when job is burried. If the job does not exist, or it is not
        reserved by the client, the callback gets a CommandFailed exception.
        """
        cmd = 'bury {} {}'.format(job_id, priority).encode('utf8')
        request = Bunch(cmd=cmd, ok=['BURIED'], err=['NOT_FOUND'])
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def touch(self, job_id):
        """Touch job with given id.

        This is for requesting more time to work on a reserved job before it
        expires.

        Calls back when job is touched. If the job does not exist, or it is not
        reserved by the client, the callback gets a CommandFailed exception.
        """
        cmd = 'touch {}'.format(job_id).encode('utf8')
        request = Bunch(cmd=cmd, ok=['TOUCHED'], err=['NOT_FOUND'])
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def watch(self, name):
        """Watch tube with given name.

        Calls back with number of tubes currently in the watch list.
        """
        cmd = 'watch {}'.format(name).encode('utf8')
        request = Bunch(cmd=cmd, ok=['WATCHING'], read_value=True)
        resp = yield Task(self._interact, request)
        # add to the client's watch list
        self._watching.add(name)
        raise Return(resp)

    @coroutine
    def ignore(self, name):
        """Stop watching tube with given name.

        Calls back with the number of tubes currently in the watch list. On an
        attempt to ignore the only tube in the watch list, the callback gets a
        CommandFailed exception.
        """
        cmd = 'ignore {}'.format(name).encode('utf8')
        request = Bunch(cmd=cmd, ok=['WATCHING'], err=['NOT_IGNORED'],
                read_value=True)
        resp = yield Task(self._interact, request)
        if name in self._watching:
            # remove from the client's watch list
            self._watching.remove(name)
        raise Return(resp)

    #
    #  Other commands
    #

    def _peek(self, variant, callback):
        # a shared gateway for the peek* commands
        cmd = 'peek{}'.format(variant).encode('utf8')
        request = Bunch(cmd=cmd, ok=['FOUND'], err=['NOT_FOUND'],
                read_body=True)
        self._interact(request, callback)

    @coroutine
    def peek(self, job_id):
        """Peek at job with given id.

        Calls back with a job dict (keys id and body). If no job exists with
        that id, the callback gets a CommandFailed exception.
        """
        resp = yield Task(self._peek, ' {}'.format(job_id))
        raise Return(resp)

    @coroutine
    def peek_ready(self):
        """Peek at next ready job in the current tube.

        Calls back with a job dict (keys id and body). If no ready jobs exist,
        the callback gets a CommandFailed exception.
        """
        resp = yield Task(self._peek, '-ready')
        raise Return(resp)

    @coroutine
    def peek_delayed(self):
        """Peek at next delayed job in the current tube.

        Calls back with a job dict (keys id and body). If no delayed jobs exist,
        the callback gets a CommandFailed exception.
        """
        resp = yield Task(self._peek, '-delayed')
        raise Return(resp)

    @coroutine
    def peek_buried(self):
        """Peek at next buried job in the current tube.

        Calls back with a job dict (keys id and body). If no buried jobs exist,
        the callback gets a CommandFailed exception.
        """
        resp = yield Task(self._peek, '-buried')
        raise Return(resp)

    @coroutine
    def kick(self, bound=1):
        """Kick at most `bound` jobs into the ready queue from the current tube.

        Calls back with the number of jobs actually kicked.
        """
        cmd = 'kick {}'.format(bound).encode('utf8')
        request = Bunch(cmd=cmd, ok=['KICKED'], read_value=True)
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def kick_job(self, job_id):
        """Kick job with given id into the ready queue.
        (Requires Beanstalkd version >= 1.8)

        Calls back when job is kicked. If no job exists with that id, or if
        job is not in a kickable state, the callback gets a CommandFailed
        exception.
        """
        cmd = 'kick-job {}'.format(job_id).encode('utf8')
        request = Bunch(cmd=cmd, ok=['KICKED'], err=['NOT_FOUND'])
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def stats_job(self, job_id):
        """A dict of stats about the job with given id.

        If no job exists with that id, the callback gets a CommandFailed
        exception.
        """
        cmd = 'stats-job {}'.format(job_id).encode('utf8')
        request = Bunch(cmd=cmd, ok=['OK'], err=['NOT_FOUND'], read_body=True,
                parse_yaml=True)
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def stats_tube(self, name):
        """A dict of stats about the tube with given name.

        If no tube exists with that name, the callback gets a CommandFailed
        exception.
        """
        cmd = 'stats-tube {}'.format(name).encode('utf8')
        request = Bunch(cmd=cmd, ok=['OK'], err=['NOT_FOUND'], read_body=True,
                parse_yaml=True)
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def stats(self):
        """A dict of beanstalkd statistics."""
        request = Bunch(cmd=b'stats', ok=['OK'], read_body=True,
                parse_yaml=True)
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def list_tubes(self):
        """List of all existing tubes."""
        request = Bunch(cmd=b'list-tubes', ok=['OK'], read_body=True,
                parse_yaml=True)
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def list_tube_used(self):
        """Name of the tube currently being used."""
        request = Bunch(cmd=b'list-tube-used', ok=['USING'], read_value=True)
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def list_tubes_watched(self):
        """List of tubes currently being watched."""
        request = Bunch(cmd=b'list-tubes-watched', ok=['OK'], read_body=True,
                parse_yaml=True)
        resp = yield Task(self._interact, request)
        raise Return(resp)

    @coroutine
    def pause_tube(self, name, delay):
        """Delay any new job being reserved from the tube for a given time.

        The delay is an integer number of seconds to wait before reserving any
        more jobs from the queue.

        Calls back when tube is paused. If tube does not exists, the callback
        will get a CommandFailed exception.
        """
        cmd = 'pause-tube {} {}'.format(name, delay).encode('utf8')
        request = Bunch(cmd=cmd, ok=['PAUSED'], err=['NOT_FOUND'])
        resp = yield Task(self._interact, request)
        raise Return(resp)
