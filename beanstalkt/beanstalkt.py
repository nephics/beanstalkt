#!/usr/bin/env python
"""beanstalkt - An async beanstalkd client for Tornado"""

__license__ = """
Copyright (C) 2012-2013 Nephics AB

Parts of the code adopted from the beanstalkc project are:
    Copyright (C) 2008-2012 Andreas Bolka

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

__version__ = '0.4.0'

import socket
import time

from collections import deque

from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado import stack_context


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

    def connect(self, callback=None):
        """Connect to beanstalkd server."""
        if not self.closed():
            return
        self._talking = False
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM,
                socket.IPPROTO_TCP)
        self._stream = IOStream(self._socket, io_loop=self.io_loop)
        self._stream.set_close_callback(self._reconnect)
        self._stream.connect((self.host, self.port), callback)

    def set_reconnect_callback(self, callback):
        """Set callback to be called if connection has been lost and
        re-established again.

        If the connection is closed unexpectedly, the client will automatically
        attempt to re-connect with 1 second intervals. After re-connecting, the
        client will attempt to re-establish the used tube and watched tubes.
        """
        self._reconnect_cb = callback

    def close(self, callback=None):
        """Close connection to server."""
        if self._stream:
            self._stream.set_close_callback(callback)
        if self.closed():
            # already closed
            callback()
        else:
            self._stream.write('quit\r\n', self._stream.close)

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
            command = req.cmd + '\r\n'
            if req.body:
                command += req.body + '\r\n'

            # write command and body to socket stream
            self._stream.write(command,
                    # when command is written: read line from socket stream
                    lambda: self._stream.read_until('\r\n',
                    # when a line has been read: return status and results
                    lambda data: self._recv(req, data, cb)))

    def _recv(self, req, data, cb):
        # parse the data received as server response
        spl = data.split()
        status, values = spl[0], spl[1:]

        error = None
        err_args = dict(request=req, status=status, values=values)

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
                jid, size = int(values[0]), int(values[1])
                resp.jid = int(jid)
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
            resp.body = {'jid': resp.jid, 'body': data}
            self._do_callback(cb, resp)

    def _parse_yaml(self, data, resp, cb):
        # dirty parsing of yaml data
        # (assumes that data is a yaml encoded list or dict)
        spl = data.split('\n')[1:-1]
        if spl[0].startswith('- '):
            # it is a list
            resp.body = [s[2:] for s in spl]
        else:
            # it is a dict
            conv = lambda v: ((float(v) if '.' in v else int(v))
                if v.replace('.', '').isdigit() else v)
            resp.body = dict((k, conv(v.strip())) for k, v in
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

    def put(self, body, priority=DEFAULT_PRIORITY, delay=0, ttr=120,
            callback=None):
        """Put a job body (a byte string) into the current tube.

        The job can be delayed a number of seconds, before it is put in the
        ready queue, default is no delay.

        The job is assigned a Time To Run (ttr, in seconds), the mininum is
        1 sec., default is ttr=120 sec.

        Calls back with jid (job id) when job is inserted. If an error occured,
        the callback gets a Buried or CommandFailed exception. The job is
        buried when either the body is too big, so server ran out of memory,
        or when the server is in draining mode.
        """
        request = Bunch(cmd='put {} {} {} {}'.format(priority, delay, ttr,
                len(body)), ok=['INSERTED'], err=['BURIED', 'JOB_TOO_BIG',
                'DRAINING'], body=body, read_value=True)
        self._interact(request, callback)

    def use(self, name, callback=None):
        """Use the tube with given name.

        Calls back with the name of the tube now being used.
        """
        def using(resp):
            if not isinstance(resp, Exception):
                self._using = resp
            if callback:
                callback(resp)

        request = Bunch(cmd='use {}'.format(name), ok=['USING'],
                read_value=True)
        self._interact(request, using)

    #
    #  Worker commands
    #

    def reserve(self, timeout=None, callback=None):
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

        Calls back with a job dict (jid and body). If the request timed out,
        the callback gets a TimedOut exception. If a reserved job has deadline
        within the next second, the callback gets a DeadlineSoon exception.
        """
        if timeout is not None:
            command = 'reserve-with-timeout {}'.format(timeout)
        else:
            command = 'reserve'
        request = Bunch(cmd=command, ok=['RESERVED'], err=['DEADLINE_SOON',
                'TIMED_OUT'], read_body=True)
        self._interact(request, callback)

    def delete(self, jid, callback=None):
        """Delete job with given jid.

        Calls back when job is deleted. If the job does not exist, or it is not
        neither reserved by the client, ready or buried; the callback gets a
        CommandFailed exception.
        """
        request = Bunch(cmd='delete {}'.format(jid), ok=['DELETED'],
                err=['NOT_FOUND'])
        self._interact(request, callback)

    def release(self, jid, priority=DEFAULT_PRIORITY, delay=0, callback=None):
        """Release a reserved job back into the ready queue.

        A new priority can be assigned to the job.

        It is also possible to specify a delay (in seconds) to wait before
        putting the job in the ready queue. The job will be in the "delayed"
        state during this time.

        Calls back when job is released. If the job was buried, the callback
        gets a Buried exception. If the job does not exist, or it is not
        reserved by the client, the callback gets a CommandFailed exception.
        """
        request = Bunch(cmd='release {} {} {}'.format(jid, priority, delay),
                ok=['RELEASED'], err=['BURIED', 'NOT_FOUND'])
        self._interact(request, callback)

    def bury(self, jid, priority=DEFAULT_PRIORITY, callback=None):
        """Bury job with given jid.

        A new priority can be assigned to the job.

        Calls back when job is burried. If the job does not exist, or it is not
        reserved by the client, the callback gets a CommandFailed exception.
        """
        request = Bunch(cmd='bury {} {}'.format(jid, priority), ok=['BURIED'],
                err=['NOT_FOUND'])
        self._interact(request, callback)

    def touch(self, jid, callback=None):
        """Touch job with given jid.

        This is for requesting more time to work on a reserved job before it
        expires.

        Calls back when job is touched. If the job does not exist, or it is not
        reserved by the client, the callback gets a CommandFailed exception.
        """
        request = Bunch(cmd='touch {}'.format(jid), ok=['TOUCHED'],
                err=['NOT_FOUND'])
        self._interact(request, callback)

    def watch(self, name, callback=None):
        """Watch tube with given name.

        Calls back with number of tubes currently in the watch list.
        """
        def watching(count):
            if not isinstance(count, Exception):
                # add to the client's watch list
                self._watching.add(name)
            if callback:
                callback(count)

        request = Bunch(cmd='watch {}'.format(name), ok=['WATCHING'],
                read_value=True)
        self._interact(request, watching)

    def ignore(self, name, callback=None):
        """Stop watching tube with given name.

        Calls back with the number of tubes currently in the watch list. On an
        attempt to ignore the only tube in the watch list, the callback gets a
        CommandFailed exception.
        """
        def ignoring(count):
            if not isinstance(count, Exception) and name in self._watching:
                # remove from the client's watch list
                self._watching.remove(name)
            if callback:
                callback(count)

        request = Bunch(cmd='ignore {}'.format(name), ok=['WATCHING'],
                err=['NOT_IGNORED'], read_value=True)
        self._interact(request, ignoring)

    #
    #  Other commands
    #

    def _peek(self, variant, callback):
        # a shared gateway for the peek* commands
        request = Bunch(cmd='peek{}'.format(variant), ok=['FOUND'],
                err=['NOT_FOUND'], read_body=True)
        self._interact(request, callback)

    def peek(self, jid, callback=None):
        """Peek at job with given jid.

        Calls back with a job dict (jid and body). If no job exists with that
        jid, the callback gets a CommandFailed exception.
        """
        self._peek(' {}'.format(jid), callback)

    def peek_ready(self, callback=None):
        """Peek at next ready job in the current tube.

        Calls back with a job dict (jid and body). If no ready jobs exist,
        the callback gets a CommandFailed exception.
        """
        self._peek('-ready', callback)

    def peek_delayed(self, callback=None):
        """Peek at next delayed job in the current tube.

        Calls back with a job dict (jid and body). If no delayed jobs exist,
        the callback gets a CommandFailed exception.
        """
        self._peek('-delayed', callback)

    def peek_buried(self, callback=None):
        """Peek at next buried job in the current tube.

        Calls back with a job dict (jid and body). If no buried jobs exist,
        the callback gets a CommandFailed exception.
        """
        self._peek('-buried', callback)

    def kick(self, bound=1, callback=None):
        """Kick at most `bound` jobs into the ready queue from the current tube.

        Calls back with the number of jobs actually kicked.
        """
        request = Bunch(cmd='kick {}'.format(bound), ok=['KICKED'],
                read_value=True)
        self._interact(request, callback)

    def kick_job(self, jid, callback=None):
        """Kick job with given id into the ready queue.
        (Requires Beanstalkd version >= 1.8)

        Calls back when job is kicked. If no job exists with that jid, or if
        job is not in a kickable state, the callback gets a CommandFailed
        exception.
        """
        request = Bunch(cmd='kick-job {}'.format(jid), ok=['KICKED'],
                err=['NOT_FOUND'])
        self._interact(request, callback)

    def stats_job(self, jid, callback=None):
        """A dict of stats about the job with given jid.

        If no job exists with that jid, the callback gets a CommandFailed
        exception.
        """
        request = Bunch(cmd='stats-job {}'.format(jid), ok=['OK'],
                err=['NOT_FOUND'], read_body=True, parse_yaml=True)
        self._interact(request, callback)

    def stats_tube(self, name, callback=None):
        """A dict of stats about the tube with given name.

        If no tube exists with that name, the callback gets a CommandFailed
        exception.
        """
        request = Bunch(cmd='stats-tube {}'.format(name), ok=['OK'],
                err=['NOT_FOUND'], read_body=True, parse_yaml=True)
        self._interact(request, callback)

    def stats(self, callback=None):
        """A dict of beanstalkd statistics."""
        request = Bunch(cmd='stats', ok=['OK'], read_body=True,
                parse_yaml=True)
        self._interact(request, callback)

    def list_tubes(self, callback=None):
        """List of all existing tubes."""
        request = Bunch(cmd='list-tubes', ok=['OK'], read_body=True,
                parse_yaml=True)
        self._interact(request, callback)

    def list_tube_used(self, callback=None):
        """Name of the tube currently being used."""
        request = Bunch(cmd='list-tube-used', ok=['USING'], read_value=True)
        self._interact(request, callback)

    def list_tubes_watched(self, callback=None):
        """List of tubes currently being watched."""
        request = Bunch(cmd='list-tubes-watched', ok=['OK'], read_body=True,
                parse_yaml=True)
        self._interact(request, callback)

    def pause_tube(self, name, delay, callback=None):
        """Delay any new job being reserved from the tube for a given time.

        The delay is an integer number of seconds to wait before reserving any
        more jobs from the queue.

        Calls back when tube is paused. If tube does not exists, the callback
        will get a CommandFailed exception.
        """
        request = Bunch(cmd='pause-tube {} {}'.format(name, delay),
                ok=['PAUSED'], err=['NOT_FOUND'])
        self._interact(request, callback)
