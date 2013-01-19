#!/usr/bin/env python
"""beanstalktc - An async beanstalkd client for Tornado"""

__license__ = '''
Copyright (C) 2012 Nephics AB

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
'''

__version__ = '0.1.0'

import socket

from tornado.ioloop import IOLoop
from tornado.iostream import IOStream


DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 120  # Time (in seconds) To Run a job, min. 1 sec.


class BeanstalktcException(Exception): pass
class UnexpectedResponse(BeanstalktcException): pass
class CommandFailed(BeanstalktcException): pass
class Buried(BeanstalktcException): pass
class DeadlineSoon(BeanstalktcException): pass


class Client(object):

    def __init__(self, host='localhost', port=11300,
                 connect_timeout=socket.getdefaulttimeout(), io_loop=None):
        self._connect_timeout = connect_timeout
        self.host = host
        self.port = port
        self.io_loop = io_loop or IOLoop.instance()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM,
                socket.IPPROTO_TCP)
        self._stream = IOStream(self._socket, io_loop=self.io_loop)

    def connect(self, callback=None):
        """Connect to beanstalkd server."""
        self._stream.connect((self.host, self.port), callback)

    def close(self, callback=None):
        """Close connection to server."""
        self._stream.set_close_callback(callback)
        self._stream.write('quit\r\n', self._stream.close)

    def closed(self):
        '''Returns true if the connection has been closed.'''
        return self._stream.closed()

    def _interact(self, command, callback, expected_ok=[], expected_err=[]):
        # write command to socket stream
        self._stream.write(command,
                # when command is written: read line from socket stream
                lambda: self._stream.read_until('\r\n',
                # when a line has been read: return status and results
                lambda resp: self._parse_response(resp, command, expected_ok,
                expected_err, callback)))

    def _parse_response(self, resp, command, expected_ok, expected_err,
            callback):
        spl = resp.split()
        status, results = spl[0], spl[1:]
        if status in expected_ok:
            callback(results)
            return
        cmd = command.split('\r\n')[0]
        if status == 'BURIED':
            raise Buried(cmd, status, int(results[0])
                    if results and results[0].isdigit() else results)
        elif status in expected_err:
            raise CommandFailed(cmd, status, results)
        else:
            raise UnexpectedResponse(cmd, status, results)

    def _interact_success(self, command, callback, expected_ok=[],
            expected_err=[]):
        self._interact(command, lambda r: callback and callback(), expected_ok,
                expected_err)

    def _interact_value(self, command, callback, expected_ok=[],
            expected_err=[]):
        # callback with a string or integer value
        self._interact(command, lambda r: callback and callback(int(r[0])
                if r[0].isdigit() else r[0]), expected_ok, expected_err)

    def _interact_peek(self, command, callback):
        # callback with a job or None
        try:
            self._interact(command,
                lambda r: self._read_job(int(r[0]), int(r[1]), callback,
                reserved=False), ['FOUND'], ['NOT_FOUND'])
        except CommandFailed:
            callback and callback(None)

    def _read_job(self, jid, size, callback, reserved=True):
        # read the body including the terminating two bytes of crlf
        self._stream.read_bytes(size + 2, lambda resp: callback and callback({
                'jid': jid, 'body': resp[:-2], 'reserved': reserved}))

    def _interact_data(self, command, callback, expected_ok=[],
            expected_err=[]):
        self._interact(command,
                # on success, read data bytes including the terminating two
                # bytes of crlf, and parse the yaml data
                lambda r: self._stream.read_bytes(int(r[0]) + 2,
                lambda data: self._parse_yaml(data, callback)),
                expected_ok, expected_err)

    def _parse_yaml(self, data, callback):
        # dirty parsing of yaml data (assume either list or dict)
        spl = data[:-2].split('\n')[1:-1]
        if spl[0].startswith('- '):
            # it is a list
            callback([s[2:] for s in spl])
        else:
            # it is a dict
            conv = lambda v: ((float(v) if '.' in v else int(v))
                if v[0].isdigit() or v[-1].isdigit() else v)
            callback(dict((k, conv(v.strip())) for k, v in
                    (s.split(':') for s in spl)))

    #
    #  Producer commands
    #

    def put(self, body, priority=DEFAULT_PRIORITY, delay=0, ttr=120,
            callback=None):
        """Put a job body (a string) into the current tube.

        The job can be delayed a number of seconds, before it is put in the
        ready queue, default is no delay.

        The job is assigned a Time To Run (ttr, in seconds), the mininum is
        1 sec., default ttr=120 sec.
        
        Calls back with job id when inserted.
        
        Raises a Buried or CommandFailed exception if job is buried (the
        server ran out of memory trying to grow the priority queue data 
        structure), the body is too big or the server is in draining mode.
        """
        self._interact_value('put %d %d %d %d\r\n%s\r\n' %
                (priority, delay, ttr, len(body), body),
                callback, ['INSERTED'], ['BURIED', 'JOB_TOO_BIG',
                'DRAINING'])

    def use(self, name, callback=None):
        """Use the tube with given name.
    
        Calls back with the name of the tube now being used.
        """
        self._interact_value('use %s\r\n' % name, callback, ['USING'])
    
    #
    #  Worker commands
    #

    def reserve(self, timeout=None, callback=None):
        """Reserve a job from one of the watched tubes, with optional timeout
        in seconds.

        Calls back with a job dict, with keys jid (job id), body (string), and
        reserved (boolean). If request times out, call back with None.

        Raises a DeadlineSoon exception if ....
        """
        if timeout is not None:
            command = 'reserve-with-timeout %d\r\n' % timeout
        else:
            command = 'reserve\r\n'
        try:
            self._interact(command,
                    lambda r: self._read_job(int(r[0]), int(r[1]), callback),
                    ['RESERVED'], ['DEADLINE_SOON', 'TIMED_OUT'])
        except CommandFailed as (_, status, results):
            if status == 'TIMED_OUT':
                callback and callback(None)
            elif status == 'DEADLINE_SOON':
                raise DeadlineSoon(results)

    def delete(self, jid, callback=None):
        """Delete a job, by job id.
    
        Call back when job is deleted.
    
        A CommandFailed exception is raised if job id does not exists.
        """
        self._interact_success('delete %d\r\n' % jid, callback, ['DELETED'],
                ['NOT_FOUND'])
    
    def release(self, jid, priority=DEFAULT_PRIORITY, delay=0, callback=None):
        """Release a reserved job back into the ready queue.
    
        Call back when job is released or buried (if the server ran out of
        memory trying to grow the priority queue data structure).
    
        A CommandFailed exception is raised if the job does not exist or is not
        reserved by the client.
        """
        self._interact_success('release %d %d %d\r\n' % (jid, priority, delay),
                callback, ['RELEASED'], ['BURIED', 'NOT_FOUND'])
    
    def bury(self, jid, priority=DEFAULT_PRIORITY, callback=None):
        """Bury a job, by job id.
    
        Call back when job is buried.
    
        A CommandFailed exception is raised if the job does not exist or is not
        reserved by the client.
        """
        self._interact_success('bury %d %d\r\n' % (jid, priority), callback,
                ['BURIED'], ['NOT_FOUND'])
    
    def touch(self, jid, callback=None):
        """Touch a job, by job id, requesting more time to work on a reserved
        job before it expires.
    
        Call back when job is touched.
    
        A CommandFailed exception is raised if the job does not exist or is not
        reserved by the client.
        """
        self._interact_success('touch %d\r\n' % jid, callback, ['TOUCHED'],
                ['NOT_FOUND'])

    def watch(self, name, callback=None):
        """Watch a given tube.
    
        Call back with number of tubes currently in the watch list.
        """
        self._interact_value('watch %s\r\n' % name, callback, ['WATCHING'])

    def ignore(self, name, callback=None):
        """Stop watching a given tube.
    
        Call back with number of tubes currently in the watch list.
    
        A CommandFailed exception is raised on an attempt to ignore the only
        tube in the watch list.
        """
        self._interact_value('ignore %s\r\n' % name, callback, ['WATCHING'],
            ['NOT_IGNORED'])

    #
    #  Other commands
    #

    def peek(self, jid, callback=None):
        """Peek at a job.
    
        Call back with a job dict or None.
        """
        self._interact_peek('peek %d\r\n' % jid, callback)
    
    def peek_ready(self, callback=None):
        """Peek at next ready job in the current tube.
    
        Call back with a job dict or None.
        """
        self._interact_peek('peek-ready\r\n', callback)
    
    def peek_delayed(self, callback=None):
        """Peek at next delayed job in the current tube.
    
        Call back with a job dict or None.
        """
        self._interact_peek('peek-delayed\r\n', callback)
    
    def peek_buried(self, callback=None):
        """Peek at next buried job in the current tube.
    
        Call back with a job dict or None.
        """
        self._interact_peek('peek-buried\r\n', callback)
    
    def kick(self, bound=1, callback=None):
        """Kick at most bound jobs into the ready queue.

        Call back with the number of jobs actually kicked.
        """
        self._interact_value('kick %d\r\n' % bound, callback, ['KICKED'])

    def kick_job(self, jid, callback=None):
        """Kick job with given id into the ready queue.
        (Requires Beanstalkd version >= 1.8)

        Call back if job is kicked.

        A CommandFailed exception is raised if job does not exists or is not in
        a kickable state.
        """
        self._interact_success('kick-job %d\r\n' % jid, callback, ['KICKED'],
                ['NOT_FOUND'])

    def stats_job(self, jid, callback=None):
        """Return a dict of stats about a job, by job id."""
        self._interact_data('stats-job %d\r\n' % jid, callback,
                ['OK'], ['NOT_FOUND'])

    def stats_tube(self, name, callback=None):
        """Return a dict of stats about a given tube.
    
        A CommandFailed exception is raised if tube does not exists.
        """
        self._interact_data('stats-tube %s\r\n' % name, callback,
                ['OK'], ['NOT_FOUND'])
    
    def stats(self, callback=None):
        """Call back with a dict of beanstalkd statistics."""
        return self._interact_data('stats\r\n', callback, ['OK'])
    
    def list_tubes(self, callback=None):
        """Call back with a list of all existing tubes."""
        self._interact_data('list-tubes\r\n', callback, ['OK'])

    def list_tube_used(self, callback=None):
        """Call back with a name of tube currently being used."""
        self._interact_value('list-tube-used\r\n', callback, ['USING'])

    def list_tubes_watched(self, callback=None):
        """Call back with a list of all tubes being watched."""
        self._interact_data('list-tubes-watched\r\n', callback, ['OK'])

    def pause_tube(self, name, delay, callback=None):
        """Pause a tube for a given delay time, in seconds.

        Call back when tube is paused.

        A CommandFailed exception is raised if tube does not exists.
        """
        self._interact_success('pause-tube %s %d\r\n' % (name, delay),
                callback, ['PAUSED'], ['NOT_FOUND'])
