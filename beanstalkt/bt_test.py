"""Tests for beanstalkt.

Requires a running instance of beanstalkd.
"""

import uuid

from tornado.testing import main, AsyncTestCase, gen_test

import beanstalkt


class BeanstalkTest(AsyncTestCase):

    def setUp(self):
        AsyncTestCase.setUp(self)
        self.btc = beanstalkt.Client(io_loop=self.io_loop)
        self.btc.connect(callback=self.stop)
        self.wait(timeout=0.1)

    def tearDown(self):
        self.btc.close(callback=self.stop)
        self.wait(timeout=0.1)
        AsyncTestCase.tearDown(self)

    @gen_test
    def test_basics(self):
        """Test put-reserve-delete cycle"""
        # put the job on the queue
        body = b'test job'
        job_id = yield self.btc.put(body)
        self.assertIsInstance(job_id, int)

        # reserve the job
        job = yield self.btc.reserve()
        self.assertIsNotNone(job)
        self.assertEqual(job['id'], job_id)
        self.assertEqual(job['body'], body)

        # delete the job
        yield self.btc.delete(job_id)

    @gen_test
    def test_peek_bury_kick(self):
        """Test peeking, burying and kicking"""
        # put the job on the queue with 1 sec delay
        body = b'test job'
        job_id = yield self.btc.put(body, delay=1)

        def check(job):
            self.assertIsNotNone(job)
            self.assertEqual(job['id'], job_id)
            self.assertEqual(job['body'], body)

        # peak the next delayed job
        resp = yield self.btc.peek_delayed()
        check(resp)

        # peak the job
        resp = yield self.btc.peek(job_id)
        check(resp)

        # kick the job to ready
        try:
            yield self.btc.kick_job(job_id)
        except beanstalkt.UnexpectedResponse as e:
            status = e[1]
            if status != 'UNKNOWN_COMMAND':
                raise
            # kick-job command is not available in Beanstalkd version <= 1.7
            yield self.btc.kick()

        # peak next ready
        resp = yield self.btc.peek_ready()
        check(resp)

        # reserve and bury the job
        job = yield self.btc.reserve()
        check(job)
        yield self.btc.bury(job_id)

        # peak the next buried job
        resp = yield self.btc.peek_buried()
        check(resp)

        # kick the job to ready
        resp = yield self.btc.kick()
        self.assertEqual(resp, 1)

        # delete the job
        yield self.btc.delete(job_id)

    @gen_test
    def test_use_watch_ignore(self):
        # a random channel name
        key = uuid.uuid4().hex

        # watch the channel by that random name, and ignore default channel
        yield self.btc.watch(key)
        yield self.btc.ignore('default')

        # put jobs on the default and random channels
        body = b'test job'
        job1_id = yield self.btc.put(body)
        yield self.btc.use(key)
        job2_id = yield self.btc.put(body)

        def check(job, job_id):
            self.assertIsNotNone(job)
            self.assertEqual(job['id'], job_id)
            self.assertEqual(job['body'], body)

        # reserve and delete job from the random channel
        job2 = yield self.btc.reserve()
        check(job2, job2_id)
        yield self.btc.delete(job2_id)

        # watch default channel, ignore random channel
        yield self.btc.watch('default')
        yield self.btc.ignore(key)

        # reserve and delete job from the default channel
        job1 = yield self.btc.reserve()
        check(job1, job1_id)
        yield self.btc.delete(job1_id)


if __name__ == '__main__':
    import sys
    if len(sys.argv) == 1:
        sys.argv.append('bt_test')
    main()
