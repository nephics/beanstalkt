import tornado.testing

import beanstalkt


class BeanstalkTest(tornado.testing.AsyncTestCase):

    def setUp(self):
        tornado.testing.AsyncTestCase.setUp(self)
        self.btc = beanstalkt.Client(io_loop=self.io_loop)
        self.btc.connect(self.stop)
        self.wait(timeout=0.1)

    def tearDown(self):
        self.btc.close(self.stop)
        self.wait(timeout=0.1)
        tornado.testing.AsyncTestCase.tearDown(self)

    def test_basics(self):
        '''Test put-reserve-delete cycle'''
        # put the job on the queue
        body = b'test job'
        self.btc.put(body, callback=self.stop)
        job_id = self.wait()
        self.assertIsInstance(job_id, int)

        # reserve the job
        self.btc.reserve(callback=self.stop)
        job = self.wait()
        self.assertIsNotNone(job)
        self.assertEqual(job['id'], job_id)
        self.assertEqual(job['body'], body)

        # delete the job
        self.btc.delete(job_id, callback=self.stop)
        self.wait()

    def test_peek_bury_kick(self):
        '''Test peeking, burying and kicking'''
        # put the job on the queue with 1 sec delay
        body = b'test job'
        self.btc.put(body, delay=1, callback=self.stop)
        job_id = self.wait()

        def check(job):
            self.assertNotIsInstance(job, Exception)
            self.assertEqual(job['id'], job_id)
            self.assertEqual(job['body'], body)

        # peak the next delayed job
        self.btc.peek_delayed(callback=self.stop)
        check(self.wait())

        # peak the job
        self.btc.peek(job_id, callback=self.stop)
        check(self.wait())

        # kick the job to ready
        self.btc.kick_job(job_id, callback=self.stop)
        try:
            self.wait()
        except beanstalkt.UnexpectedResponse as e:
            status = e[1]
            if status != 'UNKNOWN_COMMAND':
                raise
            # kick-job command is not available in Beanstalkd version <= 1.7
            self.btc.kick(callback=self.stop)
            self.wait()

        # peak next ready
        self.btc.peek_ready(callback=self.stop)
        check(self.wait())

        # reserve and bury the job
        self.btc.reserve(callback=self.stop)
        job = self.wait()
        check(job)
        self.btc.bury(job_id, callback=self.stop)
        self.wait()

        # peak the next buried job
        self.btc.peek_buried(callback=self.stop)
        check(self.wait())

        # kick the job to ready
        self.btc.kick(callback=self.stop)
        self.assertEqual(self.wait(), 1)

        # delete the job
        self.btc.delete(job_id, callback=self.stop)
        self.wait()


if __name__ == '__main__':
    import sys
    if len(sys.argv) == 1:
        sys.argv.append('bt_test')
    tornado.testing.main()