# To run tests: python -m unittest beanstalk_test

import time
import unittest
import beanstalk

class BeanstalkClientTest(unittest.TestCase):

    def setUp(self):
        self.bc = beanstalk.Client('localhost', 11300)
        self.bc.use('test')
        self.bc.watch('test')
        self.clear_tube()

    def tearDown(self):
        self.bc.close()

    def test_use(self):
        tube = self.bc.use('test')
        self.assertTrue(tube, 'test')

    def test_put(self):
        data = 'test-data'
        job = self.bc.put(data)
        self.assertFalse(job is None)
        self.assertEqual(job.data, data)
        job.delete()

    def clear_tube(self):
        self.bc.kick(64)
        try:
            while True:
                job = self.bc.reserve(0)
                job.delete()
        except beanstalk.Error:
            pass

    def test_reserve(self):
        ts = '%s' % time.time()
        self.bc.put(ts)
        job = self.bc.reserve()
        self.assertEqual(ts, job.data)

    def test_reserve_timeout(self):
        self.assertRaises(beanstalk.CommandFailed, self.bc.reserve, 0)

    def test_delete(self):
        self.bc.put('data')
        job = self.bc.reserve()
        job.delete()
        self.assertRaises(beanstalk.CommandFailed, self.bc.reserve, 0)

    def test_release(self):
        self.bc.put('data')
        job = self.bc.reserve(0)
        self.bc.release(job.id)
        job2 = self.bc.reserve(0)
        self.assertEqual(job.id, job2.id)

    def test_bury_and_kick(self):
        self.bc.put('data')
        job = self.bc.reserve(0)
        self.bc.bury(job.id)
        self.assertRaises(beanstalk.CommandFailed, self.bc.reserve, 0)

        job_buried = self.bc.peek_buried()
        self.assertEqual(job.id, job_buried.id)

        self.bc.kick_job(job.id)
        job2 = self.bc.reserve(0)
        self.assertTrue(job.id, job2.id)
        job.delete()

    def test_touch(self):
        self.bc.put('data', ttr=1)
        job = self.bc.reserve(0)
        self.bc.touch(job.id)
        job.delete()

    def test_watch(self):
        self.assertEqual(self.bc.watch('default'), 2)

    def test_ignore(self):
        self.assertEqual(self.bc.ignore('default'), 1)
        self.assertRaises(beanstalk.CommandFailed, self.bc.ignore, 'test')

    def test_peek(self):
        job = self.bc.put('data')
        pjob = self.bc.peek(job.id)
        self.assertEqual(job.id, pjob.id)

        pjob2 = self.bc.peek_ready()
        self.assertEqual(job.id, pjob2.id)

        job = self.bc.reserve(0)
        self.bc.release(job.id, delay=10)
        pjob = self.bc.peek_delayed()
        self.assertEqual(job.id, pjob.id)

        self.bc.kick_job(job.id)
        job = self.bc.reserve(0)
        self.bc.bury(job.id)
        pjob = self.bc.peek_buried()
        self.assertEqual(job.id, pjob.id)

    def test_stats_job(self):
        job = self.bc.put('data')
        self.assertIsInstance(self.bc.stats_job(job.id), str)

    def test_stats_tube(self):
        self.assertIsInstance(self.bc.stats_tube('test'), str)

    def test_stats(self):
        self.assertIsInstance(self.bc.stats(), str)

    def test_list_tubes(self):
        ret = self.bc.list_tubes()
        self.assertIsInstance(ret, list)

        ret = self.bc.list_tubes_watched()
        self.assertIsInstance(ret, list)

        ret = self.bc.list_tube_used()
        self.assertEqual('test', ret)

    def test_quit(self):
        self.bc.quit()
        self.assertRaises(beanstalk.SocketError, self.bc.put, 'data')

    def test_pause_tube(self):
        self.bc.pause_tube('test', 1)
        self.bc.put('data')
        self.bc.reserve()
