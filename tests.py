import unittest
import pylibmc as plm

import distsemaphore as dsem

class DistributedSemaphoreTest(unittest.TestCase):
    
    def setUp(self):
        self.c1 = plm.Client(["127.0.0.1"],
            behaviors={"cas": True})
        self.c2 = plm.Client(["127.0.0.1"],
            behaviors={"cas": True})
        self.c3 = plm.Client(["127.0.0.1"],
            behaviors={"cas": True})

    def tearDown(self):
        del(self.c1)
        del(self.c2)

    def testLockedCounter(self):
        c1, c2 = self.c1, self.c2
        key = "counter"
        initVal = 5

        c1.set(key, initVal)
        
        counter1, stamp1 = c1.gets(key)
        counter2, stamp2 = c2.gets(key)
        status2 = c2.cas(key, counter2 - 1, stamp2)
        status1 = c1.cas(key, counter1 - 1, stamp1)

        self.assertFalse(status1)

    def testSemaphoreLock(self):
        s1 = dsem.DistributedSemaphore(self.c1, "counter", retry=5,
        total=1)
        s2 = dsem.DistributedSemaphore(self.c2, "counter", retry=5)

        s1.acquire()
        self.assertFalse(s2.acquire())

    def testSemaphoreRelease(self):
        s1 = dsem.DistributedSemaphore(self.c1, "counter", retry=5,
        total=2)
        s2 = dsem.DistributedSemaphore(self.c2, "counter", retry=5)
        s3 = dsem.DistributedSemaphore(self.c3, "counter", retry=5)

        status1 = s1.acquire()
        status2 = s2.acquire()
        status3 = s3.acquire()
        
        status4 = s2.release()
        status5 = s3.acquire()

        self.assertTrue(status1)
        self.assertTrue(status2)
        self.assertFalse(status3)
        self.assertTrue(status4)
        self.assertTrue(status5)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(DistributedSemaphoreTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
