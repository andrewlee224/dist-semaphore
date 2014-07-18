"""
Distributed Semaphore based on atomic operations performed
using memcached or memcachedb. Use memcachedb if you want guarantee
of persistence.
"""

import pylibmc as plm
import time

class DistributedSemaphore(object):
    """
    Create an object for accessing or creating and accessing
    the semaphore. If 'total' is supplied in the constructor,
    the semaphore which can be accessed by maximum 'total'
    number of clients is created.
    """

    def __init__(self, c, name, retry=10, total=None):
        """
        name - name of the semaphore, used as a key in memcache
        c - instance of pylibmc.Client
        retry - number of retries if acquiring or releasing
        fails
        total - limit of concurrent clients acquiring the
        semaphore
        """

        self.c = c
        self.name = name
        self.retry = retry
        self.total = total
        self.acquired = False

        if total:
            # initialize counter
            self.c.set(name, total)

    def acquire(self):
        c = self.c
        if self.acquired: return False

        retry = self.retry

        while(retry >= 0):
            retry -= 1
            val, stamp = c.gets(self.name)
            if val is None:
                return False
            if val <= 0: 
                continue

            success = c.cas(self.name, val-1, stamp)
            if success:
                self.acquired = True
                return True
            time.sleep(1)

        return False


    def release(self):
        c = self.c
        if not self.acquired: return False

        retry = self.retry
        
        while(retry >= 0):
            retry -= 1
            val, stamp = c.gets(self.name)
            if val is None:
                return False

            success = c.cas(self.name, val+1, stamp)
            if success:
                self.acquired = False
                return True
            time.sleep(1)

        return False


    def __del__(self):
        self.release()
