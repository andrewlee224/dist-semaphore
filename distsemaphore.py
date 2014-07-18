"""
Distributed Semaphore based on atomic operations performed
using memcached or memcachedb. Use memcachedb if you want guarantee
of persistence.
"""

import pylibmc as plm
import time

class DistributedSemaphore(object):
    
    def __init__(self, c, name, retry=10, total=None):
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
            success = c.cas(self.name, val+1, stamp)
            if success:
                self.acquired = False
                return True
            time.sleep(1)

        return False
