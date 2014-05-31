#!/usr/bin/env python
#! -*- coding: utf-8 -*-


import threading
import time

class WorkerThread(threading.Thread):
    """
    """
    def __init__(self, elem, lock):
        """
        """
        self.elem = elem
        self.lock = lock
        self.ready = False
        threading.Thread.__init__(self)

    def run(self):
        """
        """
        self.ready = True
        self.lock.acquire()
        print self.getName() + ": " + str(self.elem.pop())
        self.lock.release()

class ThreadPool(object):
    """
    """
    def __init__(self, elem=[]):
        """
        """
        self.elem = elem
        self._threads = []
        self._lock = threading.Lock()

    def start(self):
        """
        """
        for i in xrange(len(self.elem)):
            self._threads.append(WorkerThread(self.elem, self._lock))

        for worker in self._threads:
            worker.start()

        for worker in self._threads:
            while not worker.ready:
                time.sleep(0.1)

    def stop(self):
        """
        """
        current = threading.currentThread()
        for worker in self._threads:
            worker = self._threads.pop()
            if current is not worker and worker.isAlive():
                worker.join()

def main():
    """
    """
    elem = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tp = ThreadPool(elem)
    tp.start()
    tp.stop()


if __name__ == '__main__':
    """
    """
    main()
