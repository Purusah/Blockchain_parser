import multiprocessing as mp
import queue

class ContainerQueue(object):
    def __init__(self, lock):
        self.container = mp.Queue()
        self.lock = lock

    def get(self):
        try:
            self.lock.acquire()
            tx = self.container.get_nowait()
        except queue.Empty:
            tx = None
        self.lock.release()
        return tx

    def put(self, item):
        self.lock.acquire()
        self.container.put_nowait(item)
        self.lock.release()

    def __len__(self):
        return self.container.qsize()