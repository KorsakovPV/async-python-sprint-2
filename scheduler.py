import heapq
import threading
import time
import datetime

from job import Job
from setting_log import logger


class Scheduler:
    def __init__(self, pool_size: int = 10):
        self._pool_size: int = pool_size
        self._tasks = []

    def schedule(self, task: Job):
        logger.info('Try add task')
        # if len(self._tasks) < self._pool_size:
        heapq.heappush(self._tasks, task)
        # self._tasks.append(task)
        # logger.info(f'Task is added. Total count task is {len(self._tasks)}')
        # else:
        #     logger.warning(f'Task is not added. Scheduler pool is full. pool_size = {self._pool_size}')

    def run(self):
        logger.info('Scheduler run')

        while True:
            print(100)
            task = heapq.heappop(self._tasks)

            # def __init__(self, interval, function, args=None, kwargs=None):
            # interval =
            thread = threading.Timer(0.3, task)

    def restart(self):
        logger.info('Scheduler restart')

    def stop(self):
        logger.info('Scheduler stop')
