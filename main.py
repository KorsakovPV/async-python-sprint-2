import concurrent
import json
import random
from concurrent.futures import ThreadPoolExecutor

from job import Job
from scheduler import Scheduler
from setting_log import logger
import datetime
import time

from tasks import task_for_test_0, worker_tasks, add_task


def main():
    scheduler = Scheduler()

    scheduler_stop(scheduler)

    scheduler = Scheduler()
    scheduler_restart(scheduler)

    # scheduler.run()
    with ThreadPoolExecutor(max_workers=2) as pool:
        pool.submit(scheduler.run)
        pool.submit(add_task(scheduler))


def scheduler_restart(scheduler):
    scheduler.restart()


def scheduler_stop(scheduler):
    add_task(scheduler)
    scheduler.stop()

concurrent.futures.Future

if __name__ == "__main__":
    main()
