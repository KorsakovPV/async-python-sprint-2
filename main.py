from job import Job
from scheduler import Scheduler
from setting_log import logger
import datetime


def now_hour(hour=0):
    return datetime.datetime.now().hour


def now_minute(minute=0):
    return (datetime.datetime.now().minute + minute) % 60


def now_second(second=0):
    return (datetime.datetime.now().second + second) % 60


def task_for_test_0(string):
    logger.info('Start task_for_test_0')
    print(string)


job0 = Job(
    fn=task_for_test_0, kwargs={'string': 'strung'},
    start_at=datetime.time(hour=now_hour(), minute=now_minute(), second=now_second())
)


def task_for_test_1(string):
    logger.info('Start task_for_test_1')
    print(string)


job1 = Job(
    fn=task_for_test_1, kwargs={'string': 'strung'},
    start_at=datetime.time(hour=now_hour(), minute=now_minute(3), second=now_second())
)


def task_for_test_2(string):
    logger.info('Start task_for_test_2')
    print(string)


job2 = Job(
    fn=task_for_test_1, kwargs={'string': 'strung'},
    start_at=datetime.time(hour=now_hour(), minute=now_minute(), second=now_second(3))
)


def task_for_test_3(string):
    logger.info('Start task_for_test_3')
    print(string)


job3 = Job(
    fn=task_for_test_1, kwargs={'string': 'strung'},
    start_at=datetime.time(hour=now_hour(), minute=now_minute(1), second=now_second(3))
)


def task_for_test_4(string):
    logger.info('Start task_for_test_4')
    print(string)


job4 = Job(
    fn=task_for_test_1, kwargs={'string': 'strung'},
    start_at=datetime.time(hour=now_hour(), minute=now_minute() + 2, second=now_second())
)


def task_for_test_5(string):
    logger.info('Start task_for_test_5')
    print(string)


job5 = Job(
    fn=task_for_test_1, kwargs={'string': 'strung'},
    start_at=datetime.time(hour=now_hour(), minute=now_minute() + 1, second=now_second())
)


def task_for_test_6(string):
    logger.info('Start task_for_test_6')
    print(string)


job6 = Job(
    fn=task_for_test_1, kwargs={'string': 'strung'},
    start_at=datetime.time(hour=now_hour(), minute=now_minute(), second=now_second())
)

scheduler = Scheduler()

scheduler.schedule(task=job0)
scheduler.schedule(task=job1)
scheduler.schedule(task=job2)
scheduler.schedule(task=job3)
scheduler.schedule(task=job4)
scheduler.schedule(task=job5)
scheduler.schedule(task=job6)
# scheduler.schedule(task=job)
# scheduler.schedule(task=job)
# scheduler.schedule(task=job)
# scheduler.schedule(task=job)
# scheduler.schedule(task=job)

scheduler.run()
