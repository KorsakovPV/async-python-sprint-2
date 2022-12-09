import datetime
import random
import time

from job import Job
from setting_log import logger

pause = 0

def task_for_test_0():
    # logger.info('Start task_for_test_0 +0:00')
    logger.info(f'Started task_for_test_0')
    time.sleep(pause*1)
    # print('task_for_test_0')
    # print(f'В очереди осталось {len(string._tasks)} тасок')
    return 'task_for_test_0'


def task_for_test_1():
    # logger.info('Start task_for_test_0 +0:00')
    logger.info(f'Started task_for_test_1')
    time.sleep(pause*2)
    # print(f'В очереди осталось {len(string._tasks)} тасок')
    return 'task_for_test_1'


def task_for_test_2():
    # logger.info('Start task_for_test_0 +0:00')
    logger.info(f'Started task_for_test_2')
    time.sleep(pause*3)
    # time.sleep(12000)
    # print(f'В очереди осталось {len(string._tasks)} тасок')
    return 'task_for_test_2'


def task_for_test_3():
    # logger.info('Start task_for_test_0 +0:00')
    logger.info(f'Started task_for_test_3')
    time.sleep(pause*4)
    # time.sleep()
    # print(f'В очереди осталось {len(string._tasks)} тасок')
    return 'task_for_test_3'
def task_for_test_inner():
    # logger.info('Start task_for_test_0 +0:00')
    logger.info(f'Started task_for_test_3')
    time.sleep(pause*4)
    # time.sleep()
    # print(f'В очереди осталось {len(string._tasks)} тасок')
    return 'task_for_test_3'


worker_tasks = {
    'task_for_test_0': task_for_test_0,
    'task_for_test_1': task_for_test_1,
    'task_for_test_2': task_for_test_2,
    'task_for_test_3': task_for_test_3,
    'task_for_test_inner': task_for_test_inner,
}

def add_task(scheduler):
    job_inner = Job(
        fn=worker_tasks[f'task_for_test_inner'], kwargs={},
    )
    scheduler.schedule(task=job_inner)

    for i in range(1):
        start_at = datetime.datetime.now() + datetime.timedelta(seconds=(random.randrange(10)+10))
        job0 = Job(
            fn=worker_tasks[f'task_for_test_{i%4}'],
            args=[],
            kwargs={},
            start_at=start_at,
            max_working_time=0,
            tries=5,#(random.randrange(10)+1),
            dependencies=[job_inner]
        )
        scheduler.schedule(task=job0)

    # time.sleep(1)

    # for i in range(20):
    #     job0 = Job(
    #         fn=worker_tasks[f'task_for_test_{i%4}'], kwargs={},
    #     )
    #     scheduler.schedule(task=job0)


