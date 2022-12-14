import datetime
import random
import uuid

import pytest as pytest

from job import Job
from scheduler import Scheduler
from tasks import worker_tasks


def fn():
    pass
@pytest.fixture()
def job_inner():
    return Job(
        fn=fn,
        args=[],
        kwargs={},
        start_datetime=datetime.datetime(year=2022, month=1, day=1, hour=0, minute=20, second=0),
        max_working_time=20,
        tries=0,
        dependencies=[],
        id='123e4567-e89b-12d3-a456-426655440001',
    )

@pytest.fixture()
def job_with_inner(job_inner):
    return Job(
        fn=fn,
        args=[],
        kwargs={},
        start_datetime=datetime.datetime(year=2022, month=1, day=1, hour=0, minute=0, second=0),
        max_working_time=20,
        tries=0,
        dependencies=[job_inner],
        id='123e4567-e89b-12d3-a456-426655440002',
    )

@pytest.fixture()
def job(job_inner):
    return Job(
        fn=fn,
        args=[],
        kwargs={},
        start_datetime=datetime.datetime(year=2022, month=1, day=1, hour=0, minute=0, second=0),
        max_working_time=20,
        tries=0,
        dependencies=[],
        id='123e4567-e89b-12d3-a456-426655440003',
    )

@pytest.fixture()
def scheduler(job_inner):
    return Scheduler()

def get_start_time():
    """
    Метод генерирует время, которое используется как время запуска функции.
    Используется для интеграционных тестов

    :return:
    """
    return datetime.datetime.now() + datetime.timedelta(
        seconds=(random.randrange(30) + 2)
    )

def create_tasks(scheduler):
    """
    Метод генерирует задачи для интеграционных тестов

    :param scheduler:
    :return:
    """
    for i in range(40):
        job0 = Job(
            fn=worker_tasks[f'task_for_test_{i % 10}'],
            args=[],
            kwargs={},
            start_datetime=get_start_time(),
            max_working_time=20,
            tries=0,
            dependencies=[],
            id=uuid.uuid4(),
        )
        scheduler.schedule(task=job0)

    for i in range(5):
        job_inner0 = Job(
            fn=worker_tasks['task_for_test_inner_0'], kwargs={},
            id=uuid.uuid4(),
        )
        job_inner1 = Job(
            fn=worker_tasks['task_for_test_inner_1'], kwargs={},
            id=uuid.uuid4(),
        )
        job_inner2 = Job(
            fn=worker_tasks['task_for_test_inner_2'], kwargs={},
            id=uuid.uuid4(),
        )
        job_inner3 = Job(
            fn=worker_tasks['task_for_test_inner_3'], kwargs={},
            id=uuid.uuid4(),
        )
        job = Job(
            fn=worker_tasks[f'task_for_test_{i % 4}'],
            args=[],
            kwargs={},
            start_datetime=get_start_time(),
            max_working_time=20,
            tries=0,
            dependencies=[job_inner0, job_inner1, job_inner2, job_inner3],
            id=uuid.uuid4(),
        )
        scheduler.schedule(task=job)

