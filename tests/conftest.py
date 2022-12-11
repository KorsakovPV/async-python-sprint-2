import uuid
import datetime

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

