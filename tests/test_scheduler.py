import datetime

from job import JobStatus


class TestScheduler:

    def test_schedule(self, scheduler, job):
        scheduler.schedule(job)
        assert job in scheduler.tasks

    def test_schedule_inner(self, scheduler, job_inner, job_with_inner):
        scheduler.schedule(job_with_inner)
        assert job_with_inner in scheduler.tasks
        assert job_inner in scheduler.tasks

    def test_get_or_create_job(self, scheduler, job_inner, job_with_inner):
        scheduler.schedule(job_with_inner)
        new_job = scheduler.get_or_create_job(
            fn_name='task_for_test_0',
            args=[],
            kwargs={},
            start_datetime=datetime.datetime(year=2022, month=1, day=1, hour=0, minute=0,
                                             second=0),
            max_working_time=20,
            tries=0,
            dependencies=[],
            id_job='123e4567-e89b-12d3-a456-426655440000',
            status=JobStatus.IN_PROGRESS
        )
        assert new_job.start_datetime == datetime.datetime(
            year=2022, month=1, day=1, hour=0, minute=0, second=0
        )
        assert new_job.max_working_time == 20
        assert new_job.id == '123e4567-e89b-12d3-a456-426655440000'
        assert new_job.status == JobStatus.IN_QUEUE

    def test_get_or_create_job2(self, scheduler, job_inner, job_with_inner):
        scheduler.schedule(job_with_inner)
        new_job = scheduler.get_or_create_job(
            fn_name='task_for_test_0',
            args=[],
            kwargs={},
            start_datetime=datetime.datetime(year=2022, month=1, day=1, hour=0, minute=0,
                                             second=0),
            max_working_time=20,
            tries=0,
            dependencies=[],
            id_job='123e4567-e89b-12d3-a456-426655440001',
            status=JobStatus.IN_PROGRESS
        )
        assert new_job.start_datetime == datetime.datetime(
            year=2022, month=1, day=1, hour=0, minute=20, second=0
        )
        assert new_job.max_working_time == 20
        assert new_job.id == '123e4567-e89b-12d3-a456-426655440001'
        assert new_job.status == JobStatus.IN_QUEUE

    def test_get_task_in_scheduler_tasks(self, scheduler, job_inner, job_with_inner):
        scheduler.schedule(job_with_inner)
        new_job = scheduler.get_task_in_scheduler_tasks(
            id_job='123e4567-e89b-12d3-a456-426655440000',
        )
        assert new_job is None

    def test_get_task_in_scheduler_tasks2(self, scheduler, job_inner, job_with_inner):
        scheduler.schedule(job_with_inner)
        new_job = scheduler.get_task_in_scheduler_tasks(
            id_job='123e4567-e89b-12d3-a456-426655440001',
        )
        assert new_job.start_datetime == datetime.datetime(
            year=2022, month=1, day=1, hour=0, minute=20, second=0
        )
        assert new_job.max_working_time == 20
        assert new_job.id == '123e4567-e89b-12d3-a456-426655440001'
        assert new_job.status == JobStatus.IN_QUEUE
