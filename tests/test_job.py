import datetime


class TestJob:

    def test_set_next_start_datetime(self, job):
        job.set_next_start_datetime()
        assert job.start_datetime==datetime.datetime(year=2022, month=1, day=1, hour=0, minute=10, second=0)

    def test_check_dependencies_task_is_complite_true(self, job):
        # job.set_next_start_datetime()
        assert job.check_dependencies_task_is_complite() is True

    def test_check_dependencies_task_is_complite_false(self, job_with_inner):
        # job.set_next_start_datetime()
        assert job_with_inner.check_dependencies_task_is_complite() is False

    def test_check_dependencies_task_start_datetime(self, job):
        job.check_dependencies_task_start_datetime()
        assert job.start_datetime==datetime.datetime(year=2022, month=1, day=1, hour=0, minute=0, second=0)
