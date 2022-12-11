import datetime
from enum import Enum

from setting_log import logger


class JobStatus(Enum):
    in_queue = 0
    in_progress = 1
    completed = 2
    error = 3


class Job:

    def __init__(
            self,
            id,
            fn,
            args=None,
            kwargs=None,
            start_at=None,
            start_datetime=None,
            max_working_time=None,
            tries=0,
            dependencies=[],
            status=JobStatus(0)
    ):
        self._fn = fn  # вызываемый объект
        self.fn_name = fn.__name__
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.start_at = start_at  # Время запуска
        self.start_datetime = start_datetime or self.get_start_datetime(start_at)
        self.max_working_time = max_working_time  # Длительность выполнения
        self.tries = tries  # количество рестартов
        self.dependencies = dependencies  # Задачи зависимости
        self.time_step = datetime.timedelta(minutes=10)
        self.response_future = None
        self.id = id
        self.rezult = None
        self.status = status
        self.check_dependencies_task_start_datetime()

    def set_next_start_datetime(self):
        self.start_datetime += self.time_step

    def run(self):
        try:
            return self._fn(*self.args, **self.kwargs)
        except Exception as err:
            logger.error(f'Error. Job fail with error. {err}')
            return None

    def pause(self):
        pass

    def stop(self):
        pass

    def check_dependencies_task_is_complite(self):
        for dependencies_task in self.dependencies:
            if dependencies_task.response_future and dependencies_task.response_future.done():
                continue
            else:
                return False
        return True

    def check_dependencies_task_start_datetime(self):
        for dependencies_task in self.dependencies:
            if dependencies_task.start_datetime > self.start_datetime:
                self.start_datetime = dependencies_task.start_datetime + datetime.timedelta(
                    minutes=1)  # noqa

    def __lt__(self, other) -> bool:
        if other.status.value != self.status.value:
            return other.status.value > self.status.value
        return other.start_datetime > self.start_datetime

    def get_start_datetime(self, start_at: datetime.datetime):
        now = datetime.datetime.now()
        if start_at:
            if now > start_at:
                return start_at + datetime.timedelta(days=1)
            return start_at
        return now
