import time
import uuid
import datetime
import signal

from setting_log import logger


class Job:
    def __init__(self, fn, args=None, kwargs=None,
                 start_at=None,
                 start_datetime_stamp=None,
                 # start_at=datetime.datetime.now().time(),
                 max_working_time=None, tries=0, dependencies=[]):
        self._fn = fn  # вызываемый объект
        self.fn_name = fn.__name__
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.start_at = start_at  # Время запуска
        self.start_datetime_stamp = start_datetime_stamp or self.get_start_datetime_stamp(start_at)
        self.max_working_time = max_working_time  # Длительность выполнения
        self.tries = tries  # количество рестартов
        self.dependencies = dependencies  # Задачи зависимости
        self.job_id = uuid.uuid4()
        self.time_step = datetime.timedelta(minutes=10)
        self.response_future = None
        self.successfully_completed = False
        self.rezult = None

        self.check_dependencies_task_start_datetime()

    def set_next_start_datetime_stamp(self):
        self.start_datetime_stamp += self.time_step

    def run(self):
        try:
            # self.tries -= 1
            # self.set_next_start_datetime_stamp()
            return self._fn(*self.args, **self.kwargs)
        except Exception as err:
            logger.error(f'Error. Job fail with error. {err}')
            return None

        # return res

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
            if dependencies_task.start_datetime_stamp>self.start_datetime_stamp:
                self.start_datetime_stamp = dependencies_task.start_datetime_stamp+datetime.timedelta(minutes=1)

    def __lt__(self, other) -> bool:
        return other.start_datetime_stamp > self.start_datetime_stamp

    def __str__(self):
        return self.start_at

    def __repr__(self):
        return f'{self.start_at}'

    def get_start_datetime_stamp(self, start_at: datetime.datetime):
        now = datetime.datetime.now()
        if start_at:
            if now > start_at:
                return start_at + datetime.timedelta(days=1)
            return start_at
        return now
