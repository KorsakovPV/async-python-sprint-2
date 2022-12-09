import uuid
import datetime


class Job:
    def __init__(self, fn, args=None, kwargs=None, start_at=datetime.datetime.now().time(), max_working_time=-1, tries=0, dependencies=[]):
        self._fn = fn  # вызываемый объект
        self._args = args
        self._kwargs = kwargs
        self.start_at = start_at  # Время запуска
        self.max_working_time = max_working_time  # Длительность выполнения
        self.tries = tries  # количество рестартов
        self.dependencies = dependencies  # Задачи зависимости
        self.job_id = uuid.uuid4()

    def run(self):
        return self._fn(*self._args, **self._kwargs)

    def pause(self):
        pass

    def stop(self):
        pass

    def __lt__(self, other) -> bool:
        return not other.start_at < self.start_at

    def __str__(self):
        return self.start_at

    def __repr__(self):
        return f'{self.start_at}'