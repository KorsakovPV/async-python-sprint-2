import datetime
from enum import Enum

from setting_log import logger


class JobStatus(Enum):
    in_queue = 0
    in_progress = 1
    completed = 2
    error = 3


class Job:
    """
    Класс задачи для планировщика.

    """

    def __init__(
            self,
            id,
            fn,
            args=None,
            kwargs=None,
            start_datetime=datetime.datetime.now(),
            max_working_time=None,
            tries=0,
            dependencies=[],
            status=JobStatus(0)
    ):
        self._fn = fn  # вызываемый объект
        self.fn_name = fn.__name__
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.start_datetime = start_datetime
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.time_step = datetime.timedelta(minutes=10)
        self.response_future = None
        self.id = id
        self.rezult = None
        self.status = status
        self.check_dependencies_task_start_datetime()

    def set_next_start_datetime(self) -> None:
        """
        Метод вычисляет новое время запуска Job если он не смог по какой-то причины быть
        выполнен в свое время.

        :return:
        """
        self.start_datetime += self.time_step

    def run(self):
        """
        Метод запускает Job

        :return:
        """
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
        """
        Метод проверяет что все зависимости выполнены.

        :return:
        """
        for dependencies_task in self.dependencies:
            if dependencies_task.response_future and dependencies_task.response_future.done():
                continue
            else:
                return False
        return True

    def check_dependencies_task_start_datetime(self):
        """
        Метод проверяет что время запуска зависимостей раньше чем у основного Job.

        :return:
        """
        for dependencies_task in self.dependencies:
            if dependencies_task.start_datetime > self.start_datetime:
                self.start_datetime = dependencies_task.start_datetime + datetime.timedelta(
                    minutes=1)  # noqa

    def __lt__(self, other) -> bool:
        """
        Метод переопределяет сортировку так чтоб в начале списка были Job с статусом in_queue и
        ближайшим временем.

        :param other:
        :return:
        """
        if other.status.value != self.status.value:
            return other.status.value > self.status.value
        return other.start_datetime > self.start_datetime

    # def get_start_datetime(self, start_at: datetime.datetime):
    #     """
    #     Если время  прошло то
    #
    #     :param start_at:
    #     :return:
    #     """
    #     now = datetime.datetime.now()
    #     if start_at:
    #         if now > start_at:
    #             return start_at + datetime.timedelta(days=1)
    #         return start_at
    #     return now
