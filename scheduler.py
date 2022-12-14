import datetime
import json
import time
from typing import Optional

from job import Job, JobStatus
from schemas import TaskSchema
from setting_log import logger
from tasks import worker_tasks
from functools import wraps


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        fn.send(None)
        return fn

    return inner


class Scheduler:
    def __init__(self, pool_size: int = 10) -> None:
        """
        Класс планировщик. Принимает Job и запускает их в соответствии с расписанием.

        :param pool_size: размер пула
        :param stop_when_queue_is_empty: поведение планировщика при пустой очереди
        """
        self._pool_size: int = pool_size
        self.tasks: list[Job] = []
        self.scheduler_run = False

    def schedule(self, task: Job) -> None:
        """
        Метод добавляет Job в список.

        :param task:
        :return:
        """
        logger.info('Try add task')

        if self.get_task_in_scheduler_tasks(task.id):
            logger.warning('The task has already been added.')

        if task.dependencies:
            logger.info('Check dependencies tasks.')
            for dependencies_task in task.dependencies:
                if dependencies_task not in self.tasks:
                    self.schedule(dependencies_task)

        self.tasks.append(task)
        logger.info('Task is added in Scheduler.')

    # @property
    # def _stop_when_queue_is_empty(self) -> bool:
    #     """
    #     Метод регулирует поведение планировщика при пустой очереди.
    #
    #     :return:
    #     """
    #     if self.stop_when_queue_is_empty:
    #         return bool(self.tasks)
    #     return True

    def get_task(self, target) -> None:
        """
        Метод генератор смотрит когда, стартует следующий Job. Ждет этого момента.
        Если у Job есть не выполненные зависимости откладывает ее. Если зависимостей нет или они
        завершены возвращает Job.

        :return: Job
        """

        while self.scheduler_run:
            self.tasks.sort()
            if self.tasks[0].status == JobStatus.IN_QUEUE:
                time_next_task = self.tasks[0].start_datetime
                now_datetime = datetime.datetime.now()

                logger.info(
                    f'Next task start at {time_next_task}'
                )
                time_to_next_task = time_next_task.timestamp() - now_datetime.timestamp()

                if time_to_next_task > 0:
                    time.sleep(time_to_next_task)

                if self.tasks[0].check_dependencies_task_is_complite() is False:
                    self.tasks[0].set_next_start_datetime()

                if len(self.count_in_queue_task(JobStatus.IN_PROGRESS)) < self._pool_size:
                    logger.info('Geting task')
                    self.tasks[0].status = JobStatus.IN_PROGRESS
                    target.send(self.tasks[0])
                else:
                    logger.info('Pool size more than 10')

    @coroutine
    def execute_job(self):

        while task := (yield):
            try:
                logger.info(
                    f'Number of tasks {len(self.count_in_queue_task(JobStatus.IN_QUEUE))} '
                    f'from {len(self.tasks)}, '
                    f'Active tasks {len(self.count_in_queue_task(JobStatus.IN_PROGRESS))}.'
                )
                task.rezult = task.run()
                task.status = JobStatus.COMPLETED
            except Exception as err:
                logger.error(f'Job id={task.id} is fail with {err}')
                if task.tries > 0:
                    task.set_next_start_datetime()
                    task.tries -= 1
                    task.status = JobStatus.IN_QUEUE
                else:
                    task.status = JobStatus.ERROR

    def run(self) -> None:
        """
        Метод генератор смотрит когда, стартует следующий Job. Ждет этого момента.
        Если у Job есть не выполненные зависимости откладывает ее. Если зависимостей нет или они
        завершены передает Job на выполнение.

        :return: Job
        """
        logger.info('Scheduler run.')

        self.scheduler_run = True

        execute = self.execute_job()

        self.get_task(execute)

    def count_in_queue_task(self, status) -> list[Job]:
        """
        Метод возвращает колличество Job в очереди.

        :return:
        """
        return [x for x in self.tasks if x.status == status]

    def get_or_create_job(
            self,
            id_job,
            fn_name,
            args,
            kwargs,
            start_datetime,
            max_working_time,
            tries,
            status,
            dependencies
    ) -> Job:
        """
        Метод получает все необходимые для создания Job параметры. Пытается найти в добавленных
        с таким же id. Если находит то, возвращает найденный, если не находит то, создает новый и
        возвращает его. Если при завершении статус Job был in_progress и он не был завершен
        то он переводится в in_queue чтоб стартавать Job заново.

        :param id_job:
        :param fn_name:
        :param args:
        :param kwargs:
        :param start_datetime:
        :param max_working_time:
        :param tries:
        :param status:
        :param dependencies:
        :return:
        """
        job = self.get_task_in_scheduler_tasks(id_job)

        if job:
            return job
        return Job(
            id=id_job,
            fn=worker_tasks.get(fn_name),
            args=args,
            kwargs=kwargs,
            start_datetime=start_datetime,
            max_working_time=max_working_time,
            tries=tries,
            status=JobStatus.IN_QUEUE if status == JobStatus.IN_PROGRESS else status,
            dependencies=dependencies
        )

    def get_task_in_scheduler_tasks(self, id_job) -> Optional[Job]:
        """
        Пытается найти в добавленных Job с таким же id. Если находит то, возвращает найденный,
        если не находит то, создает новый и
        возвращает его.

        :param id_job:
        :return:
        """
        try:
            job = next(x for x in self.tasks if x.id == id_job)
        except StopIteration:
            job = None
        return job

    def restart(self) -> None:
        """
        Метод восстанавливает работу планировщика после штатного завершения

        :return:
        """
        try:
            with open('data.json') as f:
                tasks_json = json.load(f)
        except Exception as err:
            logger.exception(f"Error. Can't open file. {err}")

        for task in tasks_json:
            try:
                valid_task = TaskSchema.parse_raw(task)
            except TypeError as err:
                logger.error(f'Error. Row is invalid. Row skip. {err=}')
                continue

            dependencies = []

            for dependence in valid_task.dependencies:
                dependencies.append(
                    self.get_or_create_job(
                        id_job=dependence.id,
                        fn_name=dependence.fn_name,
                        args=dependence.args,
                        kwargs=dependence.kwargs,
                        start_datetime=dependence.start_datetime,
                        max_working_time=dependence.max_working_time,
                        tries=dependence.tries,
                        status=dependence.status,
                        dependencies=dependence.dependencies
                    )
                )
            task_job = self.get_or_create_job(
                id_job=valid_task.id,
                fn_name=valid_task.fn_name,
                args=valid_task.args,
                kwargs=valid_task.kwargs,
                start_datetime=valid_task.start_datetime,
                max_working_time=valid_task.max_working_time,
                tries=valid_task.tries,
                status=valid_task.status,
                dependencies=dependencies
            )
            self.schedule(task=task_job)
        logger.info('Scheduler restart')
        self.run()

    def stop(self) -> None:
        """
        Метод останавливает работу планировщика.

        :return:
        """
        self.scheduler_run = False
        tasks_json = []
        for task in self.tasks:
            task_dict = task.__dict__
            task_dict['dependencies'] = [x.__dict__ for x in task_dict['dependencies']]
            tasks_json.append(TaskSchema.parse_obj(task.__dict__).json())
        with open('data.json', 'w') as f:
            json.dump(tasks_json, f)
        logger.info('Scheduler stop')
