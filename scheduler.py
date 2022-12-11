import datetime
import heapq
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Generator, Optional

from job import Job, JobStatus
from schemas import TaskSchema
from setting_log import logger
from tasks import worker_tasks


class Scheduler:
    def __init__(self, pool_size: int = 10, stop_when_queue_is_empty=False) -> None:
        """
        Класс планировщик. Принимает Job и запускает их в соответствии с расписанием.

        :param pool_size: размер пула для ThreadPoolExecutor
        :param stop_when_queue_is_empty: поведение планировщика при пустой очереди
        """
        self._pool_size: int = pool_size
        self.tasks: list[Job] = []
        self.stop_when_queue_is_empty = stop_when_queue_is_empty

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

        heapq.heappush(self.tasks, task)
        logger.info('Task is added in Scheduler.')

    def get_task(self) -> Generator[Job, None, None]:
        """
        Метод генератор смотрит когда, стартует следующий Job. Ждет этого момента.
        Если у Job есть не выполненные зависимости откладывает ее. Если зависимостей нет или они
        завершены возвращает Job.

        :return: Job
        """

        while self._stop_when_queue_is_empty:
            self.tasks.sort()
            if self.tasks[0].status == JobStatus(0):
                time_next_task = self.tasks[0].start_datetime
                now_datetime = datetime.datetime.now()

                logger.info(
                    f'Next task start at {time_next_task}'
                )
                time_to_next_task = time_next_task.timestamp() - now_datetime.timestamp()

                if time_to_next_task > 0:
                    time.sleep(time_to_next_task)

                if self.tasks[0].check_dependencies_task_is_complite() is False:
                    # task = heapq.heappop(self.tasks)
                    self.tasks[0].set_next_start_datetime()
                    # self.schedule(task)

                logger.info('Geting task')
                # yield heapq.heappop(self.tasks)
                yield self.tasks[0]

            else:
                logger.info(
                    'There are no scheduled tasks in the scheduler. Waiting for 5 minutes.'
                )
                time.sleep(5 * 60)

    def result_iterator(self) -> Generator[None, Job, None]:
        """
        Метод корутина. Получает Job с футурой и обрабатывает ее.

        :return:
        """
        while True:
            task = (yield)
            logger.info('Add result task in work_response.')
            task.rezult = task.response_future.result(timeout=task.max_working_time)

            if task.response_future.done():
                task.status = JobStatus(2)

            elif task.response_future.done() is False and task.tries:
                task.set_next_start_datetime()
                task.tries -= 1
                task.status = JobStatus(0)
                self.schedule(task)

            else:
                task.status = JobStatus(3)

    @property
    def _stop_when_queue_is_empty(self) -> bool:
        """
        Метод регулирует поведение планировщика при пустой очереди.

        :return:
        """
        if self.stop_when_queue_is_empty:
            return bool(self.tasks)
        return True

    def run(self) -> None:
        """
        Метод запускает выполнение добавленных Job

        :return:
        """
        logger.info('Scheduler run.')

        result_iterator = self.result_iterator()
        next(result_iterator)

        with ThreadPoolExecutor(max_workers=self._pool_size) as pool:
            while task := next(self.get_task()):
                task.status = JobStatus(1)
                task.response_future = pool.submit(task.run)
                logger.info(
                    f'Number of tasks {len(self.count_in_queue_task)} from {len(self.tasks)}, '
                    f'number of active_count {threading.active_count()}'
                )
                pool.submit(result_iterator.send, task)

    @property
    def count_in_queue_task(self) -> list[Job]:
        """
        Метод возвращает колличество Job в очереди.

        :return:
        """
        return [x for x in self.tasks if x.status == JobStatus(0)]

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
            status=JobStatus(0) if status == JobStatus(1) else status,
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

    def stop(self) -> None:
        """
        Метод останавливает работу планировщика.

        :return:
        """
        tasks_json = []
        for task in self.tasks:
            task_dict = task.__dict__
            task_dict['dependencies'] = [x.__dict__ for x in task_dict['dependencies']]
            tasks_json.append(TaskSchema.parse_obj(task.__dict__).json())
        with open('data.json', 'w') as f:
            json.dump(tasks_json, f)
        logger.info('Scheduler stop')
