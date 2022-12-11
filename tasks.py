import datetime
import json
import random
import uuid
from urllib.request import urlopen

from job import Job
from setting_log import logger
from utils import CITIES, ERR_MESSAGE_TEMPLATE


class YandexWeatherAPI:
    """
    Base class for requests
    """

    @staticmethod
    def _do_req(url):
        """Base request method"""
        try:
            with urlopen(url) as req:
                resp = req.read().decode("utf-8")
                resp = json.loads(resp)
            if req.status != 200:
                raise Exception(
                    "Error during execute request. {}: {}".format(
                        resp.status, resp.reason
                    )
                )
            return resp
        except Exception as ex:
            logger.error(ex)
            raise Exception(ERR_MESSAGE_TEMPLATE)

    @staticmethod
    def _get_url_by_city_name(city_name: str) -> str:
        try:
            return CITIES[city_name]
        except KeyError:
            raise Exception("Please check that city {} exists".format(city_name))

    def get_forecasting(self, city_name: str):
        """
        :param city_name: key as str
        :return: response data as json
        """
        city_url = self._get_url_by_city_name(city_name)
        return self._do_req(city_url)


def task_for_test_0():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{0}')

    CITY_NAME_FOR_TEST = "MOSCOW"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)

    return resp.get("info")


def task_for_test_1():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """

    CITY_NAME_FOR_TEST = "PARIS"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)

    return resp.get("info")


def task_for_test_2():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{2}')

    CITY_NAME_FOR_TEST = "LONDON"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)

    return resp.get("info")


def task_for_test_3():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{3}')

    CITY_NAME_FOR_TEST = "BERLIN"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)

    return resp.get("info")


def task_for_test_inner_0():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info('Started task_for_test_inner_0')

    CITY_NAME_FOR_TEST = "BEIJING"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)

    return resp.get("info")


def task_for_test_inner_1():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info('Started task_for_test_inner_1')

    CITY_NAME_FOR_TEST = "KAZAN"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)

    return resp.get("info")


def task_for_test_inner_2():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info('Started task_for_test_inner_2')

    CITY_NAME_FOR_TEST = "SPETERSBURG"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)

    return resp.get("info")


def task_for_test_inner_3():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info('Started task_for_test_inner_3')

    CITY_NAME_FOR_TEST = "VOLGOGRAD"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)

    return resp.get("info")


worker_tasks = {
    'task_for_test_0': task_for_test_0,
    'task_for_test_1': task_for_test_1,
    'task_for_test_2': task_for_test_2,
    'task_for_test_3': task_for_test_3,
    'task_for_test_inner_0': task_for_test_inner_0,
    'task_for_test_inner_1': task_for_test_inner_1,
    'task_for_test_inner_2': task_for_test_inner_2,
    'task_for_test_inner_3': task_for_test_inner_3,
}


def get_start_time():
    """
    Метод генерирует время, которое используется как время запуска функции.
    Используется для интеграционных тестов

    :return:
    """
    return datetime.datetime.now() + datetime.timedelta(
        seconds=(random.randrange(200) + 2)
    )


def add_task(scheduler):
    """
    Метод генерирует задачи для интеграционных тестов

    :param scheduler:
    :return:
    """
    for i in range(20):
        job0 = Job(
            fn=worker_tasks[f'task_for_test_{i % 4}'],
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
