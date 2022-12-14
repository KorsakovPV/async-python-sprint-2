import json
import os

from urllib.request import urlopen

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

    return get_weather(CITY_NAME_FOR_TEST)


def task_for_test_1():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """

    CITY_NAME_FOR_TEST = "PARIS"

    return get_weather(CITY_NAME_FOR_TEST)


def task_for_test_2():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{2}')

    CITY_NAME_FOR_TEST = "LONDON"

    return get_weather(CITY_NAME_FOR_TEST)


def task_for_test_3():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{3}')

    CITY_NAME_FOR_TEST = "BERLIN"

    return get_weather(CITY_NAME_FOR_TEST)


def task_for_test_4():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{4}')

    CITY_NAME_FOR_TEST = "BEIJING"

    yield from read_and_del_file(CITY_NAME_FOR_TEST)


def task_for_test_5():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{5}')

    CITY_NAME_FOR_TEST = "BERLIN"

    yield from read_and_del_file(CITY_NAME_FOR_TEST)


def task_for_test_6():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{6}')

    CITY_NAME_FOR_TEST = "KAZAN"

    yield from read_and_del_file(CITY_NAME_FOR_TEST)


def task_for_test_7():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{7}')

    CITY_NAME_FOR_TEST = "LONDON"

    yield from read_and_del_file(CITY_NAME_FOR_TEST)


def task_for_test_8():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{8}')

    CITY_NAME_FOR_TEST = "MOSCOW"

    yield from read_and_del_file(CITY_NAME_FOR_TEST)


def task_for_test_9():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info(f'Started task_for_test_{9}')

    CITY_NAME_FOR_TEST = "PARIS"

    yield from read_and_del_file(CITY_NAME_FOR_TEST)


def task_for_test_inner_0():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info('Started task_for_test_inner_0')

    CITY_NAME_FOR_TEST = "BEIJING"

    return get_weather(CITY_NAME_FOR_TEST)


def task_for_test_inner_1():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info('Started task_for_test_inner_1')

    CITY_NAME_FOR_TEST = "KAZAN"

    return get_weather(CITY_NAME_FOR_TEST)


def task_for_test_inner_2():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info('Started task_for_test_inner_2')

    CITY_NAME_FOR_TEST = "SPETERSBURG"

    return get_weather(CITY_NAME_FOR_TEST)


def task_for_test_inner_3():
    """
    Тестовый метод. Используется для интеграционных тестов

    :return:
    """
    logger.info('Started task_for_test_inner_3')

    CITY_NAME_FOR_TEST = "VOLGOGRAD"

    return get_weather(CITY_NAME_FOR_TEST)


def get_weather(CITY_NAME_FOR_TEST):
    """
    Метод создает или удаляет папку если она создана, читает данные из сети и сохраняет в файл.

    :param CITY_NAME_FOR_TEST:
    :return:
    """
    if not os.path.exists(CITY_NAME_FOR_TEST):
        os.makedirs(CITY_NAME_FOR_TEST)
    else:
        os.rmdir(CITY_NAME_FOR_TEST)

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)
    with open(f"{CITY_NAME_FOR_TEST}.txt", "w") as file:
        file.write(str(resp.get("info")))
    return resp.get("info")


def read_and_del_file(CITY_NAME_FOR_TEST):
    """
    Читает данные из файла и возвращает их как результат работы метода.
    после чего файл удаляется.

    :param CITY_NAME_FOR_TEST:
    :return:
    """
    with open(f"{CITY_NAME_FOR_TEST}.txt", "r") as file:
        yield file.readlines()
    os.remove(file.name)


worker_tasks = {
    'task_for_test_0': task_for_test_0,
    'task_for_test_1': task_for_test_1,
    'task_for_test_2': task_for_test_2,
    'task_for_test_3': task_for_test_3,
    'task_for_test_4': task_for_test_4,
    'task_for_test_5': task_for_test_5,
    'task_for_test_6': task_for_test_6,
    'task_for_test_7': task_for_test_7,
    'task_for_test_8': task_for_test_8,
    'task_for_test_9': task_for_test_9,
    'task_for_test_inner_0': task_for_test_inner_0,
    'task_for_test_inner_1': task_for_test_inner_1,
    'task_for_test_inner_2': task_for_test_inner_2,
    'task_for_test_inner_3': task_for_test_inner_3,
}
