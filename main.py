import threading
from concurrent.futures import ThreadPoolExecutor

from scheduler import Scheduler
from tasks import add_task


def main():
    """
    Метод для интеграционного тестирования. И демонстрации работы планировщика.

    Создает экземпляр класса Scheduler наполняет его задачами. Штатно останавливает планировщик.
    Создает новый экземпляр класса Scheduler, рестартует его. В запушенные планировщик добавляет
    новые таски.


    :return:
    """
    scheduler = Scheduler()
    scheduler_stop(scheduler)

    scheduler = Scheduler(stop_when_queue_is_empty=False)
    scheduler_restart(scheduler)
    # scheduler.run()

    with ThreadPoolExecutor() as pool:
        pool.submit(scheduler.run)
        pool.submit(add_task(scheduler))

    while threading.active_count() > 0:
        pass

    for task in scheduler.tasks:
        print(task.rezult)

    assert len(scheduler.tasks_completed) == 45
    assert len(scheduler.tasks_fail) == 0


def scheduler_restart(scheduler):
    scheduler.restart()


def scheduler_stop(scheduler):
    add_task(scheduler)
    scheduler.stop()


if __name__ == "__main__":
    main()
