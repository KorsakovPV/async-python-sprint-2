import datetime
from typing import Optional, Any

from pydantic import BaseModel

class TaskSchemaInnner(BaseModel):
    fn_name: str
    args: list
    kwargs: dict
    start_datetime_stamp: datetime.datetime
    max_working_time: Optional[int]
    tries: int
    dependencies: list
class TaskSchema(TaskSchemaInnner):
    # fn_name: str
    # args: list
    # kwargs: dict
    # start_datetime_stamp: datetime.datetime
    # max_working_time: Optional[int]
    # tries: int
    dependencies: list[TaskSchemaInnner]
