from enum import Enum
from typing import TypedDict


class RecnodeDoneTaskStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class RecnodeDoneTaskResult(TypedDict):
    status: str
    message: str
