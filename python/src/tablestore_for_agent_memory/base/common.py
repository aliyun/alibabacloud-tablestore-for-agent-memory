import time
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Any, TypeVar, Generic

import tablestore


class MetaType(Enum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    BOOLEAN = "BOOLEAN"
    DOUBLE = "DOUBLE"
    BINARY = "BINARY"


class Order(Enum):
    ASC = tablestore.Direction.FORWARD
    DESC = tablestore.Direction.BACKWARD


def microseconds_timestamp() -> int:
    return int(round(time.time() * 1000000))


T = TypeVar('T')


@dataclass
class Response(Generic[T], ABC):
    hits: Optional[List[T]] = None
    """
    Hit data
    """

    next_token: Optional[str] = None
    """
    Pagination token
    """
