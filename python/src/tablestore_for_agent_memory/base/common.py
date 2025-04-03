import time
from enum import Enum

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
