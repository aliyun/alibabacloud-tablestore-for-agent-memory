from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, Field


class FilterType(Enum):
    Operator = 1
    Condition = 2


class Filter(BaseModel, ABC):
    @abstractmethod
    def filter_type(self) -> FilterType:
        pass


class BaseConditionFilter(Filter):
    filters: list[Filter] = Field(default=None)

    def __init__(self, filters: list[Filter]):
        super().__init__()
        self.filters = filters

    def filter_type(self) -> FilterType:
        return FilterType.Condition


class AND(BaseConditionFilter):

    def __init__(self, filters: list[Filter]):
        super().__init__(filters)


class OR(BaseConditionFilter):

    def __init__(self, filters: list[Filter]):
        def __init__(self, filters: list[Filter]):
            super().__init__(filters)


class NOT(BaseConditionFilter):

    def __init__(self, filters: list[Filter]):
        super().__init__(filters)


class BaseOperatorFilter(Filter):
    meta_key: str = Field(default=None)
    meta_value: Optional[Union[int, float, bool, str]] = Field(default=None)

    def __init__(self, meta_key: str, meta_value: Optional[Union[int, float, bool, str]]):
        super().__init__()
        self.meta_key = meta_key
        self.meta_value = meta_value

    def filter_type(self) -> FilterType:
        return FilterType.Operator


class Eq(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Optional[Union[int, float, bool, str]]):
        super().__init__(meta_key, meta_value)


class NotEQ(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Optional[Union[int, float, bool, str]]):
        super().__init__(meta_key, meta_value)


class GT(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Optional[Union[int, float, bool, str]]):
        super().__init__(meta_key, meta_value)


class LT(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Optional[Union[int, float, bool, str]]):
        super().__init__(meta_key, meta_value)


class GTE(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Optional[Union[int, float, bool, str]]):
        super().__init__(meta_key, meta_value)


class LTE(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Optional[Union[int, float, bool, str]]):
        super().__init__(meta_key, meta_value)


class Filters(ABC):

    @staticmethod
    def logical_and(filters: list[Filter]) -> Filter:
        return AND(filters)

    @staticmethod
    def logical_or(filters: list[Filter]) -> Filter:
        return OR(filters)

    @staticmethod
    def logical_not(filters: list[Filter]) -> Filter:
        return NOT(filters)

    @staticmethod
    def eq(meta_key: str, meta_value: Optional[Union[int, float, bool, str]]) -> Filter:
        return Eq(meta_key, meta_value)

    @staticmethod
    def not_eq(meta_key: str, meta_value: Optional[Union[int, float, bool, str]]) -> Filter:
        return NotEQ(meta_key, meta_value)

    @staticmethod
    def gt(meta_key: str, meta_value: Optional[Union[int, float, bool, str]]) -> Filter:
        return GT(meta_key, meta_value)

    @staticmethod
    def lt(meta_key: str, meta_value: Optional[Union[int, float, bool, str]]) -> Filter:
        return LT(meta_key, meta_value)

    @staticmethod
    def gte(meta_key: str, meta_value: Optional[Union[int, float, bool, str]]) -> Filter:
        return GTE(meta_key, meta_value)

    @staticmethod
    def lte(meta_key: str, meta_value: Optional[Union[int, float, bool, str]]) -> Filter:
        return LTE(meta_key, meta_value)
