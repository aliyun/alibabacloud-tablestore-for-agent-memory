from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Union, List

from pydantic import BaseModel, Field


class FilterType(Enum):
    Operator = 1
    Condition = 2


class Filter(BaseModel, ABC):
    @abstractmethod
    def filter_type(self) -> FilterType:
        pass

    def __str__(self):
        class_name = self.__class__.__name__
        return f"{class_name}({super().__str__()})"


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
        super().__init__(filters)


class NOT(BaseConditionFilter):

    def __init__(self, filters: list[Filter]):
        super().__init__(filters)


class BaseOperatorFilter(Filter):
    meta_key: Optional[str] = Field(default=None)
    meta_value: Optional[Union[int, float, bool, str]] = Field(default=None)

    def __init__(self, meta_key: Optional[str]=None, meta_value: Optional[Union[int, float, bool, str]]=None):
        super().__init__()
        self.meta_key = meta_key
        self.meta_value = meta_value

    def filter_type(self) -> FilterType:
        return FilterType.Operator


class Eq(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Union[int, float, bool, str]):
        super().__init__(meta_key, meta_value)


class NotEQ(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Union[int, float, bool, str]):
        super().__init__(meta_key, meta_value)


class GT(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Union[int, float, bool, str]):
        super().__init__(meta_key, meta_value)


class LT(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Union[int, float, bool, str]):
        super().__init__(meta_key, meta_value)


class GTE(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value: Union[int, float, bool, str]):
        super().__init__(meta_key, meta_value)


class LTE(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_value:Union[int, float, bool, str]):
        super().__init__(meta_key, meta_value)

class All(BaseOperatorFilter):
    def __init__(self):
        super().__init__(None,None)

class IN(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_values: List[Union[int, float, bool, str]]):
        super().__init__(meta_key, None)
        self.meta_values=meta_values
        
class NotIN(BaseOperatorFilter):

    def __init__(self, meta_key: str, meta_values: List[Union[int, float, bool, str]]):
        super().__init__(meta_key, None)
        self.meta_values=meta_values

class TextMatch(BaseOperatorFilter):
    def __init__(self, meta_key: str, meta_value: str):
        super().__init__(meta_key, meta_value)
        
class TextMatchPhrase(BaseOperatorFilter):
    
    def __init__(self, meta_key: str, meta_value: str):
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
    def eq(meta_key: str, meta_value: Union[int, float, bool, str]) -> Filter:
        return Eq(meta_key, meta_value)

    @staticmethod
    def not_eq(meta_key: str, meta_value:Union[int, float, bool, str]) -> Filter:
        return NotEQ(meta_key, meta_value)

    @staticmethod
    def gt(meta_key: str, meta_value: Union[int, float, bool, str]) -> Filter:
        return GT(meta_key, meta_value)

    @staticmethod
    def lt(meta_key: str, meta_value: Union[int, float, bool, str]) -> Filter:
        return LT(meta_key, meta_value)

    @staticmethod
    def gte(meta_key: str, meta_value:Union[int, float, bool, str]) -> Filter:
        return GTE(meta_key, meta_value)

    @staticmethod
    def lte(meta_key: str, meta_value:Union[int, float, bool, str]) -> Filter:
        return LTE(meta_key, meta_value)
    
    @staticmethod
    def _in(meta_key: str, meta_values: List[Union[int, float, bool, str]]) -> Filter:
        return IN(meta_key, meta_values)
    
    @staticmethod
    def not_in(meta_key: str, meta_values: List[Union[int, float, bool, str]]) -> Filter:
        return NotIN(meta_key, meta_values)
    
    @staticmethod
    def text_match(meta_key: str, meta_value:str) -> Filter:
        return TextMatch(meta_key, meta_value)
    
    @staticmethod
    def text_match_phrase(meta_key: str, meta_value:str) -> Filter:
        return TextMatchPhrase(meta_key, meta_value)
    
    @staticmethod
    def all() -> Filter:
        return All()
