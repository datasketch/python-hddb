from enum import Enum
from typing import List, Dict, Optional
from pydantic import BaseModel


class TextFilterType(str, Enum):
    EQUALS = "equals"
    NOT_EQUAL = "notEqual"
    CONTAINS = "contains"
    NOT_CONTAINS = "notContains"
    STARTS_WITH = "startsWith"
    ENDS_WITH = "endsWith"
    IS_NULL = "isNull"
    IS_NOT_NULL = "isNotNull"


class TextFilter(BaseModel):
    type: TextFilterType
    filter: str


class FilterModel(BaseModel):
    filterType: str = "text"  # Por ahora solo texto
    type: TextFilterType
    filter: str


class FetchParams(BaseModel):
    start_row: int
    end_row: int
    sort: Optional[str] = None
    filter_model: Optional[Dict[str, FilterModel]] = None
    group: Optional[str] = None


class FieldsParams(BaseModel):
    with_categories: Optional[bool] = False
