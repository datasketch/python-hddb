# models/filters.py
from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional


class TextFilterType(str, Enum):
    """
    Enumeration of available text filter types.

    Attributes:
        EQUALS: Exact match
        NOT_EQUAL: Inverse of exact match
        CONTAINS: Substring match
        NOT_CONTAINS: Inverse of substring match
        STARTS_WITH: Prefix match
        ENDS_WITH: Suffix match
        IS_NULL: Null value check
        IS_NOT_NULL: Non-null value check
    """

    EQUALS = "equals"
    NOT_EQUAL = "notEqual"
    CONTAINS = "contains"
    NOT_CONTAINS = "notContains"
    STARTS_WITH = "startsWith"
    ENDS_WITH = "endsWith"
    IS_NULL = "isNull"
    IS_NOT_NULL = "isNotNull"


class TextFilter(BaseModel):
    """
    Model for text-based filtering.

    Attributes:
        type (TextFilterType): Type of text filter to apply
        filter (str): Value to filter by
    """

    type: TextFilterType
    filter: str = Field(..., description="Value to filter by")


class FilterModel(BaseModel):
    """
    Model for all types of filters.

    Attributes:
        filterType (str): Type of filter, currently only supports "text"
        type (TextFilterType): Specific filter operation to apply
        filter (str): Value to filter by
    """

    filterType: str = Field(default="text", description="Type of filter")
    type: TextFilterType
    filter: str
