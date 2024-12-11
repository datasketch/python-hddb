# models/params.py
from typing import Dict, List, Optional
from pydantic import BaseModel, Field, field_validator
from .filters import FilterModel


class FetchParams(BaseModel):
    """
    Parameters for fetching data with pagination, sorting, filtering, and grouping.

    Attributes:
        start_row (int): Starting row for pagination (inclusive)
        end_row (int): Ending row for pagination (exclusive)
        sort (Optional[str]): Sorting expression (e.g., "column ASC")
        filter_model (Optional[Dict[str, FilterModel]]): Filters to apply
        row_group_cols (List[str]): Columns to group by
        group_keys (List[str]): Values to group by
    """

    start_row: int = Field(..., ge=0, description="Starting row (inclusive)")
    end_row: int = Field(..., gt=0, description="Ending row (exclusive)")
    sort: Optional[str] = None
    filter_model: Optional[Dict[str, FilterModel]] = None
    row_group_cols: List[str] = Field(default_factory=list)
    group_keys: List[str] = Field(default_factory=list)

    @field_validator("end_row")
    def end_row_must_be_greater_than_start_row(cls, v, values):
        if "start_row" in values and v <= values["start_row"]:
            raise ValueError("end_row must be greater than start_row")
        return v


class FieldsParams(BaseModel):
    """
    Parameters for field operations.

    Attributes:
        with_categories (bool): Whether to include category information
    """

    with_categories: bool = Field(
        default=False, description="Include category information in the response"
    )
