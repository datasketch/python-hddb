# utils.py
from .models import FetchParams


def build_select_sql(params: FetchParams) -> str:
    """Build SELECT clause based on grouping parameters"""

    row_group_cols = params.row_group_cols
    group_keys = params.group_keys
    if is_doing_grouping(params):
        cols_to_select = []
        row_group_col = row_group_cols[len(group_keys)]
        cols_to_select.append(row_group_col)
        return "SELECT cast(uuid() as varchar) as rcd___id," + ", ".join(cols_to_select)

    return "SELECT *"


def build_where_sql(params: FetchParams) -> str:
    """Build WHERE clause for expanded groups"""
    group_keys = params.group_keys
    row_group_cols = params.row_group_cols
    where_parts = []
    for idx, key in enumerate(group_keys):
        col = row_group_cols[idx]
        where_parts.append(f"\"{col}\" = '{key}'")

    return " WHERE " + " AND ".join(where_parts) if where_parts else ""


def build_group_sql(params: FetchParams) -> str:
    """Build GROUP BY clause"""
    row_group_cols = params.row_group_cols
    group_keys = params.group_keys
    if is_doing_grouping(params):
        cols_to_group_by = []
        row_group_col = row_group_cols[len(group_keys)]
        cols_to_group_by.append(row_group_col)
        return f'GROUP BY {", ".join(cols_to_group_by)}'
    return ""


def build_order_sql(params: FetchParams) -> str:
    """Build ORDER BY clause"""
    sort = params.sort
    if sort:
        return f" ORDER BY {sort}"
    return ""


def is_doing_grouping(params: FetchParams) -> bool:
    row_group_cols = params.row_group_cols
    group_keys = params.group_keys
    return len(row_group_cols) > len(group_keys)
