# utils.py
from .models import FetchParams


def build_select_sql(params: FetchParams) -> str:
    """Build SELECT clause based on grouping parameters"""
    row_group_cols = params.get("row_group_cols", [])
    group_keys = params.get("group_keys", [])
    if row_group_cols and group_keys:
        if len(row_group_cols) > len(group_keys):
            current_group_col = row_group_cols[len(group_keys)]
            return f'SELECT DISTINCT "{current_group_col.field}"'
    return "SELECT *"


def build_where_sql(params: FetchParams) -> str:
    """Build WHERE clause for expanded groups"""
    group_keys = params.get("group_keys", [])
    row_group_cols = params.get("row_group_cols", [])
    if not group_keys or not row_group_cols:
        return ""

    where_parts = []
    for idx, key in enumerate(params.group_keys):
        if idx < len(row_group_cols):  # Asegurar que tenemos la columna correspondiente
            col = row_group_cols[idx].field
            where_parts.append(f"\"{col}\" = '{key}'")

    return " WHERE " + " AND ".join(where_parts) if where_parts else ""


def build_group_sql(params: FetchParams) -> str:
    """Build GROUP BY clause"""
    row_group_cols = params.get("row_group_cols", [])
    group_keys = params.get("group_keys", [])
    if row_group_cols and group_keys:
        if len(row_group_cols) > len(group_keys):
            current_group_col = row_group_cols[len(group_keys)]
            return f' GROUP BY "{current_group_col.field}"'
    return ""


def build_order_sql(params: FetchParams) -> str:
    """Build ORDER BY clause"""
    sort = params.get("sort", None)
    if sort:
        return f" ORDER BY {sort}"
    return ""


# def get_last_row(params: FetchParams, result_size: int) -> int:
#     end_row = params.get("end_row", 0)
#     start_row = params.get("start_row", 0)
#     if result_size < (end_row - start_row):
#         return start_row + result_size
#     return -1
