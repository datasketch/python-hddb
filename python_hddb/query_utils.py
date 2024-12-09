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
    if is_doing_grouping(params) and sort:
        order_parts = [part.strip() for part in sort.split(",")]
        filtered_order_parts = []
        for part in order_parts:
            column_name = part.split()[0]
            if column_name in params.row_group_cols:
                filtered_order_parts.append(part)

        if filtered_order_parts:
            current_order = filtered_order_parts[len(params.group_keys)]
            return f"ORDER BY {current_order}"

    if sort:
        return f"ORDER BY {sort}"
    return ""


def is_doing_grouping(params: FetchParams) -> bool:
    row_group_cols = params.row_group_cols
    group_keys = params.group_keys
    return len(row_group_cols) > len(group_keys)


def build_count_sql(params: FetchParams, from_sql: str, where_sql: str) -> str:
    """
    Build COUNT query based on grouping parameters

    Args:
        params (FetchParams): Query parameters
        from_sql (str): FROM clause
        where_sql (str): WHERE clause

    Returns:
        str: Complete COUNT query
    """
    if is_doing_grouping(params):
        row_group_col = params.row_group_cols[len(params.group_keys)]
        return f"""
            SELECT COUNT(*) 
            FROM (
                SELECT DISTINCT {row_group_col} 
                {from_sql} 
                {where_sql}
            )
        """
    return f"SELECT COUNT(*) {from_sql} {where_sql}"
