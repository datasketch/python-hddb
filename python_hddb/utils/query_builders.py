from ..models.params import FetchParams


def is_doing_grouping(params: FetchParams) -> bool:
    """
    Check if grouping should be applied based on parameters.

    Args:
        params (FetchParams): Query parameters containing grouping information

    Returns:
        bool: True if grouping should be applied
    """
    return len(params.row_group_cols) > len(params.group_keys)


def build_select_sql(params: FetchParams) -> str:
    """
    Build SELECT clause for query.

    Args:
        params (FetchParams): Query parameters containing selection information

    Returns:
        str: SELECT clause of the SQL query

    Example:
        >>> params = FetchParams(row_group_cols=['country'], group_keys=[])
        >>> build_select_sql(params)
        'SELECT cast(uuid() as varchar) as rcd___id, country'
    """
    if is_doing_grouping(params):
        cols_to_select = []
        row_group_col = params.row_group_cols[len(params.group_keys)]
        cols_to_select.append(row_group_col)
        return "SELECT cast(uuid() as varchar) as rcd___id," + ", ".join(cols_to_select)

    return "SELECT *"


def build_where_sql(params: FetchParams) -> str:
    """
    Build WHERE clause based on group keys.

    Args:
        params (FetchParams): Query parameters containing filtering information

    Returns:
        str: WHERE clause of the SQL query or empty string if no filters
    """
    if not params.group_keys or not params.row_group_cols:
        return ""

    where_parts = [
        f"\"{params.row_group_cols[idx]}\" = '{key}'"
        for idx, key in enumerate(params.group_keys)
    ]

    return " WHERE " + " AND ".join(where_parts) if where_parts else ""


def build_group_sql(params: FetchParams) -> str:
    """
    Build GROUP BY clause for grouped queries.

    Args:
        params (FetchParams): Query parameters containing grouping information

    Returns:
        str: GROUP BY clause of the SQL query or empty string if no grouping
    """
    if not is_doing_grouping(params):
        return ""

    row_group_col = params.row_group_cols[len(params.group_keys)]
    return f'GROUP BY "{row_group_col}"'


def build_order_sql(params: FetchParams) -> str:
    """
    Build ORDER BY clause considering grouping and sorting parameters.

    Args:
        params (FetchParams): Query parameters containing sorting information

    Returns:
        str: ORDER BY clause of the SQL query or empty string if no sorting
    """
    if not params.sort:
        return ""

    if is_doing_grouping(params):
        order_parts = [part.strip() for part in params.sort.split(",")]
        filtered_parts = [
            part for part in order_parts if part.split()[0] in params.row_group_cols
        ]

        if filtered_parts and len(filtered_parts) > len(params.group_keys):
            return f"ORDER BY {filtered_parts[len(params.group_keys)]}"

    return f"ORDER BY {params.sort}"


def build_count_sql(params: FetchParams, from_sql: str, where_sql: str) -> str:
    """
    Build COUNT query for pagination.

    Args:
        params (FetchParams): Query parameters
        from_sql (str): FROM clause of the main query
        where_sql (str): WHERE clause of the main query

    Returns:
        str: Complete COUNT query

    Example:
        >>> params = FetchParams(row_group_cols=['country'], group_keys=[])
        >>> build_count_sql(params, 'FROM table', 'WHERE x = 1')
        'SELECT COUNT(*) FROM (SELECT DISTINCT country FROM table WHERE x = 1)'
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
