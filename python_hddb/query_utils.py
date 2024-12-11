from .models import FetchParams


def build_select_sql(params: FetchParams) -> str:
    """Build SELECT clause based on grouping parameters"""
    row_group_cols = params.row_group_cols
    group_keys = params.group_keys

    if is_doing_grouping(params):
        cols_to_select = []
        row_group_col = row_group_cols[len(group_keys)].split(":")[
            0
        ]  # Extract just the column name

        if not group_keys:  # If no group_keys, we only need the category
            cols_to_select.append(f'"{row_group_col}"')
        else:  # If there are group_keys, we need all columns
            cols_to_select.append("*")

        return "SELECT cast(uuid() as varchar) as rcd___id, " + ", ".join(
            cols_to_select
        )

    return "SELECT *"


def build_where_sql(params: FetchParams) -> str:
    """Build WHERE clause for expanded groups"""
    group_keys = params.group_keys
    row_group_cols = params.row_group_cols
    where_parts = []

    for idx, key in enumerate(group_keys):
        col = row_group_cols[idx].split(":")[0]  # Extract just the column name
        where_parts.append(f"\"{col}\" = '{key}'")

    return " WHERE " + " AND ".join(where_parts) if where_parts else ""


def build_group_sql(params: FetchParams) -> str:
    """Build GROUP BY clause"""
    row_group_cols = params.row_group_cols
    group_keys = params.group_keys

    if is_doing_grouping(params) and not group_keys:  # Only group if no group_keys
        cols_to_group_by = []
        row_group_col = row_group_cols[len(group_keys)].split(":")[0]
        cols_to_group_by.append(f'"{row_group_col}"')
        return f'GROUP BY {", ".join(cols_to_group_by)}'

    return ""


def build_order_sql(params: FetchParams) -> str:
    """Build ORDER BY clause"""
    if is_doing_grouping(params):
        current_group_col = params.row_group_cols[len(params.group_keys)]
        group_parts = current_group_col.split(":")
        group_col = group_parts[0]
        group_order = group_parts[1] if len(group_parts) > 1 else "asc"

        if not params.group_keys:  # If no group_keys
            return f'ORDER BY "{group_col}" {group_order}'
        else:  # If there are group_keys and sort
            # In this case we can use sort for internal ordering
            if params.sort:
                return f"ORDER BY {params.sort}"

    elif params.sort:
        return f"ORDER BY {params.sort}"

    return ""


def is_doing_grouping(params: FetchParams) -> bool:
    row_group_cols = params.row_group_cols
    group_keys = params.group_keys
    return len(row_group_cols) > len(group_keys)


def build_count_sql(params: FetchParams, from_sql: str, where_sql: str) -> str:
    """Build COUNT query based on grouping parameters"""
    if is_doing_grouping(params):
        row_group_col = params.row_group_cols[len(params.group_keys)].split(":")[0]
        return f"""
            SELECT COUNT(*) 
            FROM (
                SELECT DISTINCT \"{row_group_col}\"
                {from_sql} 
                {where_sql}
            )
        """
    return f"SELECT COUNT(*) {from_sql} {where_sql}"
