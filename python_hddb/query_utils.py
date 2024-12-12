from .models import FetchParams


def build_select_sql(params: FetchParams) -> str:
    """Build SELECT clause based on grouping parameters"""
    row_group_cols = params.row_group_cols
    group_keys = params.group_keys

    if not is_doing_grouping(params):
        return "SELECT *"

    # Si estamos viendo elementos específicos de un grupo
    if len(row_group_cols) == len(group_keys):
        return "SELECT cast(uuid() as varchar) as rcd___id, *"

    # Si hay más de un nivel de agrupación y estamos en un nivel intermedio
    if len(row_group_cols) > len(group_keys):
        current_group_col = row_group_cols[len(group_keys)].split(":")[0]
        return f'SELECT cast(uuid() as varchar) as rcd___id, "{current_group_col}"'

    return "SELECT *"


def build_where_sql(params: FetchParams) -> str:
    """Build WHERE clause for expanded groups"""
    group_keys = params.group_keys
    row_group_cols = params.row_group_cols
    where_parts = []

    for idx, key in enumerate(group_keys):
        col = row_group_cols[idx].split(":")[0]
        where_parts.append(f"\"{col}\" = '{key}'")

    return " WHERE " + " AND ".join(where_parts) if where_parts else ""


def build_group_sql(params: FetchParams) -> str:
    """Build GROUP BY clause"""
    row_group_cols = params.row_group_cols
    group_keys = params.group_keys

    if not is_doing_grouping(params):
        return ""

    # No agrupamos si estamos viendo elementos finales
    if len(row_group_cols) == len(group_keys):
        return ""

    if len(row_group_cols) > len(group_keys):
        current_group_col = row_group_cols[len(group_keys)].split(":")[0]
        return f'GROUP BY "{current_group_col}"'

    return ""


def build_order_sql(params: FetchParams) -> str:
    """Build ORDER BY clause"""
    row_group_cols = params.row_group_cols
    group_keys = params.group_keys

    # Si no hay agrupación
    if not is_doing_grouping(params):
        return f"ORDER BY {params.sort}" if params.sort else ""

    # Si estamos viendo elementos finales y hay sort
    if len(row_group_cols) == len(group_keys):
        return f"ORDER BY {params.sort}" if params.sort else ""

    # Si estamos en un nivel de agrupación
    if len(row_group_cols) > len(group_keys):
        current_group_col = row_group_cols[len(group_keys)]
        group_parts = current_group_col.split(":")
        group_col = group_parts[0]
        group_order = group_parts[1] if len(group_parts) > 1 else "asc"
        return f'ORDER BY "{group_col}" {group_order}'

    return ""


def is_doing_grouping(params: FetchParams) -> bool:
    """Check if we are doing any kind of grouping"""
    return bool(params.row_group_cols)


def build_count_sql(params: FetchParams, from_sql: str, where_sql: str) -> str:
    """Build COUNT query based on grouping parameters"""
    if not is_doing_grouping(params):
        return f"SELECT COUNT(*) {from_sql} {where_sql}"

    # Si estamos viendo elementos finales
    if len(params.row_group_cols) == len(params.group_keys):
        return f"SELECT COUNT(*) {from_sql} {where_sql}"

    # Si estamos en un nivel de agrupación
    row_group_col = params.row_group_cols[len(params.group_keys)].split(":")[0]
    return f"""
        SELECT COUNT(*) 
        FROM (
            SELECT DISTINCT \"{row_group_col}\"
            {from_sql} 
            {where_sql}
        )
    """
