# utils/helpers.py
import random
import string
import uuid
from typing import Dict, List
import pandas as pd
from slugify import slugify


def generate_field_id(column_name: str, length: int = 6) -> str:
    """
    Generate a unique field ID for a column.

    Args:
        column_name (str): Original column name
        length (int, optional): Length of random string suffix. Defaults to 6.

    Returns:
        str: Generated field ID in format 'slugified_name_RANDOM'
    """
    if column_name == "rcd___id":
        return "rcd___id"

    random_suffix = "".join(random.choices(string.ascii_letters, k=length))
    slugified_name = slugify(column_name, separator="_", regex_pattern=r"[^a-z0-9_]+")
    return f"{slugified_name}_{random_suffix}"


def generate_field_metadata(df: pd.DataFrame) -> List[Dict[str, str]]:
    """
    Generate metadata for each column in the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame with column names

    Returns:
        List[Dict[str, str]]: List of dictionaries containing:
            - fld___id: UUID for the field
            - label: Original column name
            - id: Generated unique field ID

    Example:
        >>> df = pd.DataFrame(columns=['Name', 'Age'])
        >>> metadata = generate_field_metadata(df)
        >>> metadata[0]
        {
            'fld___id': '123e4567-e89b-12d3-a456-426614174000',
            'label': 'Name',
            'id': 'name_AbCdEf'
        }
    """
    return [
        {
            "fld___id": str(uuid.uuid4()),
            "label": column,
            "id": generate_field_id(column),
        }
        for column in df.columns
    ]


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame by handling missing values and converting types.

    Args:
        df (pd.DataFrame): Input DataFrame

    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    return df.astype(str).replace({"nan": "", "NaN": "", "None": "", float("nan"): ""})
