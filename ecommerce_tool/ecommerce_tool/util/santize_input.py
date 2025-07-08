import pandas as pd

def sanitize_value(value, default=None, value_type=None):
    """
    Sanitize values to prevent NaN/None from being saved to the database.
    
    Args:
        value: The value to sanitize
        default: Default value if NaN/None (defaults to 0 for numbers, "" for strings)
        value_type: Expected type ('float', 'int', 'str')
    
    Returns:
        Sanitized value of the specified type.
    """
    # Handle None and NaN from pandas or numpy
    if value is None or pd.isna(value):
        if default is not None:
            return default
        if value_type == float:
            return 0.0
        elif value_type == int:
            return 0
        elif value_type == str:
            return ""
        else:
            return None

    # Handle "nan" or "NaN" as string
    if isinstance(value, str) and value.strip().lower() == 'nan':
        if value_type == float:
            return 0.0
        elif value_type == int:
            return 0
        else:
            return ""

    # Attempt conversion
    try:
        if value_type == float:
            return float(value)
        elif value_type == int:
            return int(float(value))  # handles float strings like "5.0"
        elif value_type == str:
            return str(value)
    except (ValueError, TypeError):
        if value_type == float:
            return 0.0
        elif value_type == int:
            return 0
        elif value_type == str:
            return ""

    # Fallback
    return value
