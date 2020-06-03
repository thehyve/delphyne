from typing import Dict, Optional

import pandas as pd


def replace_substrings(string: str, mapping: Dict[str, str]) -> str:
    """
    Replace substrings in string, based on the items in mapping.
    :param string:
        The string to be edited
    :param mapping: Dict[str, str]
        Dictionary containing the replacements as {old: new}
    :return: str
    """
    for old, new in mapping.items():
        string = string.replace(old, new)
    return string


def construct_full_date_value(date: Optional[str],
                              fallback_date: Optional[str] = None
                              ) -> Optional[str]:
    """
    Turn a partial date value into a full yyyy-mm-dd date string.
    If only year is available, return yyyy-07-01.
    If year and month are available, return yyyy-mm-15.
    :param date: str
        date value as present in the source data
    :param fallback_date: str
        if true, a fallback date will be returned when date is empty
    :return: str or NaN
        the full date value, if available
    """
    if pd.isnull(date) or date == '':
        if fallback_date is not None:
            return fallback_date
        return date
    elif len(date) == 10:  # a full date requires no changes
        return date
    date_parts = date.split('-')
    year = date_parts[0]
    month = date_parts[1] if len(date_parts) == 2 else '07'
    day = '01' if len(date_parts) == 1 else '15'
    return '-'.join([year, month, day])
