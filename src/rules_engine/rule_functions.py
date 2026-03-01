"""
Rule Functions Library
=====================
Built-in functions available inside rule expressions.

These map to Ab Initio DML built-in functions:

  Ab Initio DML Function    →  Python Equivalent
  ──────────────────────────────────────────────────
  is_null(field)            →  is_null(value)
  is_blank(field)           →  is_blank(value)
  string_length(field)      →  string_length(value)
  string_substring(f,s,l)   →  string_substring(value, start, length)
  decimal_round(field, n)   →  decimal_round(value, n)
  date_difference(d1, d2)   →  date_difference(d1, d2)
  lookup(key, table)        →  (handled by engine directly)
"""

from datetime import datetime, date
from typing import Any, Optional


def is_null(value: Any) -> bool:
    """Check if value is null/None. Maps to Ab Initio is_null()."""
    return value is None


def is_blank(value: Any) -> bool:
    """Check if value is null, empty, or whitespace. Maps to Ab Initio is_blank()."""
    if value is None:
        return True
    return str(value).strip() == ''


def string_length(value: Any) -> int:
    """Get string length. Maps to Ab Initio string_length()."""
    if value is None:
        return 0
    return len(str(value))


def string_substring(value: Any, start: int, length: int) -> str:
    """Extract substring. Maps to Ab Initio string_substring()."""
    if value is None:
        return ''
    s = str(value)
    return s[start:start + length]


def decimal_round(value: Any, places: int = 2) -> float:
    """Round a decimal. Maps to Ab Initio decimal_round()."""
    if value is None:
        return 0.0
    return round(float(value), places)


def date_difference(date1: str, date2: str, unit: str = 'days') -> int:
    """
    Calculate date difference. Maps to Ab Initio date_difference().
    
    Args:
        date1, date2: Date strings in YYYY-MM-DD format
        unit: 'days', 'months', or 'years'
    """
    d1 = datetime.strptime(str(date1), '%Y-%m-%d')
    d2 = datetime.strptime(str(date2), '%Y-%m-%d')
    
    delta = d2 - d1
    
    if unit == 'days':
        return delta.days
    elif unit == 'months':
        return (d2.year - d1.year) * 12 + d2.month - d1.month
    elif unit == 'years':
        return d2.year - d1.year
    return delta.days


def coalesce(*values) -> Any:
    """Return first non-null value. Maps to Ab Initio reinterpret_as / coalesce patterns."""
    for v in values:
        if v is not None:
            return v
    return None


def in_list(value: Any, values_list: list) -> bool:
    """Check membership in a list. Utility for rule conditions."""
    return value in values_list


def between(value: Any, low: Any, high: Any) -> bool:
    """Range check inclusive. Common Ab Initio pattern."""
    try:
        v = float(value)
        return float(low) <= v <= float(high)
    except (ValueError, TypeError):
        return False


# Registry of functions available to rule expressions
RULE_FUNCTIONS = {
    'is_null': is_null,
    'is_blank': is_blank,
    'string_length': string_length,
    'string_substring': string_substring,
    'decimal_round': decimal_round,
    'date_difference': date_difference,
    'coalesce': coalesce,
    'in_list': in_list,
    'between': between,
}
