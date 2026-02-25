"""Custom types for pytest functions and fixtures."""

from collections.abc import Callable
from typing import Any

get_jobstats_func = Callable[[dict[str, Any]], str]
sacct_return = list[dict[str, str]]
strip_js_func = Callable[[str, list[str]], str]
