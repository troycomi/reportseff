"""Tabular efficiency with reportseff."""

try:
    from importlib.metadata import PackageNotFoundError, version  # type: ignore[import]
except ImportError:  # pragma: no cover
    from importlib.metadata import PackageNotFoundError, version  # type: ignore[import]


try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
