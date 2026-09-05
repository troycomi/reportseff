"""Command line parameter collection."""

from __future__ import annotations

from dataclasses import dataclass

from .array_summary import parse_graph_format


@dataclass
class BaseQueryParameters:
    """Parameters shared by `report` and `summarize` for querying jobs.

    This is the "common bits" (querying slurm, building the datatable)
    referenced in the implementation plan: both commands' parameter classes
    inherit from this rather than repeating the field list.
    """

    color: bool
    jobs: tuple[str, ...]
    debug: bool = False
    format_str: str = ""
    sorting: str = "jobid"
    node: bool = False
    node_and_gpu: bool = False
    not_state: str = ""
    parsable: bool = False
    delimiter: str = ""
    since: str = ""
    until: str = ""
    state: str = ""
    slurm_format: str = ""
    user: str = ""
    partition: str = ""
    cluster: str = ""
    extra_args: str = ""
    array_min_size: int = 0  # Minimum size for job arrays to be reported

    def __post_init__(self) -> None:
        """Post init method to handle prepending format string with +."""
        if self.format_str.startswith("+"):
            self.format_str = (
                "JobID%>,State,Elapsed%>,TimeEff,CPUEff,MemEff," + self.format_str[1:]
            )


@dataclass
class ReportseffParameters(BaseQueryParameters):
    """Collection of parameters from the command line for `report`."""


@dataclass
class SummarizeParameters(BaseQueryParameters):
    """Collection of parameters from the command line for `summarize`."""

    group_by: str = "array"
    graph_style: str = "sparkline"
    graph_format: str = "runtime,cpueff,memeff"
    min_tasks: int = 50
    ascii_fallback: bool = False

    def __post_init__(self) -> None:
        """Apply the shared format_str convenience, then validate graph_format.

        Raises:
            ValueError: if --graph-format names an unrecognized metric.
        """
        super().__post_init__()
        parse_graph_format(self.graph_format)
