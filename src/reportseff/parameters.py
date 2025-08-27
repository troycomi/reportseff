"""Command line parameter collection."""

from dataclasses import dataclass


@dataclass
class ReportseffParameters:
    """Collection of parameters from the command line.

    Basically a dataclass but I want to be python 3.6 compatible without the
    pip package
    """

    color: bool
    jobs: tuple
    debug: bool = False
    format_str: str = ""
    modified_sort: bool = False
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
