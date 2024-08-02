"""Command line parameter collection."""


class ReportseffParameters:
    """Collection of parameters from the command line.

    Basically a dataclass but I want to be python 3.6 compatible without the
    pip package
    """

    color: bool
    debug: bool = False
    format_str: str = ""
    jobs: tuple
    modified_sort: bool = False
    node: bool = False
    node_and_gpu: bool = False
    not_state: str = ""
    parsable: bool = False
    since: str = ""
    until: str = ""
    state: str = ""
    slurm_format: str = ""
    user: str = ""
    partition: str = ""
    extra_args: str = ""

    def __init__(
        self,
        jobs: tuple,
        *,
        color: bool,
        debug: bool = False,
        format_str: str = "",
        modified_sort: bool = False,
        node: bool = False,
        node_and_gpu: bool = False,
        not_state: str = "",
        parsable: bool = False,
        since: str = "",
        until: str = "",
        state: str = "",
        slurm_format: str = "",
        user: str = "",
        partition: str = "",
        extra_args: str = "",
    ) -> None:
        """Create a new parameter object.

        Args:
            jobs: job ids, files, or directory to query
            color: if the output should be colored
            debug: if debug information should be printed
            format_str: format string specification
            modified_sort: if output should be sorted by modified time
            node: if node information should be displayed
            node_and_gpu: if node and gpu information should be displayed
            not_state: states to remove
            parsable: if output should be parsable
            since: display jobs since a specific time
            until: display jobs until a specific time
            state: stats to display
            slurm_format: format string passed to sbatch
            user: the user to report for
            partition: the partition to report for
            extra_args: extra arguments to forward to sacct
        """
        self.color = color
        self.debug = debug
        self.format_str = format_str
        self.jobs = jobs
        self.modified_sort = modified_sort
        self.node = node
        self.node_and_gpu = node_and_gpu
        self.not_state = not_state
        self.parsable = parsable
        self.since = since
        self.until = until
        self.state = state
        self.slurm_format = slurm_format
        self.user = user
        self.partition = partition
        self.extra_args = extra_args

        if self.format_str.startswith("+"):
            self.format_str = (
                "JobID%>,State,Elapsed%>,TimeEff,CPUEff,MemEff," + self.format_str[1:]
            )
