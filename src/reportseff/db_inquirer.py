"""Abstract and concrete implementations of scheduler databases."""

from __future__ import annotations

import datetime
import re
import shlex
import shutil
import subprocess
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import click

if TYPE_CHECKING:
    from collections.abc import Callable

# Fallback mechanism for GPU metrics recovery.
# Jobstats is migrating GPU metrics storage from Slurm AdminComment to Prometheus.
# See: https://github.com/PrincetonUniversity/jobstats/pull/55
# When the optional mirror_to_admin_comment config flag is not enabled,
# reportseff can recover GPU metrics from jobstats via the -b flag.
# This allows continued GPU efficiency reporting even if AdminComment is unavailable.

# Minimum length for a valid AdminComment payload (mirrors job.ADMIN_COMMENT_MIN_LENGTH)
_ADMIN_COMMENT_MIN_LENGTH = 10

# Module-level cache so the availability check runs at most once per process.
_JOBSTATS_AVAILABLE: bool | None = None


def _check_jobstats_available() -> bool:
    """Return True if `jobstats -b` is usable on the current PATH.

    The result is cached after the first call so subsequent invocations are
    free.

    Returns:
        True when `jobstats` is found on PATH **and** its help text confirms
        the ``-b`` / ``--base64`` flag is present.
    """
    global _JOBSTATS_AVAILABLE  # noqa: PLW0603
    if _JOBSTATS_AVAILABLE is not None:
        return _JOBSTATS_AVAILABLE

    if shutil.which("jobstats") is None:
        _JOBSTATS_AVAILABLE = False
        return False

    try:
        result = subprocess.run(
            ["jobstats", "-h"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf8",
            timeout=10,
        )
        help_text = result.stdout
    except Exception:  # noqa: BLE001
        _JOBSTATS_AVAILABLE = False
        return False

    _JOBSTATS_AVAILABLE = "--base64" in help_text or "-b" in help_text
    return _JOBSTATS_AVAILABLE


def augment_with_jobstats(
    rows: list[dict[str, str]],
    *,
    debug_cmd: "Callable[[str], Any] | None" = None,
) -> list[dict[str, str]]:
    """Fill missing AdminComment values using `jobstats -b`.

    For rows whose ``AdminComment`` is absent or shorter than the minimum
    threshold, this function calls ``jobstats <id1> <id2> … -b`` in a single
    subprocess and injects the returned base64 payloads back as
    ``JS1:<payload>``, which is the format expected by
    :func:`~reportseff.job._parse_admin_comment_to_dict`.

    The subprocess call is batched (one fork for all missing rows) to avoid
    spawning one process per array-job task.  jobstats processes IDs in order
    and prints each result before moving to the next, so on a partial failure
    the output lines map positionally to the *first K* input IDs.

    Args:
        rows: sacct rows as returned by :meth:`SacctInquirer.get_db_output`.
        debug_cmd: optional callable to receive a debug message.

    Returns:
        The same list with ``AdminComment`` updated in-place for matched rows.
    """
    # Collect rows that need augmentation.  Skip .batch/.extern sub-steps
    # (JobIDRaw is empty or equals the parent) and rows already having data.
    missing_idx: list[int] = []
    missing_rawids: list[str] = []

    for i, row in enumerate(rows):
        admin = row.get("AdminComment", "")
        rawid = row.get("JobIDRaw", "")
        if (
            len(admin) <= _ADMIN_COMMENT_MIN_LENGTH
            and rawid
            and "." not in row.get("JobID", "")
        ):
            missing_idx.append(i)
            missing_rawids.append(rawid)

    if not missing_rawids:
        return rows

    if debug_cmd is not None:
        debug_cmd(
            f"jobstats fallback: fetching GPU metrics for "
            f"{len(missing_rawids)} job(s) via `jobstats -b`"
        )

    try:
        proc = subprocess.run(
            ["jobstats", *missing_rawids, "-b"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf8",
        )
    except Exception:  # noqa: BLE001
        # jobstats unavailable or crashed entirely — leave rows unchanged.
        return rows

    output_lines = [line for line in proc.stdout.splitlines() if line]

    # Map lines back to input IDs positionally.  jobstats exits on the first
    # failure so stdout may contain fewer lines than requested.
    for list_pos, row_idx in enumerate(missing_idx):
        if list_pos >= len(output_lines):
            break
        payload = output_lines[list_pos]
        # Skip jobstats sentinel values.
        if payload in ("None", "Short"):
            continue
        rows[row_idx]["AdminComment"] = "JS1:" + payload

    return rows


class BaseInquirer(ABC):
    """Abstract interface for inquiring different schedulers."""

    @abstractmethod
    def __init__(self) -> None:
        """Initialize a new inquirer."""

    @abstractmethod
    def get_valid_formats(self) -> list[str]:
        """Get the valid formatting options supported by the inquirer.

        Returns:
            List of valid format options
        """

    @abstractmethod
    def set_sacct_args(self, jobs: list[str]) -> list[str]:
        """Set arguments of sacct query.

        Args:
            jobs: list of job names

        Returns:
            String of sacct arguments

        Raises:
            RuntimeError: if sacct doesn't return properly
        """

    @abstractmethod
    def get_db_output(
        self,
        columns: list[str],
        jobs: list[str],
        debug_cmd: Callable[[str], Any] | None,
    ) -> list[dict[str, str]]:
        """Query the database with the supplied columns.

        Args:
            columns: validated format names as strings
            jobs: list of job names
            debug_cmd: If specified, the raw output will passed to this function

        Returns:
            List of rows, where each row is a dictionary
            with the columns as keys and entries as values
            Output order is not garunteed to match the jobs list

        """

    @abstractmethod
    def set_user(self, user: str) -> None:
        """Set the collection of jobs based on the provided user.

        Args:
            user: user name
        """

    @abstractmethod
    def set_partition(self, partition: str) -> None:
        """Set the collection of jobs based on the provided partition.

        Args:
            partition: partition name
        """

    @abstractmethod
    def set_cluster(self, cluster: str) -> None:
        """Set the collection of jobs based on the provided cluster.

        Args:
            cluster: cluster name
        """

    @abstractmethod
    def set_extra_args(self, extra_args: str) -> None:
        """Set extra arguments to be forwarded to sacct.

        Args:
            extra_args: list of arguments
        """

    @abstractmethod
    def all_users(self) -> None:
        """Ignore provided jobs, query for all users."""

    @abstractmethod
    def set_state(self, state: str) -> None:
        """Set the state to include output jobs.

        Args:
            state: comma separated list of state names or codes
        """

    @abstractmethod
    def set_not_state(self, state: str) -> None:
        """Set the state to exclude from output jobs.

        Args:
            state: comma separated list of state names or codes
        """

    @abstractmethod
    def parse_date(self, d: str) -> str:
        """Parse and convert custom string date format.

        Args:
            d: the string of date.

        Returns:
            converted string of date
        """

    @abstractmethod
    def set_until(self, until: str) -> None:
        """Set the filter for time of jobs to consider.

        Args:
            until: the string for filtering.  If specified as time=amount
                will subtract that amount from the current time
        """

    @abstractmethod
    def set_since(self, since: str) -> None:
        """Set the filter for time of jobs to consider.

        Args:
            since: the string for filtering.  If specified as time=amount
                will subtract that amount from the current time
        """

    @abstractmethod
    def has_since(self) -> bool:
        """Tests if `since` has been set.

        Returns:
            True if set_since has been called on this inquirer
        """

    @abstractmethod
    def get_partition_timelimits(self) -> dict[str, str]:
        """Get partition time limits.

        Returns:
            dict mapping partition names to maximum timelimits.
        """


class SacctInquirer(BaseInquirer):
    """Implementation of BaseInquirer for the sacct slurm function."""

    def __init__(self) -> None:
        """Initialize a new inquirer."""
        self.default_args = ["sacct", "--parsable", "-n", "--delimiter=^|^"]
        self.user: str | None = None
        self.state: set[str] | None = None
        self.not_state: set[str] | None = None
        self.since: str | None = None
        self.until: str | None = None
        self.query_all_users: bool = False
        self.partition: str | None = None
        self.cluster: str | None = None
        self.extra_args: str | None = None

    def get_valid_formats(self) -> list[str]:
        """Get the valid formatting options supported by the inquirer.

        Returns:
            List of valid format options

        Raises:
            RuntimeError: if sacct raises an error
        """
        command_args = ["sacct", "--helpformat"]
        cmd_result = subprocess.run(
            args=command_args,
            stdout=subprocess.PIPE,
            encoding="utf8",
            check=True,
            text=True,
            shell=False,
        )
        if cmd_result.returncode != 0:
            msg = "Error retrieving sacct options with --helpformat"
            raise RuntimeError(msg)
        return cmd_result.stdout.split()

    def set_sacct_args(self, jobs: list[str]) -> list[str]:
        """Set arguments of sacct query.

        Args:
            jobs: list of job names

        Returns:
            String of sacct arguments

        """
        args = []
        if self.user:
            if not self.since:
                # want to use the local, cluster time
                start_date = datetime.date.today() - datetime.timedelta(days=7)  # noqa: DTZ011
                self.since = start_date.strftime("%m%d%y")  # MMDDYY
            args += [f"--user={self.user}"]
        elif self.query_all_users:
            args += ["--allusers"]
        else:
            args += ["--jobs=" + ",".join(jobs)]

        if self.since:
            args += [f"--starttime={self.since}"]
        if self.partition:
            args += [f"--partition={self.partition}"]
        if self.cluster:
            args += [f"--cluster={self.cluster}"]
        if self.until:
            args += [f"--endtime={self.until}"]
        if self.extra_args:
            args += shlex.split(self.extra_args)
        return args

    def get_db_output(
        self,
        columns: list[str],
        jobs: list[str],
        debug_cmd: Callable[[str], Any] | None = None,
    ) -> list[dict[str, str]]:
        """Query the database with the supplied columns.

        Args:
            columns: validated format names as strings
            jobs: list of job names
            debug_cmd: If specified, the raw output will passed to this function

        Returns:
            List of rows, where each row is a dictionary
            with the columns as keys and entries as values
            Output order is not guaranteed to match the jobs list

        Raises:
            RuntimeError: if sacct doesn't return properly
        """
        args = [*self.default_args, "--format=" + ",".join(columns)]
        args += self.set_sacct_args(jobs)
        try:
            cmd_result = subprocess.run(
                args=args,
                stdout=subprocess.PIPE,
                encoding="utf8",
                check=True,
                text=True,
                shell=False,
            )
            cmd_result.check_returncode()

        except subprocess.CalledProcessError as error:
            msg = f"Error running sacct!\n{error.stderr}"
            raise RuntimeError(msg) from error

        sacct_line_split = re.compile(r"\^\|\^\n")
        # convert newlines to printable \n
        lines = [
            line.replace("\n", "\\n")
            for line in sacct_line_split.split(cmd_result.stdout)
        ]
        if debug_cmd is not None:
            debug_cmd("^|^\n".join(line.replace("\n", "\\n") for line in lines))

        sacct_split = re.compile(r"\^\|\^")
        result = [
            dict(zip(columns, sacct_split.split(line), strict=True))
            for line in lines
            if line
        ]

        # Sometimes the main job has a different state than the sub jobs
        # e.g. timeouts have a state of canceled for the batch jobs.
        # When state filtering is active, need to filter main ids, then retain
        # only the jobs with matching job ids
        if self.state or self.not_state:
            main_jobs = [r for r in result if "." not in r["JobID"]]
            if self.state:
                # split to get first word in entries like "CANCELLED BY X"
                main_jobs = [
                    r for r in main_jobs if r["State"].split()[0] in self.state
                ]

            if self.not_state:
                # split to get first word in entries like "CANCELLED BY X"
                main_jobs = [
                    r for r in main_jobs if r["State"].split()[0] not in self.not_state
                ]

            main_job_ids = {r["JobID"] for r in main_jobs}
            result = [r for r in result if r["JobID"].split(".")[0] in main_job_ids]

        return result

    def set_user(self, user: str) -> None:
        """Set the collection of jobs based on the provided user.

        Args:
            user: user name
        """
        self.user = user

    def set_partition(self, partition: str) -> None:
        """Set the collection of jobs based on the provided partition.

        Args:
            partition: partition name
        """
        self.partition = partition

    def set_cluster(self, cluster: str) -> None:
        """Set the specific cluster in multi-cluster environment.

        Args:
            cluster: cluster name
        """
        self.cluster = cluster

    def set_extra_args(self, extra_args: str) -> None:
        """Set extra arguments to be forwarded to sacct.

        Args:
            extra_args: list of arguments
        """
        self.extra_args = extra_args

    def all_users(self) -> None:
        """Query for all users if `since` is set."""
        self.query_all_users = True

    def set_state(self, state: str) -> None:
        """Set the state to include output jobs.

        Args:
            state: comma separated list of state names or codes
        """
        if not state:
            return

        self.state = get_states_as_set(state)

    def set_not_state(self, state: str) -> None:
        """Set the state to exclude from output jobs.

        Args:
            state: comma separated list of state names or codes

        """
        if not state:
            return

        self.not_state = get_states_as_set(state)

    def parse_date(self, d: str) -> str:
        """Parse and convert custom string date format.

        Args:
            d: the string of date.

        Returns:
            converted string of date
        """
        abbrev_to_key = {
            "w": "weeks",
            "W": "weeks",
            "d": "days",
            "D": "days",
            "h": "hours",
            "H": "hours",
            "m": "minutes",
            "M": "minutes",
        }
        valid_args = ["weeks", "days", "hours", "minutes"]
        date_args = {}

        args = d.split(",")
        for arg in args:
            if "=" not in arg:
                continue

            toks = arg.split("=")

            # convert key to name
            if toks[0] in abbrev_to_key:
                toks[0] = abbrev_to_key[toks[0]]

            toks[0] = toks[0].casefold()

            if toks[0] in valid_args:
                try:
                    date_args[toks[0]] = int(toks[1])
                except ValueError:
                    continue

        # want to use the local, cluster time
        date = datetime.datetime.today()  # noqa: DTZ002
        date -= datetime.timedelta(**date_args)
        return date.strftime("%Y-%m-%dT%H:%M")  # MMDDYY

    def set_until(self, until: str) -> None:
        """Set the filter for time of jobs to consider.

        Args:
            until: the string for filtering. If specified as time=amount
                will subtract that amount from the current time
        """
        if not until:
            return
        if "=" in until:  # handle custom format
            self.until = self.parse_date(until)
        else:
            self.until = until

    def set_since(self, since: str) -> None:
        """Set the filter for time of jobs to consider.

        Args:
            since: the string for filtering.  If specified as time=amount
                will subtract that amount from the current time
        """
        if not since:
            return
        if "=" in since:  # handle custom format
            self.since = self.parse_date(since)
        else:
            self.since = since

    def has_since(self) -> bool:
        """Check if since has been set.

        Returns:
            True if since has been set properly
        """
        return bool(self.since)

    def get_partition_timelimits(self) -> dict[str, str]:
        """Get partition time limits.

        Returns:
            dict mapping partition names to maximum timelimits.

        Raises:
            RuntimeError: if scontrol raises an error
        """
        args = ""
        if self.cluster:
            args = f"--cluster {self.cluster}"

        command_args = f"scontrol {args} show partition".split()
        cmd_result = subprocess.run(
            args=command_args,
            stdout=subprocess.PIPE,
            encoding="utf8",
            check=True,
            text=True,
            shell=False,
        )
        if cmd_result.returncode != 0:
            msg = "Error retrieving information from scontrol"
            raise RuntimeError(msg)

        partition_name = re.compile(r"^PartitionName=(?P<name>\S+)$")
        time_limit = re.compile(r"MaxTime=(?P<time>\S+)")

        partition = ""
        result = {}
        for line in cmd_result.stdout.split():
            match = re.match(partition_name, line)
            if match:
                partition = match.group("name")
            match = re.match(time_limit, line)
            if match:
                result[partition] = match.group("time")

        return result


def get_states_as_set(state_list: str) -> set[str]:
    """Helper method to parse the state string.

    Args:
        state_list: comma separated string with codes and states

    Returns:
        Set with valid state names in upper case
    """
    codes_to_states = {
        "BF": "BOOT_FAIL",
        "CA": "CANCELLED",
        "CD": "COMPLETED",
        "DL": "DEADLINE",
        "F": "FAILED",
        "NF": "NODE_FAIL",
        "OOM": "OUT_OF_MEMORY",
        "PD": "PENDING",
        "PR": "PREEMPTED",
        "R": "RUNNING",
        "RQ": "REQUEUED",
        "RS": "RESIZING",
        "RV": "REVOKED",
        "S": "SUSPENDED",
        "TO": "TIMEOUT",
    }
    possible_states = set(codes_to_states.values())
    states = {
        codes_to_states.get(state, state) for state in state_list.upper().split(",")
    }

    for state in states:
        if state not in possible_states:
            click.secho(f"Unknown state {state}", fg="yellow", err=True)

    result = states.intersection(possible_states)
    if result == set():
        click.secho("No valid states provided", fg="yellow", err=True)
        result.add("")

    return result
