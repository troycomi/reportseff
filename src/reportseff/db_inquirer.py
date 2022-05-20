"""Abstract and concrete implementations of scheduler databases."""
from abc import ABC, abstractmethod
import datetime
import subprocess
from typing import Callable, Dict, List, Optional, Set

import click


class BaseInquirer(ABC):
    """Abstract interface for inquiring different schedulers."""

    def __init__(self) -> None:
        """Initialize a new inquirer."""

    @abstractmethod
    def get_valid_formats(self) -> List[str]:
        """Get the valid formatting options supported by the inquirer.

        Returns:
            List of valid format options
        """

    @abstractmethod
    def get_db_output(
        self,
        columns: List[str],
        jobs: List[str],
        debug_cmd: Optional[Callable],
    ) -> List[Dict[str, str]]:
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


class SacctInquirer(BaseInquirer):
    """Implementation of BaseInquirer for the sacct slurm function."""

    def __init__(self) -> None:
        """Initialize a new inquirer."""
        self.default_args = "sacct -P -n".split()
        self.user: Optional[str] = None
        self.state: Optional[Set] = None
        self.not_state: Optional[Set] = None
        self.since: Optional[str] = None
        self.query_all_users: bool = False

    def get_valid_formats(self) -> List[str]:
        """Get the valid formatting options supported by the inquirer.

        Returns:
            List of valid format options

        Raises:
            RuntimeError: if sacct raises an error
        """
        command_args = "sacct --helpformat".split()
        cmd_result = subprocess.run(
            args=command_args,
            stdout=subprocess.PIPE,
            encoding="utf8",
            check=True,
            universal_newlines=True,
            shell=False,
        )
        if cmd_result.returncode != 0:
            raise RuntimeError("Error retrieving sacct options with --helpformat")
        result = cmd_result.stdout.split()
        return result

    def get_db_output(
        self,
        columns: List[str],
        jobs: List[str],
        debug_cmd: Optional[Callable] = None,
    ) -> List[Dict[str, str]]:
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
        args = self.default_args + ["--format=" + ",".join(columns)]

        if self.user:
            if not self.since:
                start_date = datetime.date.today() - datetime.timedelta(days=7)
                self.since = start_date.strftime("%m%d%y")  # MMDDYY
            args += [f"--user={self.user}", f"--starttime={self.since}"]
        elif self.query_all_users:
            args += ["--allusers", f"--starttime={self.since}"]
        else:
            args += ["--jobs=" + ",".join(jobs)]
            if self.since:
                args += [f"--starttime={self.since}"]

        try:
            cmd_result = subprocess.run(
                args=args,
                stdout=subprocess.PIPE,
                encoding="utf8",
                check=True,
                universal_newlines=True,
                shell=False,
            )
            cmd_result.check_returncode()

        except subprocess.CalledProcessError as error:
            raise RuntimeError(f"Error running sacct!\n{error.stderr}") from error

        lines = cmd_result.stdout.split("\n")
        if debug_cmd is not None:
            debug_cmd("\n".join(lines))

        result = [dict(zip(columns, line.split("|"))) for line in lines if line]

        if self.state:
            # split to get first word in entries like "CANCELLED BY X"
            result = [r for r in result if r["State"].split()[0] in self.state]

        if self.not_state:
            # split to get first word in entries like "CANCELLED BY X"
            result = [r for r in result if r["State"].split()[0] not in self.not_state]

        return result

    def set_user(self, user: str) -> None:
        """Set the collection of jobs based on the provided user.

        Args:
            user: user name
        """
        self.user = user

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
        # add a single value if it's empty here
        if not self.state:
            click.secho("No valid states provided to include", fg="yellow", err=True)
            self.state.add(None)

    def set_not_state(self, state: str) -> None:
        """Set the state to exclude from output jobs.

        Args:
            state: comma separated list of state names or codes
        """
        if not state:
            return

        self.not_state = get_states_as_set(state)
        # add a single value if it's empty here
        if not self.not_state:
            click.secho("No valid states provided to exclude", fg="yellow", err=True)
            self.not_state = None

    def set_since(self, since: str) -> None:
        """Set the filter for time of jobs to consider.

        Args:
            since: the string for filtering.  If specified as time=amount
                will subtract that amount from the current time
        """
        if not since:
            return
        if "=" in since:  # handle custom format
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

            args = since.split(",")
            for arg in args:
                toks = arg.split("=")

                # lines don't have an equal
                if len(toks) < 2:
                    continue

                # convert key to name
                if toks[0] in abbrev_to_key:
                    toks[0] = abbrev_to_key[toks[0]]

                toks[0] = toks[0].lower()

                if toks[0] in valid_args:
                    try:
                        date_args[toks[0]] = int(toks[1])
                    except ValueError:
                        continue

            start_date = datetime.datetime.today()
            start_date -= datetime.timedelta(**date_args)
            self.since = start_date.strftime("%Y-%m-%dT%H:%M")  # MMDDYY

        else:
            self.since = since

    def has_since(self) -> bool:
        """Check if since has been set.

        Returns:
            True if since has been set properly
        """
        return bool(self.since)


def get_states_as_set(state_list: str) -> Set:
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

    return states.intersection(possible_states)
