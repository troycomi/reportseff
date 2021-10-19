from abc import ABC, abstractmethod
import datetime
import subprocess
from typing import Callable, Dict, List, Optional, Set

import click


class BaseInquirer(ABC):
    def __init__(self) -> None:
        """
        Initialize a new inquirer
        """

    @abstractmethod
    def get_valid_formats(self) -> List[str]:
        """
        Get the valid formatting options supported by the inquirer.
        Return as list of strings
        """

    @abstractmethod
    def get_db_output(
        self,
        columns: List[str],
        jobs: List[str],
        debug_cmd: Optional[Callable],
    ) -> List[Dict[str, str]]:
        """
        Query the databse with the supplied columns
        Format output to be a list of rows, where each row is a dictionary
        with the columns as keys and entries as values
        Output order is not garunteed to match the jobs list
        """

    @abstractmethod
    def set_user(self, user: str) -> None:
        """
        Set the collection of jobs based on the provided user
        """

    @abstractmethod
    def set_state(self, state: str) -> None:
        """
        Set the state to filter output jobs with
        """

    @abstractmethod
    def set_since(self, since: str) -> None:
        """
        Set the filter for time of jobs to consider
        """


class SacctInquirer(BaseInquirer):
    """
    Implementation of BaseInquirer for the sacct slurm function
    """

    def __init__(self) -> None:
        self.default_args = "sacct -P -n".split()
        self.user: Optional[str] = None
        self.state: Optional[Set] = None
        self.since: Optional[str] = None

    def get_valid_formats(self) -> List[str]:
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
            raise Exception("Error retrieving sacct options with --helpformat")
        result = cmd_result.stdout.split()
        return result

    def get_db_output(
        self,
        columns: List[str],
        jobs: List[str],
        debug_cmd: Optional[Callable] = None,
    ) -> List[Dict[str, str]]:
        """
        Assumes the columns have already been validated.
        if debug_cmd is set, passes the subprocess result as
        to the functions
        """
        args = self.default_args + ["--format=" + ",".join(columns)]

        if self.user:
            if not self.since:
                start_date = datetime.date.today() - datetime.timedelta(days=7)
                self.since = start_date.strftime("%m%d%y")  # MMDDYY
            args += [f"--user={self.user}", f"--starttime={self.since}"]
        else:
            args += ["--jobs=" + ",".join(jobs)]
            if self.since:
                args += [f"--starttime={self.since}"]

        cmd_result = subprocess.run(
            args=args,
            stdout=subprocess.PIPE,
            encoding="utf8",
            check=True,
            universal_newlines=True,
            shell=False,
        )

        if cmd_result.returncode != 0:
            raise Exception("Error running sacct!")

        lines = cmd_result.stdout.split("\n")
        result = [dict(zip(columns, line.split("|"))) for line in lines if line]

        if self.state:
            result = [r for r in result if r["State"] in self.state]

        if debug_cmd is not None:
            debug_cmd("\n".join(lines))

        return result

    def set_user(self, user: str) -> None:
        """
        Set the collection of jobs based on the provided user
        """
        self.user = user

    def set_state(self, state: str) -> None:
        """
        state is a comma separated string with codes and states
        Need to convert codes to states and set to upper
        Add states to list for searching later
        """
        if not state:
            return
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
        possible_states = codes_to_states.values()
        states = []
        for st in state.split(","):
            st = st.upper()
            if st in codes_to_states:
                st = codes_to_states[st]
            if st not in states:
                states.append(st)

        for st in states:
            if st not in possible_states:
                click.secho(f"Unknown state {st}", fg="yellow", err=True)

        self.state = {st for st in states if st in possible_states}
        # add a single value if it's empty here
        if not self.state:
            click.secho("No valid states provided", fg="yellow", err=True)
            self.state.add(None)

    def set_since(self, since: str) -> None:
        """
        since is either a comma separated string with codes ints or
        an sacct time.  The list will have '='
        Need to convert codes to datetimes
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
