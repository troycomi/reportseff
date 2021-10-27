"""Module for rendering tabulated values."""
import re
from typing import Any, Callable, List, Optional, Tuple, Union

import click

from .job import Job, state_colors


#: Regex for format tokens, NAME[%[ALIGNMENT][WIDTH[e?]]]
FORMAT_RE = re.compile(
    r"(?P<title>[^%:]+)"  # must have title
    r"([%:]"  # may have other formatting
    r"(?P<alignment>[<^>])?"  # like alignment
    r"((?P<width>\d+)(?P<end>[e$])?)?"  # if width is present, may have an e
    r")?"
)


class OutputRenderer:
    """A collection of formatting columns for rendering output."""

    def __init__(
        self,
        valid_titles: List,
        format_str: str = "JobID%>,State,Elapsed%>,CPUEff,MemEff",
    ) -> None:
        """Initialize renderer with format string and list of valid titles.

        Args:
            valid_titles: List of valid options for format tokens
            format_str: comma separated list of format tokens
        """
        # values required for proper parsing, need not be included in output
        self.required = ["JobID", "JobIDRaw", "State"]
        # values derived from other values, list includes all dependent values
        self.derived = {
            "CPUEff": ["TotalCPU", "AllocCPUS", "Elapsed"],
            "MemEff": ["REQMEM", "NNodes", "AllocCPUS", "MaxRSS"],
            "TimeEff": ["Elapsed", "Timelimit"],
        }

        # build formatters
        self.formatters = build_formatters(format_str)

        # validate with titles and derived keys
        valid_titles += self.derived.keys()
        self.query_columns = self.validate_formatters(valid_titles)

        # build columns for sacct call
        self.correct_columns()

    def validate_formatters(self, valid_titles: List) -> List:
        """Validate titles of formatters attribute.

        Args:
            valid_titles: List of valid options for format tokens

        Returns:
            Return list of validated ColumnFormatters
        """
        return [fmt.validate_title(valid_titles) for fmt in self.formatters]

    def correct_columns(self) -> None:
        """Expand derived values of query columns and remove duplicates."""
        result = [
            self.derived[c] if c in self.derived else [c] for c in self.query_columns
        ]
        # flatten
        flat_result = [item for sublist in result for item in sublist]

        # add in required values
        flat_result += self.required

        # remove duplicates
        self.query_columns = list(sorted(set(flat_result)))

    def format_jobs(self, jobs: List[Job]) -> str:
        """Given list of jobs, build output table.

        Args:
            jobs: List of job objects

        Returns:
            Formatted table as single string
        """
        result = ""
        if len(self.formatters) == 0:
            return result

        if len(self.formatters) == 1:
            # if only one formatter is present, override the alignment
            self.formatters[0].alignment = "<"
            # skip adding the title

        else:
            for fmt in self.formatters:
                fmt.compute_width(jobs)

            result += " ".join([fmt.format_title() for fmt in self.formatters])
            if len(jobs) != 0:
                result += "\n"

        result += "\n".join(
            " ".join([fmt.format_job(job) for fmt in self.formatters]).rstrip()
            for job in jobs
        )

        return result


class ColumnFormatter:
    """A single column formatting object."""

    def __init__(self, token: str) -> None:
        """Build column entry.

        Args:
            token: format string of the form NAME[%[ALIGNMENT][WIDTH[e?]]]

        Raises:
            ValueError: if unable to parse the format token
        """
        match = re.fullmatch(FORMAT_RE, token)
        if not match or (
            ("%" in token or ":" in token)
            and not match.group("alignment")
            and not match.group("width")
        ):
            err = f"Unable to parse format token '{token}'"
            if "%" in token:
                err += ", did you forget to wrap in quotes?"
            raise ValueError(err)

        self.title = match.group("title")

        self.alignment = match.group("alignment")
        if not self.alignment:
            self.alignment = "^"

        self.width = None
        if match.group("width"):
            self.width = int(match.group("width"))  # none will be calcualted later

        self.end = match.group("end")

        self.color_function: Callable[[str], Tuple[str, Any]] = lambda x: (x, None)
        fold_title = self.title.casefold()
        if fold_title == "state":
            self.color_function = color_state
        elif fold_title == "cpueff":
            self.color_function = lambda x: render_eff(x, "high")
        elif fold_title in ("timeeff", "memeff"):
            self.color_function = lambda x: render_eff(x, "mid")

    def __eq__(self, other: Any) -> bool:
        """Test for equality.

        Args:
            other: other object

        Returns:
            True if the other object is a column formatter with matching attributes
            or if this title match the other object as a string

        """
        if isinstance(other, ColumnFormatter):
            for k in self.__dict__:
                if k != "color_function" and self.__dict__[k] != other.__dict__[k]:
                    return False
            return True
        if isinstance(other, str):
            return self.title == other
        return False

    def __repr__(self) -> str:
        """Recreate format string of formatter.

        Returns:
            String representation of column formatter
        """
        return f"{self.title}%{self.alignment}{self.width}"

    def validate_title(self, valid_titles: List[str]) -> str:
        """Validate the title against a list.

        Tries to find this formatter's title in the valid titles list in a case
        insensitive manner.  If found, replace with valid_title to correct
        capitalization to match valid_titles entry.

        Args:
            valid_titles: list of valid title strings

        Returns:
            The self title validated from the valid_titles list

        Raises:
            ValueError: if self.title is not found in the valid list
        """
        fold_title = self.title.casefold()
        for title in valid_titles:
            if fold_title == title.casefold():
                self.title = title
                return title

        raise ValueError(
            f"'{self.title}' is not a valid title. "
            "Run sacct --helpformat for a list of allowed values."
        )

    def compute_width(self, jobs: List[Job]) -> None:
        """Set width for this column based on job listing.

        Determine the max width of all entries if the width attribute is unset.
        Includes title in determination

        Args:
            jobs: List of job objects to consider
        """
        if self.width is not None:
            return

        self.width = len(self.title)
        for job in jobs:
            entry = job.get_entry(self.title)
            if isinstance(entry, str):
                width = len(entry)
                self.width = self.width if self.width > width else width

        self.width += 2  # add some boarder

    def format_title(self) -> str:
        """Format title of column for printing.

        Returns:
            the formatted title
        """
        result = self.format_entry(self.title)
        return click.style(result, bold=True)

    def format_job(self, job: Job) -> str:
        """Format the provided job for printing.

        Args:
            job: the Job to format

        Returns:
            the formatted job entry
        """
        value = job.get_entry(self.title)
        return self.format_entry(*self.color_function(value))

    def format_entry(self, entry: str, color: Optional[str] = None) -> str:
        """Format the entry to match width, alignment, and color.

        If no color is supplied, will just return string
        If supplied, use click.style to change fg
        If the entry is longer than self.width, truncate the end

        Args:
            entry: the string to format
            color: set foreground of style string

        Returns:
            entry colored, aligned, and possibly truncated
        """
        if self.width is None:
            result = entry
        else:
            if self.end:
                entry = entry[-self.width :]
            else:
                entry = entry[: self.width]
            result = (f"{{:{self.alignment}{self.width}}}").format(entry)

        if color:
            result = click.style(result, fg=color)
        return result


def color_state(value: str) -> Tuple[str, Optional[str]]:
    """Get the color name of the provided state string.

    Args:
        value: the state name of the job

    Returns:
        The state value and it's color (if found) or None
    """
    return value, state_colors.get(value, None)


def render_eff(value: Union[str, float], target_type: str) -> Tuple[str, Optional[str]]:
    """Return a styled string for efficiency values.

    Args:
        value: the number or string to render
        target_type: "mid" or "high", the type of color map to use

    Returns:
        A tuple with:
        the value formatted with a percent sybol
        the color or None if it should remain the default
    """
    color_maps = {"mid": color_mid, "high": color_high}
    if isinstance(value, str):  # a "---"
        color = None
    else:
        color = color_maps[target_type](value)
        value = f"{value}%"
    return value, color


def color_mid(value: float) -> Optional[str]:
    """Determine color for efficiency value where "mid" values are the target.

    Args:
        value: percent (0-100) of value to color

    Returns:
        The color string for click or None if color should be unchanged
    """
    if value < 20 or value > 90:  # too close to limit
        return "red"
    if value > 60:  # good
        return "green"
    return None


def color_high(value: float) -> Optional[str]:
    """Determine color for efficiency value where "high" values are the target.

    Args:
        value: percent (0-100) of value to color

    Returns:
        The color string for click or None if color should be unchanged
    """
    if value < 20:  # too low
        return "red"
    if value > 80:  # good
        return "green"
    return None


def build_formatters(format_str: str) -> List:
    """Generate list of formatters from comma separated list in format string.

    Args:
        format_str: comma separated list of format tokens

    Returns:
        Return list of ColumnFormatters
    """
    return [ColumnFormatter(fmt) for fmt in format_str.split(",") if fmt != ""]
