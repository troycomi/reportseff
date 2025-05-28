"""Module for rendering tabulated values."""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from typing import Any, Callable, Generator

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

# efficiency limits for mid and high measurements
MID_LIMIT_LOW = 20
MID_LIMIT_HIGH = 90
MID_LIMIT_GOOD = 60
HIGH_LIMIT_LOW = 20
HIGH_LIMIT_GOOD = 80


@dataclass
class RenderOptions:
    """Data class for holding output rendering options.

    Args:
        node: bool indicating if node-level data should be reported
        gpu: in addition to node, should each GPU be reported
        parsable: should output be rendered in a parsable format
        delimiter: string to use for separating columns when parsable is set
    """

    node: bool = False
    gpu: bool = False
    parsable: bool = False
    delimiter: str = "|"

    def __post_init__(self) -> None:
        """Post init method to handle delimiter and parsable."""
        if not self.parsable:
            self.delimiter = " "


class OutputRenderer:
    """A collection of formatting columns for rendering output."""

    def __init__(
        self,
        valid_titles: list,
        options: RenderOptions,
        format_str: str = "JobID%>,State,Elapsed%>,CPUEff,MemEff",
    ) -> None:
        """Initialize renderer with format string and list of valid titles.

        Args:
            valid_titles: List of valid options for format tokens
            format_str: comma separated list of format tokens
            options: RenderOptions controlling display behavior
        """
        self.options = options
        # values required for proper parsing, need not be included in output
        self.required = ["JobID", "JobIDRaw", "State", "AdminComment"]
        # values derived from other values, list includes all dependent values
        self.derived: dict[str, list] = {
            "CPUEff": ["TotalCPU", "AllocCPUS", "Elapsed"],
            "MemEff": ["REQMEM", "NNodes", "AllocCPUS", "MaxRSS", "NTasks"],
            "TimeEff": ["Elapsed", "Timelimit"],
            "GPU": [],
            "GPUMem": [],
            "GPUEff": [],
            "Energy": ["TRESUsageOutAve"],
        }

        # build formatters
        self.formatters = build_formatters(format_str)

        # validate with titles and derived keys
        valid_titles += self.derived.keys()
        self.query_columns = self.validate_formatters(valid_titles)

        # build columns for sacct call
        self.correct_columns()

    def validate_formatters(self, valid_titles: list) -> list:
        """Validate titles of formatters attribute.

        Expands GPU to GPUEff and GPUMem in formatters

        Args:
            valid_titles: List of valid options for format tokens

        Returns:
            Return list of validated strings to query
        """
        result = [fmt.validate_title(valid_titles) for fmt in self.formatters]

        if self.options.node:
            if "JobID" not in self.formatters:
                self.formatters.insert(0, ColumnFormatter("JobID"))
            # ensure alignment is <, regardless of inputs
            self.formatters[self.formatters.index("JobID")].alignment = "<"

        if "GPU" in self.formatters:
            ind = self.formatters.index("GPU")
            self.formatters[ind].title = "GPUEff"
            gpu_mem = copy.copy(self.formatters[ind])
            gpu_mem.title = "GPUMem"
            gpu_mem.color_function = lambda x: render_eff(x, "mid")
            self.formatters.insert(ind + 1, gpu_mem)

        if (
            self.options.gpu
            and "GPUEff" not in self.formatters
            and "GPUMem" not in self.formatters
        ):
            # need to assign color functions as that normally happens before this
            formatter = ColumnFormatter("GPUEff")
            self.formatters.append(formatter)
            formatter = ColumnFormatter("GPUMem")
            self.formatters.append(formatter)

        return result

    def correct_columns(self) -> None:
        """Expand derived values of query columns and remove duplicates."""
        result: list[list] = [self.derived.get(c, [c]) for c in self.query_columns]
        # flatten
        flat_result = [item for sublist in result for item in sublist]

        # add in required values
        flat_result += self.required

        # remove duplicates
        self.query_columns = sorted(set(flat_result))

    def format_jobs(self, jobs: list[Job]) -> str:
        """Given list of jobs, build output table.

        Args:
            jobs: List of job objects

        Returns:
            Formatted table as single string
        """
        result = ""
        delimiter = self.options.delimiter

        if len(self.formatters) == 0:
            return result

        if len(self.formatters) == 1:
            # if only one formatter is present, override the alignment
            self.formatters[0].no_formatting()
            # skip adding the title to result

        else:
            for fmt in self.formatters:
                if self.options.parsable:
                    fmt.no_formatting()
                else:
                    fmt.compute_width(
                        jobs, node=self.options.node, gpu=self.options.gpu
                    )

            result += delimiter.join(
                fmt.format_title(bold=not self.options.parsable)
                for fmt in self.formatters
            )
            if len(jobs) != 0:
                result += "\n"

        if self.options.node:
            # join each row by newlines
            result += "\n".join(
                # join each column entry by spaces
                delimiter.join(str(column) for column in columns).rstrip()
                # for each job
                for job in jobs
                # columns is a tuple of generators from format_node_job
                for columns in zip(
                    *(
                        fmt.format_node_job(job, gpu=self.options.gpu)
                        for fmt in self.formatters
                    ),
                )
            )

        else:
            result += "\n".join(
                delimiter.join(fmt.format_job(job) for fmt in self.formatters).rstrip()
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
            err = f"Unable to parse format token {token!r}"
            if "%" in token:
                err += ", did you forget to wrap in quotes?"
            raise ValueError(err)

        self.title = match.group("title")

        self.alignment = match.group("alignment")
        if not self.alignment:
            self.alignment = "^"

        self.width = None
        if match.group("width"):
            self.width = int(match.group("width"))  # none will be calculated later

        self.end = match.group("end")

        self.color_function: Callable[[str], tuple[str, Any]] = lambda x: (str(x), None)
        fold_title = self.title.casefold()
        if fold_title == "state":
            self.color_function = color_state
        elif fold_title in ("cpueff", "gpueff", "gpu"):
            self.color_function = lambda x: render_eff(x, "high")
        elif fold_title in ("timeeff", "memeff", "gpumem"):
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

    def validate_title(self, valid_titles: list[str]) -> str:
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

        msg = (
            f"{self.title!r} is not a valid title. "
            "Run sacct --helpformat for a list of allowed values."
        )
        raise ValueError(msg)

    def compute_width(
        self,
        jobs: list[Job],
        *,
        node: bool = False,
        gpu: bool = False,
    ) -> None:
        """Set width for this column based on job listing.

        Determine the max width of all entries if the width attribute is unset.
        Includes title in determination

        Args:
            jobs: List of job objects to consider
            node: If True, report individual node stats
            gpu: If True, report individual gpu stats
        """
        if self.width is not None:
            return

        self.width = len(self.title)
        if len(jobs) > 0:
            if node:
                width = max(
                    len(str(entry))
                    for job in jobs
                    for entry in job.get_node_entries(self.title, gpu=gpu)
                )
            else:
                width = max(len(str(job.get_entry(self.title))) for job in jobs)
            self.width = max(self.width, width)

        self.width += 2  # add some boarder

    def no_formatting(self) -> None:
        """Set the formatter to just display the entries."""
        self.alignment = "<"
        self.width = None
        self.color_function = lambda x: (str(x), None)

    def format_title(self, *, bold: bool = True) -> str:
        """Format title of column for printing.

        Args:
            bold: if true, the resulting string will be styled bold

        Returns:
            the formatted title
        """
        result = self.format_entry(self.title)
        return click.style(result, bold=bold)

    def format_job(self, job: Job) -> str:
        """Format the provided job for printing.

        Args:
            job: the Job to format

        Returns:
            the formatted job entry
        """
        value = job.get_entry(self.title)
        return self.format_entry(*self.color_function(value))

    def format_node_job(
        self,
        job: Job,
        *,
        gpu: bool = False,
    ) -> Generator[str, None, None]:
        """Format the provided job for printing with individual node stats.

        Args:
            job: the Job to format
            gpu: If True, report individual gpu stats

        Returns:
            generator with values for the job
        """
        return (
            self.format_entry(*self.color_function(value))
            for value in job.get_node_entries(self.title, gpu=gpu)
        )

    def format_entry(self, entry: str, color: str | None = None) -> str:
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
            entry = entry[-self.width :] if self.end else entry[: self.width]
            result = (f"{{:{self.alignment}{self.width}}}").format(entry)

        if color:
            result = click.style(result, fg=color)
        return result


def color_state(value: str) -> tuple[str, str | None]:
    """Get the color name of the provided state string.

    Args:
        value: the state name of the job

    Returns:
        The state value and it's color (if found) or None
    """
    return value, state_colors.get(value, None)


def render_eff(value: str | float, target_type: str) -> tuple[str, str | None]:
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


def color_mid(value: float) -> str | None:
    """Determine color for efficiency value where "mid" values are the target.

    Args:
        value: percent (0-100) of value to color

    Returns:
        The color string for click or None if color should be unchanged
    """
    if value < MID_LIMIT_LOW or value > MID_LIMIT_HIGH:
        return "red"
    if value > MID_LIMIT_GOOD:
        return "green"
    return None


def color_high(value: float) -> str | None:
    """Determine color for efficiency value where "high" values are the target.

    Args:
        value: percent (0-100) of value to color

    Returns:
        The color string for click or None if color should be unchanged
    """
    if value < HIGH_LIMIT_LOW:
        return "red"
    if value > HIGH_LIMIT_GOOD:
        return "green"
    return None


def build_formatters(format_str: str) -> list:
    """Generate list of formatters from comma separated list in format string.

    Args:
        format_str: comma separated list of format tokens

    Returns:
        Return list of ColumnFormatters
    """
    return [ColumnFormatter(fmt) for fmt in format_str.split(",") if fmt != ""]
