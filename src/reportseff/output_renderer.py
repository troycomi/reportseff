"""Module for rendering tabulated values."""

from __future__ import annotations

import copy
import itertools
import locale
import re
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import click

from .array_summary import (
    MetricStat,
    build_array_summary,
    format_duration,
    group_jobs_by_array,
    group_jobs_by_name,
    is_array_group,
    parse_graph_format,
    render_histogram,
    render_sparkline,
)
from .job import Job, state_colors

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

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
MID_LIMIT_TOO_HIGH = 105
HIGH_LIMIT_LOW = 20
HIGH_LIMIT_GOOD = 80


def _supports_unicode() -> bool:
    """Return True if the active output encoding is UTF-based."""
    encoding = (
        getattr(sys.stdout, "encoding", None)
        or locale.getpreferredencoding(do_setlocale=False)
        or ""
    )
    return "utf" in encoding.lower()


#: Whether the terminal can render Unicode glyphs (used by `summarize`).
SUPPORTS_UNICODE = _supports_unicode()


def _summary_target_type(title: str) -> str | None:
    """Map a column title to its efficiency color type ("high"/"mid"/None)."""
    fold = title.casefold()
    if fold in ("cpueff", "gpueff", "gpu"):
        return "high"
    if fold in ("timeeff", "memeff", "gpumem"):
        return "mid"
    return None


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
        valid_titles: list[str],
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
        self.derived: dict[str, list[str]] = {
            "CPUEff": ["TotalCPU", "AllocCPUS", "Elapsed"],
            "MemEff": ["ReqMem", "NNodes", "AllocCPUS", "MaxRSS", "NTasks"],
            "TimeEff": ["Elapsed", "Timelimit"],
            "GPU": [],
            "GPUMem": [],
            "GPUEff": [],
            "Energy": ["TRESUsageOutAve"],
        }

        # build formatters
        self.formatters = build_formatters(format_str)

        # validate with titles and derived keys
        self.query_columns = self.validate_formatters(
            valid_titles, list(self.derived.keys())
        )

        # build columns for sacct call
        self.correct_columns()

    def validate_formatters(
        self,
        valid_titles: list[str],
        derived_titles: list[str],
    ) -> list[str]:
        """Validate titles of formatters attribute.

        Expands GPU to GPUEff and GPUMem in formatters

        Args:
            valid_titles: List of valid options for format tokens
            derived_titles: list of derived title strings, are valid unless
                requesting a total value

        Returns:
            Return list of validated strings to query
        """
        result = [
            fmt.validate_title(valid_titles, derived_titles) for fmt in self.formatters
        ]

        if self.options.node:
            if "JobID" not in self.formatters:
                self.formatters.insert(0, ColumnFormatter("JobID"))
            # ensure alignment is <, regardless of inputs
            self.formatters[self.formatters.index(cast("Any", "JobID"))].alignment = "<"

        if "GPU" in self.formatters:
            ind = self.formatters.index(cast("Any", "GPU"))
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
        result: list[list[str]] = [self.derived.get(c, [c]) for c in self.query_columns]
        # flatten
        flat_result = [item for sublist in result for item in sublist]

        # add in required values
        flat_result += self.required

        # remove duplicates
        self.query_columns = sorted(set(flat_result))

    def add_required_column(self, title: str) -> None:
        """Ensure an additional column is always fetched, even if not displayed.

        Used by ``summarize --group-by=name``, which needs ``JobName`` from
        sacct for grouping even though it isn't necessarily a displayed
        column. Idempotent, and does not affect `report`, which never calls
        it.

        Args:
            title: the sacct column title to always include in query_columns
        """
        if title not in self.required:
            self.required.append(title)
            self.correct_columns()

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

        result += "\n".join(self._format_single_job(job) for job in jobs)

        return result

    def _format_single_job(self, job: Job) -> str:
        """Render one job's row(s) as a string (multi-line in node mode)."""
        delimiter = self.options.delimiter
        if self.options.node:
            return "\n".join(
                # join each column entry by spaces
                delimiter.join(str(column) for column in columns).rstrip()
                # columns is a tuple of generators from format_node_job
                for columns in zip(
                    *(
                        fmt.format_node_job(job, gpu=self.options.gpu)
                        for fmt in self.formatters
                    ),
                    strict=True,
                )
            )
        return delimiter.join(fmt.format_job(job) for fmt in self.formatters).rstrip()

    def format_grouped_summary(
        self,
        jobs: list[Job],
        *,
        group_by: str,
        min_tasks: int,
        graph_style: str,
        graph_format: str,
        ascii_fallback: bool,
    ) -> str:
        """Render jobs grouped by array id or job name, each with a summary.

        This is `summarize`'s top-level rendering entry point: per-task rows
        (via `_format_single_job`, same as `report`), followed by a summary
        block for any group with more than one task. Which columns get
        summarized is driven implicitly by `self.formatters` -- the same
        columns `--format` would otherwise display -- not by a separate
        selector; `graph_format` only narrows which of those additionally
        get a sparkline/histogram.

        Args:
            jobs: the jobs to render, already sorted
            group_by: "array" (base job id) or "name" (sacct JobName)
            min_tasks: minimum tasks in a group before a graph is drawn
            graph_style: "sparkline", "histogram", or "none"
            graph_format: comma-separated, case-insensitive metrics to graph
            ascii_fallback: force ASCII glyphs, overriding auto-detection

        Returns:
            The rendered, colored output string. In `--parsable` mode, only
            the per-task rows are returned -- the decorative summary block
            (headers, bullets, colors) would corrupt a delimited consumer,
            so it's suppressed there, same as the original array-summary
            prototype did.
        """
        grouping = group_jobs_by_name if group_by == "name" else group_jobs_by_array
        label = "Group" if group_by == "name" else "Array"
        titles = [fmt.title for fmt in self.formatters]
        graphed = set(parse_graph_format(graph_format))
        use_unicode = SUPPORTS_UNICODE and not ascii_fallback

        lines: list[str] = []
        for base, tasks in grouping(jobs):
            lines.extend(self._format_single_job(job) for job in tasks)
            if is_array_group(tasks) and not self.options.parsable:
                lines.extend(
                    self._format_summary_block(
                        base,
                        tasks,
                        titles,
                        label=label,
                        min_tasks=min_tasks,
                        graph_style=graph_style,
                        graphed=graphed,
                        use_unicode=use_unicode,
                    )
                )
        return "\n".join(lines)

    def _format_summary_block(  # noqa: PLR0913
        self,
        base: str,
        tasks: list[Job],
        titles: list[str],
        *,
        label: str,
        min_tasks: int,
        graph_style: str,
        graphed: set[str],
        use_unicode: bool,
    ) -> list[str]:
        """Build the colored, human-readable summary lines for one group."""
        summary = build_array_summary(base, tasks, titles)
        indent = "  "
        bullet = "•" if use_unicode else "|"
        lines: list[str] = []

        # header: id/name, completion progress, state counters
        pct = (
            round(summary.completed_tasks / summary.total_tasks * 100)
            if summary.total_tasks
            else 0
        )
        header = click.style(f"{label} {base}", bold=True)
        header += (
            f"  {bullet}  {summary.completed_tasks}/{summary.total_tasks}"
            f" completed ({pct}%)"
        )
        counters = f"  {bullet}  ".join(
            click.style(f"{state} {count}", fg=state_colors.get(state))
            for state, count in sorted(summary.state_counts.items())
        )
        if counters:
            header += f"  {bullet}  {counters}"
        lines.append(header)

        # per-metric min/mean/max, as an aligned table
        lines.extend(self._format_metrics_table(summary.metrics, indent=indent))

        # total accumulated wall-clock runtime across all tasks
        if summary.total_task_seconds > 0:
            lines.append(
                f"{indent}Total task-time (wall-clock): "
                f"{format_duration(summary.total_task_seconds)}"
                f" across {summary.total_tasks} tasks"
            )

        # per-state mean runtime
        if summary.per_state_mean_seconds:
            parts = f"  {bullet}  ".join(
                f"{click.style(state, fg=state_colors.get(state))}"
                f" {format_duration(seconds)}"
                for state, seconds in sorted(summary.per_state_mean_seconds.items())
            )
            lines.append(f"{indent}Mean runtime: {parts}")

        # shared metadata (constant across all tasks)
        if summary.shared_values:
            shared = ", ".join(
                f"{title}={value}" for title, value in summary.shared_values
            )
            lines.append(f"{indent}Shared: {shared}")

        # heterogeneous metadata
        lines.extend(
            f"{indent}{title}: {', '.join(values)}"
            for title, values in summary.unique_values
        )

        # collapsed node hostlist
        if summary.node_hostlist:
            lines.append(f"{indent}Nodes: {summary.node_hostlist}")

        # distribution graphs (sparkline/histogram) for each requested
        # --graph-format metric, once the group is large enough. "runtime"
        # graphs summary.elapsed_minutes (always retained, independent of
        # --format); every other vocabulary entry graphs the matching
        # MetricStat's raw per-task values.
        if graph_style != "none" and summary.total_tasks > min_tasks:
            if "runtime" in graphed and summary.elapsed_minutes:
                lines.extend(
                    self._format_metric_graph(
                        "Runtime",
                        summary.elapsed_minutes,
                        unit="min",
                        graph_style=graph_style,
                        use_unicode=use_unicode,
                        indent=indent,
                    )
                )
            for metric in summary.metrics:
                if metric.title.casefold() not in graphed or not metric.values:
                    continue
                unit = (
                    "%"
                    if _summary_target_type(metric.title) in ("high", "mid")
                    else "J"
                    if metric.title.casefold() == "energy"
                    else ""
                )
                lines.extend(
                    self._format_metric_graph(
                        metric.title,
                        metric.values,
                        unit=unit,
                        graph_style=graph_style,
                        use_unicode=use_unicode,
                        indent=indent,
                    )
                )

        return lines

    def _format_metric_graph(  # noqa: PLR0913
        self,
        label: str,
        values: list[float],
        *,
        unit: str,
        graph_style: str,
        use_unicode: bool,
        indent: str,
    ) -> list[str]:
        """Render one metric's distribution as a sparkline or a histogram.

        Args:
            label: the metric name shown in the graph, e.g. "Runtime", "CPUEff"
            values: the raw per-task values to bin
            unit: unit suffix -- "min", "%", "J", or "" when none is known
            graph_style: "sparkline" or "histogram" ("none" is filtered by
                the caller before this is reached)
            use_unicode: whether to use Unicode block characters
            indent: leading whitespace applied to every line

        Returns:
            The rendered lines, or an empty list when there are no values.
        """
        if not values:
            return []

        if graph_style == "sparkline":
            spark = render_sparkline(values)
            low = round(min(values))
            high = round(max(values))
            suffix = unit if unit == "%" else (f" {unit}" if unit else "")
            return [
                f"{indent}{label} dist: {spark}"
                f" (n={len(values)}, {low}-{high}{suffix})"
            ]

        return [
            indent + line
            for line in render_histogram(
                values, ascii_only=not use_unicode, unit=unit, label=label
            )
        ]

    def _format_metrics_table(
        self, metrics: list[MetricStat], *, indent: str
    ) -> list[str]:
        """Render the per-metric min/mean/max stats as an aligned table.

        Reuses ColumnFormatter for width, alignment, and coloring instead of
        the original prototype's hand-rolled, bullet-separated line per
        metric. Out-of-range values are still flagged, but only via the
        same per-cell coloring the main report table already uses (each of
        min/mean/max is colored independently) -- there's no extra "low"
        marker glyph alongside it, since the color alone already
        communicates it without adding unicode/ASCII-dependent text.

        Args:
            metrics: the per-metric min/mean/max stats to render
            indent: leading whitespace applied to every line

        Returns:
            The table as a list of lines (header + one row per metric), or
            an empty list if there are no metrics to show.
        """
        if not metrics:
            return []

        def render_value(metric: MetricStat, value: float) -> tuple[str, str | None]:
            target = _summary_target_type(metric.title)
            if target in ("high", "mid"):
                return render_eff(round(value, 1), target)
            return f"{round(value, 1)}", None

        rows = [
            (
                metric.title,
                render_value(metric, metric.minimum),
                render_value(metric, metric.mean),
                render_value(metric, metric.maximum),
            )
            for metric in metrics
        ]

        metric_col = ColumnFormatter("Metric%<")
        min_col = ColumnFormatter("Min%>")
        mean_col = ColumnFormatter("Mean%>")
        max_col = ColumnFormatter("Max%>")
        metric_col.width = max(len(metric_col.title), *(len(r[0]) for r in rows)) + 2
        min_col.width = max(len(min_col.title), *(len(r[1][0]) for r in rows)) + 2
        mean_col.width = max(len(mean_col.title), *(len(r[2][0]) for r in rows)) + 2
        max_col.width = max(len(max_col.title), *(len(r[3][0]) for r in rows)) + 2

        header = (
            metric_col.format_title()
            + min_col.format_title()
            + mean_col.format_title()
            + max_col.format_title()
        )
        lines = [indent + header]
        for title, (min_t, min_c), (mean_t, mean_c), (max_t, max_c) in rows:
            lines.append(
                indent
                + metric_col.format_entry(title)
                + min_col.format_entry(min_t, min_c)
                + mean_col.format_entry(mean_t, mean_c)
                + max_col.format_entry(max_t, max_c)
            )
        return lines


class ColumnFormatter:
    """A single column formatting object."""

    __hash__: None = None  # type: ignore[assignment]

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

    def __eq__(self, other: object) -> bool:
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

    def validate_title(self, valid_titles: list[str], derived_titles: list[str]) -> str:
        """Validate the title against a list.

        Tries to find this formatter's title in the valid titles list in a case
        insensitive manner.  If found, replace with valid_title to correct
        capitalization to match valid_titles entry.

        Args:
            valid_titles: list of valid title strings
            derived_titles: list of derived title strings, are valid unless
                requesting a total value

        Returns:
            The self title validated from the valid_titles list

        Raises:
            ValueError: if self.title is not found in the valid list
        """
        fold_title = self.title.casefold()
        for title in itertools.chain(valid_titles, derived_titles):
            if fold_title == title.casefold():
                self.title = title
                return title

        if fold_title.startswith("total"):
            fold_title = fold_title.removeprefix("total")
            for title in valid_titles:
                if fold_title == title.casefold():
                    self.title = f"Total{title}"
                    return title

            # not valid, could be derived and need to inform user
            msg = f"{self.title!r} is not a valid title. "
            for title in derived_titles:
                if fold_title == title.casefold():
                    msg += f"{title!r} is a derived value and cannot be summed. "
                    break
            else:  # not derived
                msg += f"{fold_title!r} does not match allowed values. "
            msg += "Run `sacct --helpformat` for a list of allowed values."

        else:
            msg = (
                f"{self.title!r} is not a valid title. "
                "Run `sacct --helpformat` for a list of allowed values."
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
    if value > MID_LIMIT_TOO_HIGH:
        return "magenta"
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


def build_formatters(format_str: str) -> list[ColumnFormatter]:
    """Generate list of formatters from comma separated list in format string.

    Args:
        format_str: comma separated list of format tokens

    Returns:
        Return list of ColumnFormatters
    """
    return [ColumnFormatter(fmt) for fmt in format_str.split(",") if fmt != ""]
