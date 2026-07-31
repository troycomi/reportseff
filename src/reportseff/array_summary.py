"""Aggregation and rendering helpers for per-array job summaries.

This module is intentionally free of any ``click`` styling and does **not**
import :mod:`reportseff.output_renderer`, so that the renderer can import from
here without creating a circular dependency.  It returns structured numeric
data and plain (uncolored) strings; the renderer is responsible for applying
colors and final layout.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .job import _parse_slurm_timedelta

if TYPE_CHECKING:
    from collections.abc import Callable

    from .job import Job

#: Titles that are handled specially and never treated as generic metrics.
_EXCLUDE_TITLES = {"jobid", "jobidraw", "state", "elapsed"}
#: The title (case-insensitive) whose values are collapsed into a hostlist.
_NODELIST_TITLE = "nodelist"
#: State considered "completed" for completed-only efficiency statistics.
COMPLETED_STATE = "COMPLETED"
#: Minimum number of tasks for a group to be treated as an array.
_MIN_ARRAY_TASKS = 2

#: Recognized ``--graph-format`` metric names (case-insensitive).
#: `runtime` is the elapsed-time distribution; the rest are real, already
#: -implemented column titles. Note: only `runtime` currently has a raw
#: per-task series retained for graphing -- graphing the others requires the
#: generalization tracked separately (see the implementation plan, Phase 3).
GRAPH_FORMAT_VOCABULARY = ("runtime", "cpueff", "memeff", "energy", "gpueff", "gpumem")


def parse_graph_format(value: str) -> tuple[str, ...]:
    """Parse and validate a comma-separated ``--graph-format`` value.

    Args:
        value: the raw, comma-separated, case-insensitive metric list

    Returns:
        A de-duplicated, order-preserving tuple of lowercased metric names.

    Raises:
        ValueError: if any token isn't a recognized metric name.
    """
    tokens = [tok.strip().casefold() for tok in value.split(",") if tok.strip()]
    unknown = sorted({tok for tok in tokens if tok not in GRAPH_FORMAT_VOCABULARY})
    if unknown:
        msg = (
            f"Unknown --graph-format metric(s): {', '.join(unknown)}. "
            f"Recognized: {', '.join(GRAPH_FORMAT_VOCABULARY)}."
        )
        raise ValueError(msg)

    seen: set[str] = set()
    result: list[str] = []
    for token in tokens:
        if token not in seen:
            seen.add(token)
            result.append(token)
    return tuple(result)

#: Horizontal block characters at 1/8 resolution, index 0 (empty) .. 8 (full).
_EIGHTHS_H = " ▏▎▍▌▋▊▉█"
#: Vertical block characters for sparklines, 8 levels low .. high.
_EIGHTHS_V = "▁▂▃▄▅▆▇█"

#: Regex for a node name of the form ``<prefix><zero-padded-number>``.
_NODE_RE = re.compile(r"^(?P<prefix>.*?)(?P<num>\d+)$")
#: Regex for a bracketed hostlist token, e.g. ``somacpu[001-004,007]``.
_HOSTLIST_RE = re.compile(r"^(?P<prefix>[^\[]+)\[(?P<ranges>[0-9,\-]+)\]$")


@dataclass
class MetricStat:
    """Min / mean / max for a single numeric column over completed tasks."""

    title: str
    minimum: float
    mean: float
    maximum: float
    #: Raw per-completed-task values, in task order. Retained (not just the
    #: aggregate) so --graph-format can draw a distribution for any
    #: summarized metric, not only the runtime special case.
    values: list[float] = field(default_factory=list)


@dataclass
class ArraySummary:
    """Structured summary statistics for a single job array."""

    base_id: str
    total_tasks: int
    completed_tasks: int
    state_counts: dict[str, int] = field(default_factory=dict)
    metrics: list[MetricStat] = field(default_factory=list)
    total_task_seconds: int = 0
    per_state_mean_seconds: dict[str, float] = field(default_factory=dict)
    shared_values: list[tuple[str, str]] = field(default_factory=list)
    unique_values: list[tuple[str, list[str]]] = field(default_factory=list)
    node_hostlist: str | None = None
    elapsed_minutes: list[float] = field(default_factory=list)


def _group_jobs_by_key(
    jobs: list[Job],
    key_fn: Callable[[Job], str],
) -> list[tuple[str, list[Job]]]:
    """Group jobs by a caller-supplied key, preserving first-appearance order.

    Works regardless of sort order (jobid, mtime, filename): tasks sharing a
    key are collected together at the position of the first task seen.

    Args:
        jobs: the ordered list of jobs to group
        key_fn: function computing the grouping key for a single job

    Returns:
        A list of ``(key, [tasks])`` tuples in first-appearance order.
    """
    order: list[str] = []
    groups: dict[str, list[Job]] = {}
    for job in jobs:
        key = key_fn(job)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(job)
    return [(key, groups[key]) for key in order]


def group_jobs_by_array(jobs: list[Job]) -> list[tuple[str, list[Job]]]:
    """Group jobs by their base id (``--group-by=array``, the default).

    Args:
        jobs: the ordered list of jobs to group

    Returns:
        A list of ``(base_id, [tasks])`` tuples in first-appearance order.
    """
    return _group_jobs_by_key(jobs, lambda job: job.job)


def group_jobs_by_name(jobs: list[Job]) -> list[tuple[str, list[Job]]]:
    """Group jobs by their sacct ``JobName`` (``--group-by=name``).

    Falls back to the job's base id for tasks where ``JobName`` wasn't
    queried/available (rendered as the placeholder ``"---"``), so grouping
    degrades to per-job buckets rather than silently merging unrelated jobs
    under one empty key.

    Args:
        jobs: the ordered list of jobs to group

    Returns:
        A list of ``(name, [tasks])`` tuples in first-appearance order.
    """

    def key_fn(job: Job) -> str:
        raw = job.get_entry("JobName")
        if not isinstance(raw, str) or raw.strip() in ("", "---"):
            return job.job
        return raw.strip()

    return _group_jobs_by_key(jobs, key_fn)


def is_array_group(tasks: list[Job]) -> bool:
    """Return True if the group is large enough to warrant a summary block.

    Used for both ``--group-by=array`` and ``--group-by=name``: a group
    qualifies either by having more than one task, or -- for the array case
    specifically -- by a single task's jobid already showing array syntax
    (e.g. a lone ``123_1``).
    """
    return len(tasks) >= _MIN_ARRAY_TASKS or any("_" in job.jobid for job in tasks)


def _coerce_float(value: object) -> float | None:
    """Coerce a rendered cell value to float, stripping a trailing percent.

    Args:
        value: the value from :meth:`Job.get_entry`

    Returns:
        The float value, or None if it cannot be interpreted numerically or is
        a placeholder such as ``"---"``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        token = value.strip().rstrip("%")
        if token in ("", "---"):
            return None
        try:
            return float(token)
        except ValueError:
            return None
    return None


def _elapsed_seconds(job: Job) -> int | None:
    """Return the elapsed wall time of a job in seconds, if parseable."""
    raw = job.get_entry("Elapsed")
    if not isinstance(raw, str) or raw in ("", "---"):
        return None
    try:
        return _parse_slurm_timedelta(raw)
    except ValueError:
        return None


def build_array_summary(
    base_id: str,
    tasks: list[Job],
    titles: list[str],
) -> ArraySummary:
    """Compute summary statistics for a single job array.

    Args:
        base_id: the base job id shared by all tasks
        tasks: the array tasks
        titles: the column titles currently being displayed, in order

    Returns:
        An :class:`ArraySummary` with numeric statistics and compacted metadata.
        Efficiency-style metrics are computed over completed tasks only.
    """
    summary = ArraySummary(
        base_id=base_id,
        total_tasks=len(tasks),
        completed_tasks=sum(1 for job in tasks if job.state == COMPLETED_STATE),
    )

    # state counters
    for job in tasks:
        state = job.state or "UNKNOWN"
        summary.state_counts[state] = summary.state_counts.get(state, 0) + 1

    completed = [job for job in tasks if job.state == COMPLETED_STATE]

    # runtime aggregation (all tasks that have a parseable elapsed time)
    per_state_seconds: dict[str, list[int]] = {}
    for job in tasks:
        seconds = _elapsed_seconds(job)
        if seconds is None:
            continue
        summary.total_task_seconds += seconds
        summary.elapsed_minutes.append(seconds / 60)
        state = job.state or "UNKNOWN"
        per_state_seconds.setdefault(state, []).append(seconds)
    summary.per_state_mean_seconds = {
        state: sum(values) / len(values)
        for state, values in per_state_seconds.items()
        if values
    }

    # per-column aggregation
    seen: set[str] = set()
    for title in titles:
        fold = title.casefold()
        if fold in _EXCLUDE_TITLES or fold in seen:
            continue
        seen.add(fold)

        if fold == _NODELIST_TITLE:
            summary.node_hostlist = _summarize_nodes(tasks, title)
            continue

        # numeric metric (completed-only) when values coerce to float
        numeric = [
            v
            for job in completed
            if (v := _coerce_float(job.get_entry(title))) is not None
        ]
        if numeric:
            summary.metrics.append(
                MetricStat(
                    title=title,
                    minimum=min(numeric),
                    mean=sum(numeric) / len(numeric),
                    maximum=max(numeric),
                    values=numeric,
                )
            )
            continue

        # categorical: collapse shared vs unique across all tasks
        values = sorted(
            {
                str(raw).strip()
                for job in tasks
                if (raw := job.get_entry(title)) not in ("", "---", None)
            }
        )
        if not values:
            continue
        if len(values) == 1:
            summary.shared_values.append((title, values[0]))
        else:
            summary.unique_values.append((title, values))

    return summary


def _summarize_nodes(tasks: list[Job], title: str) -> str | None:
    """Collect and compact node names from the given column across tasks."""
    names: set[str] = set()
    for job in tasks:
        raw = job.get_entry(title)
        if not isinstance(raw, str) or raw in ("", "---"):
            continue
        names.update(expand_hostlist(raw))
    if not names:
        return None
    return compact_hostlist(names)


def _split_top_level(value: str) -> list[str]:
    """Split a hostlist string on commas that are not inside brackets."""
    tokens: list[str] = []
    depth = 0
    current = ""
    for char in value:
        if char == "[":
            depth += 1
        elif char == "]":
            depth = max(0, depth - 1)
        if char == "," and depth == 0:
            tokens.append(current)
            current = ""
        else:
            current += char
    if current:
        tokens.append(current)
    return tokens


def expand_hostlist(value: str) -> list[str]:
    """Expand a Slurm-style hostlist into individual node names.

    Examples:
        ``somacpu[001-003,005]`` -> ``[somacpu001, somacpu002, somacpu003, somacpu005]``
        ``nodeA,nodeB`` -> ``[nodeA, nodeB]``

    Args:
        value: a hostlist string, possibly containing bracketed numeric ranges

    Returns:
        The list of expanded node names.  Tokens that do not match the bracket
        pattern are returned verbatim.
    """
    result: list[str] = []
    for raw_token in _split_top_level(value.strip()):
        token = raw_token.strip()
        if not token:
            continue
        match = _HOSTLIST_RE.match(token)
        if not match:
            result.append(token)
            continue
        prefix = match.group("prefix")
        for part in match.group("ranges").split(","):
            if "-" in part:
                lo_str, hi_str = part.split("-", 1)
                width = len(lo_str)
                result.extend(
                    f"{prefix}{num:0{width}d}"
                    for num in range(int(lo_str), int(hi_str) + 1)
                )
            else:
                result.append(f"{prefix}{part}")
    return result


def compact_hostlist(names: set[str] | list[str]) -> str:
    """Collapse node names into compact Slurm-style ranges.

    Names sharing a non-numeric prefix and equal zero-pad width have their
    consecutive numeric suffixes collapsed into ``lo-hi`` ranges, e.g.::

        somacpu[001-088,101-124],somagpu[001-093]

    Args:
        names: the set/list of individual node names

    Returns:
        A compact hostlist string.  Names without a numeric suffix are listed
        verbatim.
    """
    # group by (prefix, width); collect verbatim names separately
    groups: dict[tuple[str, int], list[int]] = {}
    plain: list[str] = []
    for name in names:
        match = _NODE_RE.match(name)
        if not match:
            plain.append(name)
            continue
        prefix = match.group("prefix")
        num_str = match.group("num")
        groups.setdefault((prefix, len(num_str)), []).append(int(num_str))

    parts: list[str] = []
    for (prefix, width), numbers in groups.items():
        ranges = _collapse_numbers(sorted(set(numbers)), width)
        if len(ranges) == 1 and "-" not in ranges[0]:
            parts.append(f"{prefix}{ranges[0]}")
        else:
            parts.append(f"{prefix}[{','.join(ranges)}]")

    parts.extend(sorted(plain))
    # sort for stable output, prefix groups first by prefix name
    return ",".join(sorted(parts))


def _collapse_numbers(numbers: list[int], width: int) -> list[str]:
    """Collapse a sorted list of ints into zero-padded range strings."""
    ranges: list[str] = []
    start = prev = numbers[0]
    for num in numbers[1:]:
        if num == prev + 1:
            prev = num
            continue
        ranges.append(_format_range(start, prev, width))
        start = prev = num
    ranges.append(_format_range(start, prev, width))
    return ranges


def _format_range(start: int, end: int, width: int) -> str:
    """Format a single numeric range, zero-padded to width."""
    if start == end:
        return f"{start:0{width}d}"
    return f"{start:0{width}d}-{end:0{width}d}"


def format_duration(seconds: float) -> str:
    """Render a duration in a compact ``h/m/s`` form (e.g. ``1h12m``)."""
    total = round(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def compute_histogram(
    values: list[float],
    bins: int = 10,
) -> tuple[list[tuple[float, float]], list[int]]:
    """Bin values into fixed-width bins.

    Args:
        values: the numeric values to bin
        bins: the desired number of bins

    Returns:
        A tuple of ``(edges, counts)`` where ``edges`` is a list of
        ``(low, high)`` tuples and ``counts`` the number of values in each bin.
        Returns empty lists when there are no values.
    """
    if not values:
        return [], []
    low = min(values)
    high = max(values)
    if high == low:
        return [(low, high)], [len(values)]

    bins = max(1, bins)
    span = high - low
    width = span / bins
    edges = [(low + i * width, low + (i + 1) * width) for i in range(bins)]
    counts = [0] * bins
    for value in values:
        idx = int((value - low) / width)
        if idx >= bins:  # the maximum value falls into the last bin
            idx = bins - 1
        counts[idx] += 1
    return edges, counts


def _bar(count: int, max_count: int, width: int, *, ascii_only: bool) -> str:
    """Build a single horizontal bar string for a bin count."""
    if max_count <= 0 or count <= 0:
        return ""
    if ascii_only:
        return "#" * max(1, round(count / max_count * width))
    eighths = max(1, round(count / max_count * width * 8))
    full, remainder = divmod(eighths, 8)
    return "█" * full + (_EIGHTHS_H[remainder] if remainder else "")


def render_histogram(
    values: list[float],
    *,
    bins: int = 10,
    width: int = 24,
    ascii_only: bool = False,
    unit: str = "min",
    label: str = "Runtime",
) -> list[str]:
    """Render a histogram of a metric's distribution as a list of text lines.

    Args:
        values: the values (e.g. task runtimes in minutes)
        bins: number of histogram bins
        width: maximum bar width in characters
        ascii_only: use ``#`` bars instead of Unicode block characters
        unit: unit label shown in the header, e.g. ``"min"`` or ``"%"``.
            Pass ``""`` when no unit is known; the parentheses are omitted.
        label: the metric name shown in the header (default ``"Runtime"``,
            matching this function's original single-purpose behavior)

    Returns:
        A list of strings (header + one line per bin).  Empty when no values.
    """
    edges, counts = compute_histogram(values, bins)
    if not edges:
        return []

    max_count = max(counts)
    label_width = max(len(f"{round(low)}-{round(high)}") for low, high in edges)
    header = f"{label} ({unit})" if unit else label
    lines = [f"{header}   n={len(values)}"]
    for (low, high), count in zip(edges, counts, strict=True):
        label_text = f"{round(low)}-{round(high)}".rjust(label_width)
        bar = _bar(count, max_count, width, ascii_only=ascii_only)
        lines.append(f"{label_text}  {bar} {count}")
    return lines


def render_sparkline(values: list[float], *, bins: int = 10) -> str:
    """Render a compact one-line sparkline of the distribution.

    Args:
        values: the values to bin
        bins: number of bins (one character each)

    Returns:
        A single string of vertical block characters, or an empty string when
        there are no values.
    """
    _edges, counts = compute_histogram(values, bins)
    if not counts:
        return ""
    max_count = max(counts)
    if max_count <= 0:
        return ""
    chars = []
    for count in counts:
        if count <= 0:
            chars.append(" ")
        else:
            level = round(count / max_count * (len(_EIGHTHS_V) - 1))
            chars.append(_EIGHTHS_V[level])
    return "".join(chars)
