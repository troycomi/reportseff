"""Tests for the array summary aggregation and rendering helpers."""

from __future__ import annotations

import pytest

from reportseff import array_summary as summ
from reportseff.job import Job


def _make_job(  # noqa: PLR0913
    jobid: str,
    state: str,
    elapsed: str = "00:10:00",
    total_cpu: str = "00:09:00",
    *,
    partition: str = "CPU",
    nodelist: str = "somacpu001",
    job_name: str = "",
) -> Job:
    """Build a Job via the normal update path for testing."""
    base = jobid.split("_", 1)[0]
    job = Job(base, jobid, None)
    entry = {
        "JobID": jobid,
        "State": state,
        "AllocCPUS": "1",
        "ReqMem": "1Gn",
        "TotalCPU": total_cpu,
        "Elapsed": elapsed,
        "Timelimit": "00:20:00",
        "MaxRSS": "",
        "NNodes": "1",
        "NTasks": "",
        "Partition": partition,
        "NodeList": nodelist,
    }
    if job_name:
        entry["JobName"] = job_name
    job.update(entry)
    return job


# ---------------------------------------------------------------------------
# grouping
# ---------------------------------------------------------------------------


def test_group_jobs_by_array_preserves_order() -> None:
    """Grouping bundles tasks by base id in first-appearance order."""
    jobs = [
        _make_job("100_1", "COMPLETED"),
        _make_job("100_2", "COMPLETED"),
        _make_job("200", "COMPLETED"),
        _make_job("100_3", "COMPLETED"),
    ]
    grouped = summ.group_jobs_by_array(jobs)
    assert [base for base, _ in grouped] == ["100", "200"]
    assert [j.jobid for j in grouped[0][1]] == ["100_1", "100_2", "100_3"]
    assert [j.jobid for j in grouped[1][1]] == ["200"]


def test_is_array_group() -> None:
    """Array groups need >=2 tasks or an underscore in the jobid."""
    assert summ.is_array_group([_make_job("1_1", "COMPLETED")])
    assert summ.is_array_group(
        [_make_job("1_1", "COMPLETED"), _make_job("1_2", "COMPLETED")]
    )
    assert not summ.is_array_group([_make_job("5", "COMPLETED")])


def test_group_jobs_by_name_preserves_order() -> None:
    """Grouping bundles tasks by JobName in first-appearance order."""
    jobs = [
        _make_job("100_1", "COMPLETED", job_name="alpha"),
        _make_job("200_1", "COMPLETED", job_name="beta"),
        _make_job("100_2", "COMPLETED", job_name="alpha"),
    ]
    grouped = summ.group_jobs_by_name(jobs)
    assert [name for name, _ in grouped] == ["alpha", "beta"]
    assert [j.jobid for j in grouped[0][1]] == ["100_1", "100_2"]
    assert [j.jobid for j in grouped[1][1]] == ["200_1"]


def test_group_jobs_by_name_falls_back_to_base_id() -> None:
    """Tasks without a usable JobName fall back to their own base id.

    This protects against silently merging unrelated jobs under one empty
    key when JobName wasn't queried/available for a given task (e.g. a
    PENDING job, whose fields largely aren't cached -- see Job.update()).
    """
    jobs = [
        _make_job("100_1", "COMPLETED", job_name=""),
        _make_job("200_1", "COMPLETED", job_name=""),
    ]
    grouped = summ.group_jobs_by_name(jobs)
    assert [name for name, _ in grouped] == ["100", "200"]


def test_group_jobs_by_name_mixed_availability() -> None:
    """A named task groups by name; an unnamed one falls back separately."""
    jobs = [
        _make_job("100_1", "COMPLETED", job_name="myrule"),
        _make_job("300_1", "COMPLETED", job_name=""),
    ]
    grouped = summ.group_jobs_by_name(jobs)
    assert [name for name, _ in grouped] == ["myrule", "300"]


# ---------------------------------------------------------------------------
# --graph-format parsing
# ---------------------------------------------------------------------------


def test_graph_format_vocabulary_contents() -> None:
    """Vocabulary matches the agreed design: energy (not power), gpu metrics."""
    assert summ.GRAPH_FORMAT_VOCABULARY == (
        "runtime",
        "cpueff",
        "memeff",
        "energy",
        "gpueff",
        "gpumem",
    )


def test_parse_graph_format_basic() -> None:
    """Values are comma-split, case-folded, deduped, and order-preserving."""
    assert summ.parse_graph_format("Runtime, CPUEff,runtime") == (
        "runtime",
        "cpueff",
    )


def test_parse_graph_format_empty_tokens_ignored() -> None:
    """Blank tokens (e.g. a trailing comma) are dropped rather than erroring."""
    assert summ.parse_graph_format("runtime,,cpueff,") == ("runtime", "cpueff")


def test_parse_graph_format_full_vocabulary_accepted() -> None:
    """Every recognized metric name parses without error."""
    value = ",".join(summ.GRAPH_FORMAT_VOCABULARY)
    assert summ.parse_graph_format(value) == summ.GRAPH_FORMAT_VOCABULARY


def test_parse_graph_format_unknown_metric_raises() -> None:
    """An unrecognized metric name raises with a helpful message."""
    with pytest.raises(ValueError, match="bogus"):
        summ.parse_graph_format("runtime,bogus")


def test_parse_graph_format_power_is_not_a_valid_name() -> None:
    """`power` was superseded by `energy`; it is not a recognized alias."""
    with pytest.raises(ValueError, match="power"):
        summ.parse_graph_format("power")


# ---------------------------------------------------------------------------
# build_array_summary
# ---------------------------------------------------------------------------


def test_build_summary_counts_and_completed() -> None:
    """State counters and completion count are correct."""
    tasks = [
        _make_job("100_1", "COMPLETED"),
        _make_job("100_2", "COMPLETED"),
        _make_job("100_3", "FAILED", elapsed="00:00:30", total_cpu="00:00:10"),
        _make_job("100_4", "RUNNING", elapsed="00:03:00", total_cpu="00:00:00"),
    ]
    result = summ.build_array_summary("100", tasks, ["State", "CPUEff"])
    assert result.total_tasks == 4
    assert result.completed_tasks == 2
    assert result.state_counts == {"COMPLETED": 2, "FAILED": 1, "RUNNING": 1}


def test_build_summary_metrics_completed_only() -> None:
    """Numeric metrics are aggregated over completed tasks only."""
    tasks = [
        _make_job("100_1", "COMPLETED", elapsed="00:10:00", total_cpu="00:09:00"),
        _make_job("100_2", "COMPLETED", elapsed="00:05:00", total_cpu="00:01:00"),
        _make_job("100_3", "FAILED", elapsed="00:00:30", total_cpu="00:00:10"),
    ]
    result = summ.build_array_summary("100", tasks, ["CPUEff", "TimeEff"])
    metrics = {m.title: m for m in result.metrics}
    # CPUEff: 90.0 and 20.0 for the two completed tasks
    assert metrics["CPUEff"].minimum == 20.0
    assert metrics["CPUEff"].maximum == 90.0
    assert metrics["CPUEff"].mean == 55.0


def test_build_summary_metrics_retain_raw_values() -> None:
    """Each MetricStat retains the raw per-completed-task values, not just
    the aggregate -- needed so --graph-format can graph any metric, not
    only runtime.
    """
    tasks = [
        _make_job("100_1", "COMPLETED", elapsed="00:10:00", total_cpu="00:09:00"),
        _make_job("100_2", "COMPLETED", elapsed="00:05:00", total_cpu="00:01:00"),
        _make_job("100_3", "FAILED", elapsed="00:00:30", total_cpu="00:00:10"),
    ]
    result = summ.build_array_summary("100", tasks, ["CPUEff"])
    metrics = {m.title: m for m in result.metrics}
    assert sorted(metrics["CPUEff"].values) == [20.0, 90.0]
    # excludes the FAILED task -- completed-only, same as min/mean/max
    assert len(metrics["CPUEff"].values) == 2


def test_metric_stat_values_defaults_to_empty_list() -> None:
    """Direct construction without values= still works (backward compatible)."""
    stat = summ.MetricStat(title="CPUEff", minimum=1.0, mean=2.0, maximum=3.0)
    assert stat.values == []


def test_build_summary_total_and_per_state_runtime() -> None:
    """Total task seconds and per-state mean runtime are computed."""
    tasks = [
        _make_job("100_1", "COMPLETED", elapsed="00:10:00"),
        _make_job("100_2", "COMPLETED", elapsed="00:05:00"),
        _make_job("100_3", "FAILED", elapsed="00:00:30"),
    ]
    result = summ.build_array_summary("100", tasks, ["State"])
    assert result.total_task_seconds == 600 + 300 + 30
    assert result.per_state_mean_seconds["COMPLETED"] == 450.0
    assert result.per_state_mean_seconds["FAILED"] == 30.0


def test_build_summary_shared_and_unique() -> None:
    """Constant columns are shared; differing columns are unique lists."""
    tasks = [
        _make_job("100_1", "COMPLETED", partition="CPU"),
        _make_job("100_2", "COMPLETED", partition="GPU"),
    ]
    result = summ.build_array_summary("100", tasks, ["Partition"])
    # Partition differs -> unique
    assert ("Partition", ["CPU", "GPU"]) in result.unique_values

    tasks_same = [
        _make_job("100_1", "COMPLETED", partition="CPU"),
        _make_job("100_2", "COMPLETED", partition="CPU"),
    ]
    result_same = summ.build_array_summary("100", tasks_same, ["Partition"])
    assert ("Partition", "CPU") in result_same.shared_values


def test_build_summary_nodelist_compacted() -> None:
    """NodeList values are collapsed into a hostlist string."""
    tasks = [
        _make_job("100_1", "COMPLETED", nodelist="somacpu001"),
        _make_job("100_2", "COMPLETED", nodelist="somacpu002"),
        _make_job("100_3", "COMPLETED", nodelist="somacpu003"),
    ]
    result = summ.build_array_summary("100", tasks, ["NodeList"])
    assert result.node_hostlist == "somacpu[001-003]"


# ---------------------------------------------------------------------------
# hostlist expand / compact
# ---------------------------------------------------------------------------


def test_expand_hostlist_ranges() -> None:
    """Bracketed ranges are expanded, preserving zero padding."""
    assert summ.expand_hostlist("somacpu[001-003,005]") == [
        "somacpu001",
        "somacpu002",
        "somacpu003",
        "somacpu005",
    ]


def test_expand_hostlist_plain_and_commas() -> None:
    """Plain names and top-level commas are handled."""
    assert summ.expand_hostlist("nodeA,nodeB") == ["nodeA", "nodeB"]
    assert summ.expand_hostlist("somacpu[001-002],somagpu003") == [
        "somacpu001",
        "somacpu002",
        "somagpu003",
    ]


def test_compact_hostlist_multi_prefix() -> None:
    """Names collapse into per-prefix ranges."""
    names = [
        "somacpu001",
        "somacpu002",
        "somacpu003",
        "somagpu001",
        "somagpu093",
    ]
    result = summ.compact_hostlist(names)
    assert "somacpu[001-003]" in result
    assert "somagpu[001,093]" in result


def test_compact_hostlist_single_node() -> None:
    """A single node is not wrapped in brackets."""
    assert summ.compact_hostlist(["somacpu007"]) == "somacpu007"


def test_compact_hostlist_non_numeric_verbatim() -> None:
    """Names without a numeric suffix are kept verbatim."""
    assert summ.compact_hostlist(["login", "gateway"]) == "gateway,login"


def test_hostlist_roundtrip() -> None:
    """Expanding then compacting is stable for a typical hostlist."""
    original = "somacpu[001-004],somagpu007"
    expanded = summ.expand_hostlist(original)
    assert summ.compact_hostlist(expanded) == "somacpu[001-004],somagpu007"


# ---------------------------------------------------------------------------
# histogram / sparkline
# ---------------------------------------------------------------------------


def test_compute_histogram_basic() -> None:
    """Values are binned; counts sum to the number of values."""
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    edges, counts = summ.compute_histogram(values, bins=5)
    assert len(edges) == 5
    assert sum(counts) == len(values)


def test_compute_histogram_uniform_values() -> None:
    """Identical values produce a single bin."""
    edges, counts = summ.compute_histogram([5, 5, 5], bins=10)
    assert edges == [(5, 5)]
    assert counts == [3]


def test_compute_histogram_empty() -> None:
    """No values yields empty results."""
    assert summ.compute_histogram([], bins=10) == ([], [])


def test_render_histogram_unicode_and_ascii() -> None:
    """Histogram lines use block chars, or # in ascii mode."""
    values = [float(i) for i in range(100)]
    unicode_lines = summ.render_histogram(values, bins=5, ascii_only=False)
    assert unicode_lines[0].startswith("Runtime (min)")
    assert any("█" in line for line in unicode_lines[1:])

    ascii_lines = summ.render_histogram(values, bins=5, ascii_only=True)
    assert any("#" in line for line in ascii_lines[1:])
    assert not any("█" in line for line in ascii_lines)


def test_render_histogram_empty() -> None:
    """No values yields no lines."""
    assert summ.render_histogram([], bins=5) == []


def test_render_histogram_default_label_is_runtime() -> None:
    """Without label=, the header still says "Runtime" (backward compatible)."""
    values = [float(i) for i in range(100)]
    lines = summ.render_histogram(values, bins=5)
    assert lines[0].startswith("Runtime (min)")


def test_render_histogram_custom_label_and_unit() -> None:
    """label= and unit= let any metric reuse the same histogram renderer."""
    values = [float(i) for i in range(100)]
    lines = summ.render_histogram(values, bins=5, unit="%", label="CPUEff")
    assert lines[0].startswith("CPUEff (%)")


def test_render_histogram_empty_unit_omits_parens() -> None:
    """An empty unit (e.g. for a metric with no known unit) drops the parens."""
    values = [float(i) for i in range(100)]
    lines = summ.render_histogram(values, bins=5, unit="", label="Energy")
    assert lines[0].startswith("Energy   n=")
    assert "(" not in lines[0]


def test_render_sparkline() -> None:
    """Sparkline is a single line of block characters, one per bin."""
    values = [float(i) for i in range(50)]
    spark = summ.render_sparkline(values, bins=8)
    assert len(spark) == 8
    assert all(char in summ._EIGHTHS_V + " " for char in spark)


def test_render_sparkline_empty() -> None:
    """No values yields an empty sparkline."""
    assert summ.render_sparkline([]) == ""


# ---------------------------------------------------------------------------
# duration formatting
# ---------------------------------------------------------------------------


def test_format_duration() -> None:
    """Durations render compactly in h/m/s."""
    assert summ.format_duration(4335) == "1h12m"
    assert summ.format_duration(450) == "7m30s"
    assert summ.format_duration(30) == "30s"
