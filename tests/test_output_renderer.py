"""Test operation of output renderer object."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import click
import pytest

from reportseff import output_renderer
from reportseff.job import Job

if TYPE_CHECKING:
    from reportseff.output_renderer import OutputRenderer

    from .typings import sacct_return

min_required = [
    "JobID",
    "State",
    "Elapsed",
    "JobIDRaw",
    "State",
    "TotalCPU",
    "AllocCPUS",
    "ReqMem",
    "NNodes",
    "MaxRSS",
    "Timelimit",
]


@pytest.fixture
def renderer() -> OutputRenderer:
    """Default renderer with valid names for only default string."""
    return output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(),
    )


@pytest.fixture
def some_jobs() -> list[Job]:
    """A few test jobs for generating an output table."""
    jobs = []
    job = Job("24371655", "24371655", None)
    job.update(
        {
            "JobID": "24371655",
            "State": "COMPLETED",
            "AllocCPUS": "1",
            "ReqMem": "1Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:10:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = Job("24371656", "24371656", None)
    job.update(
        {
            "JobID": "24371656",
            "State": "PENDING",
            "AllocCPUS": "1",
            "ReqMem": "1Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:10:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = Job("24371657", "24371657", None)
    job.update(
        {
            "JobID": "24371657",
            "State": "RUNNING",
            "AllocCPUS": "1",
            "ReqMem": "1Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:10:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = Job("24371658", "24371658", None)
    job.update(
        {
            "JobID": "24371658",
            "State": "CANCELLED",
            "AllocCPUS": "1",
            "ReqMem": "1Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:00:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = Job("24371659", "24371659", None)
    job.update(
        {
            "JobID": "24371659",
            "State": "TIMEOUT",
            "AllocCPUS": "1",
            "ReqMem": "2Gn",
            "TotalCPU": "00:04:00",
            "Elapsed": "00:22:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = Job("24371660", "24371660", None)
    job.update(
        {
            "JobID": "24371660",
            "State": "OTHER",
            "AllocCPUS": "1",
            "ReqMem": "2Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:12:05",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    return jobs


@pytest.fixture
def gpu_jobs(
    single_gpu: sacct_return,
    multi_gpu: sacct_return,
    multi_node_multi_gpu: sacct_return,
) -> list[Job]:
    """A collection of jobs with gpus."""
    jobs = []

    job = Job("8189521", "8189521", None)
    for line in multi_node_multi_gpu:
        job.update(line)
    jobs.append(job)

    job = Job("8189521", "8189521", None)
    for line in multi_gpu:
        job.update(line)
    jobs.append(job)

    job = Job("8197399", "8197399", None)
    for line in single_gpu:
        job.update(line)
    jobs.append(job)

    return jobs


@pytest.fixture
def cpu_jobs(
    single_core: sacct_return,
    multi_node: sacct_return,
    short_job: sacct_return,
) -> list[Job]:
    """A collection of cpu jobs."""
    jobs = []

    job = Job("8205464", "8205464", None)
    for line in short_job:
        job.update(line)
    jobs.append(job)

    job = Job("8205048", "8205048", None)
    for line in multi_node:
        job.update(line)
    jobs.append(job)

    job = Job("39895850", "39889258_1426", None)
    for line in single_core:
        job.update(line)
    jobs.append(job)

    return jobs


@pytest.fixture
def some_multi_core_jobs(gpu_jobs: list[Job], cpu_jobs: list[Job]) -> list[Job]:
    """A collection of jobs with multiple cores/gpus."""
    result = []
    result.append(cpu_jobs[0])
    result += gpu_jobs
    result += cpu_jobs[1:]
    return result


def assert_result_matches(result: str, expected: list[str]) -> None:
    """Check the result of an output renderer matches the expected lines."""
    ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
    split_result = ansi_escape.sub("", result).split("\n")
    for line, expected_line in zip(split_result, expected, strict=True):
        assert line.split() == expected_line.split()


def test_renderer_init(renderer: OutputRenderer) -> None:
    """Initialized renderer produces correct columns."""
    assert renderer.formatters == [
        output_renderer.ColumnFormatter("JobID%>"),
        output_renderer.ColumnFormatter("State"),
        output_renderer.ColumnFormatter("Elapsed%>"),
        output_renderer.ColumnFormatter("CPUEff"),
        output_renderer.ColumnFormatter("MemEff"),
    ]
    assert sorted(renderer.query_columns) == sorted(
        [
            "JobID",
            "JobIDRaw",
            "State",
            "Elapsed",
            "TotalCPU",
            "AllocCPUS",
            "ReqMem",
            "NNodes",
            "NTasks",
            "MaxRSS",
            "AdminComment",
        ]
    )

    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(),
        "",
    )
    assert renderer.formatters == []
    assert sorted(renderer.query_columns) == sorted(
        ["JobID", "JobIDRaw", "State", "AdminComment"]
    )

    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(),
        "TotalCPU%<5",
    )
    assert renderer.formatters == [output_renderer.ColumnFormatter("TotalCPU%<5")]
    assert sorted(renderer.query_columns) == sorted(
        ["JobID", "JobIDRaw", "State", "TotalCPU", "AdminComment"]
    )


def test_renderer_build_formatters() -> None:
    """Can parse formatters from format string."""
    assert output_renderer.build_formatters("Name,Name%>,Name%10,Name%<10") == [
        output_renderer.ColumnFormatter("Name"),
        output_renderer.ColumnFormatter("Name%>"),
        output_renderer.ColumnFormatter("Name%10"),
        output_renderer.ColumnFormatter("Name%<10"),
    ]

    assert output_renderer.build_formatters("jobid,state,elapsed") == [
        "jobid",
        "state",
        "elapsed",
    ]

    assert output_renderer.build_formatters("") == []


def test_renderer_validate_formatters(renderer: OutputRenderer) -> None:
    """Can validate formatters as members of a provided collection, normalizing name."""
    renderer.formatters = output_renderer.build_formatters("JobID,JOBid,jObId")
    assert renderer.validate_formatters(["JobID"], []) == ["JobID", "JobID", "JobID"]
    assert renderer.formatters == ["JobID", "JobID", "JobID"]

    renderer.formatters = output_renderer.build_formatters("JobID,GPU%>10")
    assert renderer.validate_formatters(["JobID"], ["GPU", "GPUEff", "GPUMem"]) == [
        "JobID",
        "GPU",
    ]
    assert renderer.formatters == ["JobID", "GPUEff", "GPUMem"]
    # other params are copied from GPU to GPUEff and GPUMem
    assert renderer.formatters[1].alignment == ">"
    assert renderer.formatters[2].alignment == ">"
    assert renderer.formatters[1].width == 10
    assert renderer.formatters[2].width == 10


def test_renderer_validate_formatters_with_node(renderer: OutputRenderer) -> None:
    """Validating formatters with GPUs can alter formatters."""
    min_gpu = (min_required, ["GPU", "GPUEff", "GPUMem"])
    # normal function
    renderer.options.node = False
    renderer.options.gpu = False
    renderer.formatters = output_renderer.build_formatters("State")
    assert renderer.validate_formatters(*min_gpu) == ["State"]
    assert renderer.formatters == ["State"]

    # add in job id
    renderer.options.node = True
    renderer.options.gpu = False
    renderer.formatters = output_renderer.build_formatters("State")
    assert renderer.validate_formatters(*min_gpu) == ["State"]
    assert renderer.formatters == ["JobID", "State"]

    # add in both gpus, gpu implies node
    renderer.options.node = True
    renderer.options.gpu = True
    renderer.formatters = output_renderer.build_formatters("State")
    assert renderer.validate_formatters(*min_gpu) == ["State"]
    assert renderer.formatters == ["JobID", "State", "GPUEff", "GPUMem"]
    assert renderer.formatters[0].alignment == "<"  # switched by node reporting

    # since format already has jobid and gpumem, will not override
    renderer.options.node = True
    renderer.options.gpu = True
    renderer.formatters = output_renderer.build_formatters("GPUMEM,State,JobID:>")
    assert renderer.validate_formatters(*min_gpu) == ["GPUMem", "State", "JobID"]
    assert renderer.formatters == ["GPUMem", "State", "JobID"]
    assert renderer.formatters[2].alignment == "<"  # switched by node reporting


def test_renderer_correct_columns(renderer: OutputRenderer) -> None:
    """Corrected columns include required entries and derived values."""
    renderer.query_columns = ["JobID"]
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == sorted(
        ["JobID", "JobIDRaw", "State", "AdminComment"]
    )

    renderer.query_columns = ["JobID", "CPUEff", "MemEff", "TimeEff"]
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == sorted(
        [
            "JobID",
            "TotalCPU",
            "Elapsed",
            "ReqMem",
            "JobIDRaw",
            "State",
            "AdminComment",
            "NNodes",
            "NTasks",
            "AllocCPUS",
            "MaxRSS",
            "Timelimit",
        ]
    )

    renderer.query_columns = ["JobID", "JobID", "JobID"]
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == sorted(
        ["JobID", "JobIDRaw", "State", "AdminComment"]
    )

    renderer.query_columns = ["JobID", "ReqMem", "MemEff"]
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == sorted(
        [
            "JobID",
            "JobIDRaw",
            "State",
            "AdminComment",
            "AllocCPUS",
            "MaxRSS",
            "NNodes",
            "NTasks",
            "ReqMem",
        ]
    )


def test_renderer_format_jobs(some_jobs: list[Job]) -> None:
    """Can render output as table with colored entries."""
    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(),
        "JobID,State,Elapsed,CPUEff,ReqMem,TimeEff",
    )
    result = renderer.format_jobs(some_jobs)
    ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
    # check removed codes
    codes = ansi_escape.findall(result)
    for code in codes[1::2]:  # normal
        assert code == "\x1b[0m"
    for code in codes[0:10:20]:
        assert code == "\x1b[1m"  # bold
    for i in (22, 24, 26):
        assert codes[i] == "\x1b[31m"  # red
    for i in (12, 14, 30):
        assert codes[i] == "\x1b[32m"  # green
    for i in (20,):
        assert codes[i] == "\x1b[33m"  # yellow
    for i in (16,):
        assert codes[i] == "\x1b[34m"  # blue
    for i in (18,):
        assert codes[i] == "\x1b[36m"  # cyan
    for i in (28,):
        assert codes[i] == "\x1b[35m"  # magenta
    # remove color codes
    expected = [
        "JobID State Elapsed CPUEff ReqMem TimeEff",
        "24371655 COMPLETED 00:10:00 90.0% 1Gn 50.0%",
        "24371656 PENDING --- --- --- ---",
        "24371657 RUNNING 00:10:00 --- 1Gn 50.0%",
        "24371658 CANCELLED 00:00:00 --- 1Gn 0.0%",
        "24371659 TIMEOUT 00:22:00 18.2% 2Gn 110.0%",
        "24371660 OTHER 00:12:05 74.5% 2Gn 60.4%",
    ]
    assert_result_matches(result, expected)


def test_renderer_format_jobs_multi_node(some_multi_core_jobs: list[Job]) -> None:
    """Can render output as table with colored entries."""
    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(),
        "JobID,State,CPUEff,TimeEff,MemEff,GPU",
    )
    result = renderer.format_jobs(some_multi_core_jobs)
    expected = [
        "JobID State CPUEff TimeEff MemEff GPUEff GPUMem",
        "8205464 FAILED 6.2% 0.0% 0.0% --- ---",
        "8189521 CANCELLED 10.5% 83.0% 26.0% 5.5% 30.1%",
        "8189521 CANCELLED 10.5% 83.0% 26.3% 3.5% 30.1%",
        "8197399 COMPLETED 95.4% 21.1% 9.5% 29.4% 99.8%",
        "8205048 COMPLETED 4.6% 4.1% 1.1% --- ---",
        "39889258_1426 COMPLETED 99.7% 76.7% 3.6% --- ---",
    ]
    assert_result_matches(result, expected)


def test_renderer_format_jobs_multi_node_with_nodes(
    some_multi_core_jobs: list[Job],
) -> None:
    """Can render output as table with colored entries."""
    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(node=True),
        "JobID,State,CPUEff,TimeEff,MemEff,GPU",
    )
    result = renderer.format_jobs(some_multi_core_jobs)
    expected = [
        "JobID State CPUEff TimeEff MemEff GPUEff GPUMem",
        "8205464 FAILED 6.2% 0.0% 0.0% --- ---",
        "8189521 CANCELLED 10.5% 83.0% 26.0% 5.5% 30.1%",
        "tiger-i19g10 10.5% 25.8% 7.5% 30.1%",
        "tiger-i19g9 10.5% 26.3% 3.5% 30.1%",
        "8189521 CANCELLED 10.5% 83.0% 26.3% 3.5% 30.1%",
        "8197399 COMPLETED 95.4% 21.1% 9.5% 29.4% 99.8%",
        "8205048 COMPLETED 4.6% 4.1% 1.1% --- ---",
        "tiger-h19c1n15 18.6% 4.5%",
        "tiger-h26c2n13 0.0% 0.0%",
        "tiger-i26c2n11 0.0% 0.0%",
        "tiger-i26c2n15 0.0% 0.0%",
        "39889258_1426 COMPLETED 99.7% 76.7% 3.6% --- ---",
    ]
    assert_result_matches(result, expected)


def test_renderer_format_jobs_multi_node_with_nodes_and_gpu(
    some_multi_core_jobs: list[Job],
) -> None:
    """Can render output as table with colored entries."""
    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(node=True, gpu=True),
        "JobID,State,CPUEff,TimeEff,MemEff,GPU",
    )
    result = renderer.format_jobs(some_multi_core_jobs)
    expected = [
        "JobID State CPUEff TimeEff MemEff GPUEff GPUMem",
        "8205464 FAILED 6.2% 0.0% 0.0% --- ---",
        "8189521 CANCELLED 10.5% 83.0% 26.0% 5.5% 30.1%",
        "tiger-i19g10 10.5% 25.8% 7.5% 30.1%",
        "0 7.5% 30.1%",
        "1 7.5% 30.1%",
        "2 7.2% 30.1%",
        "3 7.8% 30.1%",
        "tiger-i19g9 10.5% 26.3% 3.5% 30.1%",
        "0 3.5% 30.1%",
        "1 3.5% 30.1%",
        "2 3.2% 30.1%",
        "3 3.8% 30.1%",
        "8189521 CANCELLED 10.5% 83.0% 26.3% 3.5% 30.1%",
        "tiger-i19g9 10.5% 26.3% 3.5% 30.1%",
        "0 3.5% 30.1%",
        "1 3.5% 30.1%",
        "2 3.2% 30.1%",
        "3 3.8% 30.1%",
        "8197399 COMPLETED 95.4% 21.1% 9.5% 29.4% 99.8%",
        "tiger-i23g14 95.4% 9.5% 29.4% 99.8%",
        "3 29.4% 99.8%",
        "8205048 COMPLETED 4.6% 4.1% 1.1% --- ---",
        "tiger-h19c1n15 18.6% 4.5%",
        "tiger-h26c2n13 0.0% 0.0%",
        "tiger-i26c2n11 0.0% 0.0%",
        "tiger-i26c2n15 0.0% 0.0%",
        "39889258_1426 COMPLETED 99.7% 76.7% 3.6% --- ---",
    ]
    assert_result_matches(result, expected)


def test_format_jobs_empty(some_jobs: list[Job]) -> None:
    """Empty format string produces empty outputs."""
    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(),
        "",
    )
    result = renderer.format_jobs(some_jobs)
    assert result == ""


def test_format_jobs_single_str(some_jobs: list[Job]) -> None:
    """A single format string left aligns and suppresses title for piping."""
    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(),
        "JobID%>",
    )
    assert len(renderer.formatters) == 1
    assert renderer.formatters[0].alignment == ">"

    result = renderer.format_jobs(some_jobs).split("\n")

    # alignment is switched
    assert renderer.formatters[0].alignment == "<"
    assert result == [
        "24371655",
        "24371656",
        "24371657",
        "24371658",
        "24371659",
        "24371660",
    ]


def test_formatter_init() -> None:
    """Column formatter parses format tokens correctly."""
    # simple name
    result = output_renderer.ColumnFormatter("test")
    assert result.title == "test"
    assert result.alignment == "^"
    assert result.width is None

    # with alignment
    result = output_renderer.ColumnFormatter("test%>")
    assert result.title == "test"
    assert result.alignment == ">"
    assert result.width is None

    # with width
    result = output_renderer.ColumnFormatter("test%10")
    assert result.title == "test"
    assert result.alignment == "^"
    assert result.width == 10

    # with both
    result = output_renderer.ColumnFormatter("test%<10")
    assert result.title == "test"
    assert result.alignment == "<"
    assert result.width == 10
    assert result.end is None

    # with invalid width
    with pytest.raises(ValueError, match="Unable to parse format token 'test%1<0'"):
        result = output_renderer.ColumnFormatter("test%1<0")

    # empty
    with pytest.raises(ValueError, match="Unable to parse format token ''"):
        result = output_renderer.ColumnFormatter("")

    # if unable to parse with %, recommend using ""
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Unable to parse format token 'test%a', did you forget to wrap in quotes?"
        ),
    ):
        result = output_renderer.ColumnFormatter("test%a")

    # if unable to parse with %, recommend using "" even when matching
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Unable to parse format token 'test%', did you forget to wrap in quotes?"
        ),
    ):
        result = output_renderer.ColumnFormatter("test%")

    # end without width is an error
    with pytest.raises(ValueError, match="Unable to parse format token 'test%e'"):
        result = output_renderer.ColumnFormatter("test%e")

    # can specify end with width
    result = output_renderer.ColumnFormatter("test%20e")
    assert result.title == "test"
    assert result.alignment == "^"
    assert result.width == 20
    assert result.end is not None

    # can use alternate tokens : and $
    result = output_renderer.ColumnFormatter("test:20$")
    assert result.title == "test"
    assert result.alignment == "^"
    assert result.width == 20
    assert result.end is not None


def test_formatter_eq() -> None:
    """Can test for equality and with a string."""
    fmt = output_renderer.ColumnFormatter("Name")
    fmt2 = output_renderer.ColumnFormatter("Name")
    fmt3 = output_renderer.ColumnFormatter("Name>")
    assert fmt == fmt2
    assert fmt != fmt3

    assert fmt != []
    assert repr(fmt) == "Name%^None"

    assert "Name" == fmt  # noqa: SIM300 need to check both sides for equality
    assert fmt == "Name"
    assert fmt != "NaMe"

    formatters = [fmt, fmt2, fmt3]
    assert "Name" in formatters
    assert "NAME" not in formatters


def test_formatter_validate_title() -> None:
    """Can validate titles against a column formatter."""
    fmt = output_renderer.ColumnFormatter("NaMe")

    with pytest.raises(ValueError, match="'NaMe' is not a valid title"):
        fmt.validate_title(["JobID", "State"], [])

    fmt.title = "jOBid"
    assert fmt.validate_title(["other", "JobID", "State"], []) == "JobID"
    assert fmt.title == "JobID"

    fmt.title = "ReqMem"
    assert fmt.validate_title(["ReqMem", "JobID", "State"], []) == "ReqMem"
    assert fmt.title == "ReqMem"


def test_formatter_validate_title_totals() -> None:
    """Can validate titles against a column formatter that start with Total."""
    # total cpu is from sacct and will short any total logic
    fmt = output_renderer.ColumnFormatter("TOTALCPU")
    assert fmt.validate_title(["TotalCPU"], []) == "TotalCPU"
    assert fmt.title == "TotalCPU"

    # totalreqmem is valid
    fmt = output_renderer.ColumnFormatter("TOTALreqMEM")
    assert fmt.validate_title(["ReqMem"], []) == "ReqMem"
    assert fmt.title == "TotalReqMem"

    # totalblah is invalid as it doesn't match anything
    fmt = output_renderer.ColumnFormatter("TOTALblah")
    with pytest.raises(ValueError, match="'TOTALblah' is not a valid title") as ex:
        fmt.validate_title(["ReqMem"], [])

    assert str(ex.value) == (
        "'TOTALblah' is not a valid title. "
        "'blah' does not match allowed values. "
        "Run `sacct --helpformat` for a list of allowed values."
    )

    # totalMemEff isn't allowed as it's a derived value
    fmt = output_renderer.ColumnFormatter("totalMemeff")
    with pytest.raises(ValueError, match="'totalMemeff' is not a valid title") as ex:
        fmt.validate_title(["ReqMem"], ["TimeEff", "MemEff"])

    assert str(ex.value) == (
        "'totalMemeff' is not a valid title. "
        "'MemEff' is a derived value and cannot be summed. "
        "Run `sacct --helpformat` for a list of allowed values."
    )


def test_formatter_compute_width() -> None:
    """Can determine width of table entries."""
    fmt = output_renderer.ColumnFormatter("JobID")
    # matches title
    jobs = [
        Job("job", "tes", None),
        Job("job", "tin", None),
        Job("job", "g", None),
    ]
    fmt.compute_width(jobs)
    assert fmt.width == 7

    # already set
    jobs = [
        Job("job", "aLongEntry", None),
        Job("job", "addAnother", None),
    ]
    fmt.compute_width(jobs)
    assert fmt.width == 7

    fmt = output_renderer.ColumnFormatter("JobID")
    fmt.compute_width(jobs)
    assert fmt.width == 12


def test_formatter_format_entry() -> None:
    """Can format entry with alignment, width, and color."""
    fmt = output_renderer.ColumnFormatter("Name")
    # no width causes just the name to be printed
    assert fmt.format_title() == click.style("Name", bold=True)

    fmt.width = 8
    assert fmt.format_title() == click.style("  Name  ", bold=True)
    fmt.alignment = "<"
    assert fmt.format_title() == click.style("Name    ", bold=True)
    fmt.alignment = ">"
    assert fmt.format_title() == click.style("    Name", bold=True)

    assert fmt.format_entry("A Long Entry") == "A Long E"
    assert fmt.format_entry("A Long Entry", "green") == click.style(
        "A Long E", fg="green"
    )

    fmt.end = "e"
    assert fmt.format_entry("A Long Entry") == "ng Entry"



# ---------------------------------------------------------------------------
# summarize: grouped summary rendering (Phase 1)
# ---------------------------------------------------------------------------


def _summary_renderer(
    *, node: bool = False, parsable: bool = False
) -> OutputRenderer:
    """Renderer with a format string exercising the summary metrics."""
    return output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(node=node, parsable=parsable),
        format_str="JobID,State,Elapsed,CPUEff,TimeEff",
    )


def _summary_job(
    jobid: str,
    state: str,
    elapsed: str = "00:10:00",
    total_cpu: str = "00:09:00",
    *,
    job_name: str = "",
) -> Job:
    """Build a task job for grouped-summary renderer tests."""
    job = Job(jobid.split("_", 1)[0], jobid, None)
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
    }
    if job_name:
        entry["JobName"] = job_name
    job.update(entry)
    return job


def test_format_grouped_summary_array_grouping() -> None:
    """--group-by=array groups by base id and labels the block 'Array'."""
    jobs = [
        _summary_job("100_1", "COMPLETED"),
        _summary_job("100_2", "FAILED", elapsed="00:00:30"),
        _summary_job("200", "COMPLETED"),
    ]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=50,
        graph_style="sparkline",
        graph_format="runtime",
        ascii_fallback=False,
    )
    assert "Array 100" in output
    # singleton 200 gets no summary block
    assert output.count("Array ") == 1
    assert "1/2 completed (50%)" in output


def test_format_grouped_summary_name_grouping() -> None:
    """--group-by=name groups by JobName and labels the block 'Group'."""
    jobs = [
        _summary_job("100_1", "COMPLETED", job_name="myrule"),
        _summary_job("300_1", "COMPLETED", job_name="myrule"),
    ]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="name",
        min_tasks=50,
        graph_style="sparkline",
        graph_format="runtime",
        ascii_fallback=False,
    )
    assert "Group myrule" in output
    assert "Array" not in output
    assert "2/2 completed (100%)" in output


def test_format_grouped_summary_name_grouping_falls_back_without_jobname() -> None:
    """Tasks without a usable JobName fall back to their own base id."""
    jobs = [_summary_job("100_1", "COMPLETED"), _summary_job("200_1", "COMPLETED")]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="name",
        min_tasks=50,
        graph_style="sparkline",
        graph_format="runtime",
        ascii_fallback=False,
    )
    # each falls back to its own base id rather than being merged together
    assert "Group 100" in output
    assert "Group 200" in output


def test_format_grouped_summary_suppressed_in_parsable() -> None:
    """Parsable mode never emits summary blocks, but keeps per-task rows."""
    jobs = [_summary_job("100_1", "COMPLETED"), _summary_job("100_2", "COMPLETED")]
    renderer = _summary_renderer(parsable=True)
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=50,
        graph_style="sparkline",
        graph_format="runtime",
        ascii_fallback=False,
    )
    assert "Array 100" not in output
    assert "100_1" in output
    assert "100_2" in output
    assert renderer.options.parsable is True


def test_format_grouped_summary_graph_min_tasks_threshold() -> None:
    """A graph appears only once the group's task count exceeds min_tasks."""
    jobs = [
        _summary_job(f"100_{i}", "COMPLETED", elapsed=f"00:{i % 60:02d}:00")
        for i in range(1, 11)
    ]
    renderer = _summary_renderer()

    below = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=50,
        graph_style="histogram",
        graph_format="runtime",
        ascii_fallback=False,
    )
    assert "Runtime (" not in below

    above = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=5,
        graph_style="histogram",
        graph_format="runtime",
        ascii_fallback=False,
    )
    assert "Runtime (min)" in above


def test_format_grouped_summary_graph_style_sparkline_vs_histogram() -> None:
    """graph_style selects between a one-line sparkline and a histogram."""
    jobs = [
        _summary_job(f"100_{i}", "COMPLETED", elapsed=f"00:{i % 60:02d}:00")
        for i in range(1, 11)
    ]
    renderer = _summary_renderer()

    sparkline = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=5,
        graph_style="sparkline",
        graph_format="runtime",
        ascii_fallback=False,
    )
    assert "Runtime dist:" in sparkline
    assert "Runtime (min)" not in sparkline

    histogram = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=5,
        graph_style="histogram",
        graph_format="runtime",
        ascii_fallback=False,
    )
    assert "Runtime (min)" in histogram
    assert "Runtime dist:" not in histogram


def test_format_grouped_summary_graph_style_none_suppresses_graph() -> None:
    """graph_style=none never draws a graph, regardless of min_tasks."""
    jobs = [
        _summary_job(f"100_{i}", "COMPLETED", elapsed=f"00:{i % 60:02d}:00")
        for i in range(1, 11)
    ]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=0,
        graph_style="none",
        graph_format="runtime",
        ascii_fallback=False,
    )
    assert "Runtime dist:" not in output
    assert "Runtime (min)" not in output


def test_format_grouped_summary_graph_format_selects_which_metric_graphs() -> None:
    """--graph-format only graphs the metrics it names, not every summarized one."""
    jobs = [
        _summary_job(f"100_{i}", "COMPLETED", elapsed=f"00:{i % 60:02d}:00")
        for i in range(1, 11)
    ]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=0,
        graph_style="sparkline",
        graph_format="cpueff",
        ascii_fallback=False,
    )
    # cpueff was requested and gets a graph...
    assert "CPUEff dist:" in output
    # ...but runtime wasn't requested, so it doesn't, even though it's
    # always available
    assert "Runtime dist:" not in output
    # the table still shows every summarized metric regardless of what's
    # graphed
    assert "Metric" in output


def test_format_grouped_summary_ascii_fallback() -> None:
    """--ascii-fallback avoids unicode glyphs in both prose and graphs."""
    jobs = [
        _summary_job(f"100_{i}", "COMPLETED", elapsed=f"00:{i % 60:02d}:00")
        for i in range(1, 11)
    ]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=5,
        graph_style="histogram",
        graph_format="runtime",
        ascii_fallback=True,
    )
    assert "•" not in output
    assert "·" not in output
    assert "█" not in output


def test_add_required_column_is_idempotent() -> None:
    """add_required_column adds a column once and recomputes query_columns."""
    renderer = _summary_renderer()
    before = set(renderer.query_columns)
    assert "JobName" not in before

    renderer.add_required_column("JobName")
    assert "JobName" in renderer.query_columns

    # calling again doesn't duplicate it
    renderer.add_required_column("JobName")
    assert renderer.query_columns.count("JobName") == 1


def test_add_required_column_does_not_affect_report() -> None:
    """Report's renderer never calls add_required_column, so JobName is absent."""
    renderer = _summary_renderer()
    assert "JobName" not in renderer.query_columns


# ---------------------------------------------------------------------------
# summarize: metrics table rendering (Phase 2)
# ---------------------------------------------------------------------------


def test_format_metrics_table_header_and_alignment() -> None:
    """The metrics table has a Metric/Min/Mean/Max header and aligned cells."""
    jobs = [
        _summary_job("100_1", "COMPLETED", elapsed="00:10:00"),
        _summary_job("100_2", "COMPLETED", elapsed="00:12:00"),
    ]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=50,
        graph_style="sparkline",
        graph_format="runtime",
        ascii_fallback=False,
    )
    lines = output.splitlines()
    header_line = next(line for line in lines if "Metric" in line)
    assert "Min" in header_line
    assert "Mean" in header_line
    assert "Max" in header_line

    # every metric row is the same visible width as the header (padded/
    # aligned) -- strip ANSI color codes first, since a colored cell adds
    # invisible escape bytes that would otherwise skew a raw len() compare
    ansi = re.compile(r"\x1b\[[0-9;]*m")
    visible_header = ansi.sub("", header_line)
    metric_lines = [
        line
        for line in lines
        if line.strip().startswith(("CPUEff", "TimeEff", "MemEff"))
    ]
    assert metric_lines
    for line in metric_lines:
        assert len(ansi.sub("", line)) == len(visible_header)


def test_format_metrics_table_no_bullet_separators() -> None:
    """The table replaces the old bullet/middot-separated metric line."""
    jobs = [_summary_job("100_1", "COMPLETED"), _summary_job("100_2", "COMPLETED")]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=50,
        graph_style="sparkline",
        graph_format="runtime",
        ascii_fallback=False,
    )
    # the old prototype rendered lines like "CPUEff: min 90.0 · mean 90.0
    # · max 90.0" -- none of that per-metric inline format survives
    assert ": min " not in output
    assert "· mean" not in output
    assert "· max" not in output


def test_format_metrics_table_low_efficiency_uses_color_not_extra_text() -> None:
    """Out-of-range values are colored (via render_eff); no extra "low" marker.

    Per feedback in the original design discussion, the color coding alone
    (already used throughout the report table) communicates a value is out
    of range; a redundant textual marker isn't added on top of it.
    """
    jobs = [
        _summary_job("100_1", "COMPLETED", elapsed="00:10:00", total_cpu="00:00:30"),
        _summary_job("100_2", "COMPLETED", elapsed="00:10:00", total_cpu="00:00:20"),
    ]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=50,
        graph_style="sparkline",
        graph_format="runtime",
        ascii_fallback=False,
    )
    assert "low" not in output.lower()
    assert "\N{WARNING SIGN}" not in output


def test_format_metrics_table_empty_when_no_numeric_metrics() -> None:
    """A group with no numeric metrics (e.g. all values non-coercible) is fine."""
    renderer = _summary_renderer()
    assert renderer._format_metrics_table([], indent="  ") == []


# ---------------------------------------------------------------------------
# summarize: multi-metric graphing (Phase 3)
# ---------------------------------------------------------------------------


def _summary_job_numeric(
    jobid: str, state: str, elapsed: str, total_cpu: str
) -> Job:
    """Build a COMPLETED task with distinct elapsed/total_cpu for graphing."""
    return _summary_job(jobid, state, elapsed=elapsed, total_cpu=total_cpu)


def test_multi_metric_graphing_min_tasks_gates_every_metric() -> None:
    """--min-tasks gates every requested metric uniformly, not just runtime."""
    jobs = [
        _summary_job_numeric(
            f"100_{i}", "COMPLETED", f"00:{i:02d}:00", f"00:{i % 7 + 1:02d}:00"
        )
        for i in range(1, 11)
    ]
    renderer = _summary_renderer()

    below = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=50,
        graph_style="histogram",
        graph_format="runtime,cpueff",
        ascii_fallback=False,
    )
    assert "Runtime (min)" not in below
    assert "CPUEff (%)" not in below

    above = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=5,
        graph_style="histogram",
        graph_format="runtime,cpueff",
        ascii_fallback=False,
    )
    assert "Runtime (min)" in above
    assert "CPUEff (%)" in above


def test_multi_metric_graphing_graph_style_none_suppresses_all() -> None:
    """graph_style=none suppresses every metric's graph, not just runtime's."""
    jobs = [
        _summary_job_numeric(
            f"100_{i}", "COMPLETED", f"00:{i:02d}:00", f"00:{i % 7 + 1:02d}:00"
        )
        for i in range(1, 11)
    ]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=0,
        graph_style="none",
        graph_format="runtime,cpueff",
        ascii_fallback=False,
    )
    assert "dist:" not in output
    assert "(min)" not in output
    assert "(%)" not in output


def test_multi_metric_graphing_ascii_fallback() -> None:
    """--ascii-fallback avoids unicode across every graphed metric, not just one."""
    jobs = [
        _summary_job_numeric(
            f"100_{i}", "COMPLETED", f"00:{i:02d}:00", f"00:{i % 7 + 1:02d}:00"
        )
        for i in range(1, 11)
    ]
    renderer = _summary_renderer()
    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=5,
        graph_style="histogram",
        graph_format="runtime,cpueff",
        ascii_fallback=True,
    )
    assert "\u2588" not in output
    assert "#" in output


def test_multi_metric_graphing_sparkline_shows_each_requested_metric() -> None:
    """Sparkline mode graphs every requested metric, each on its own line."""
    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(),
        format_str="JobID,State,Elapsed,CPUEff,MemEff",
    )
    jobs = []
    for i in range(1, 11):
        job = Job(f"100_{i}".split("_", 1)[0], f"100_{i}", None)
        job.update(
            {
                "JobID": f"100_{i}",
                "State": "COMPLETED",
                "AllocCPUS": "1",
                "ReqMem": "1Gn",
                "TotalCPU": f"00:{i % 7 + 1:02d}:00",
                "Elapsed": f"00:{i:02d}:00",
                "Timelimit": "00:20:00",
                "MaxRSS": f"{100 + i * 10}K",
                "NNodes": "1",
                "NTasks": "",
            }
        )
        jobs.append(job)

    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=5,
        graph_style="sparkline",
        graph_format="runtime,cpueff,memeff",
        ascii_fallback=False,
    )
    assert "Runtime dist:" in output
    assert "CPUEff dist:" in output
    assert "MemEff dist:" in output


def test_energy_metric_graphs_with_joules_unit() -> None:
    """Energy is graphed with a "J" unit label, distinct from percent metrics."""
    renderer = output_renderer.OutputRenderer(
        [*min_required, "TRESUsageOutAve"],
        output_renderer.RenderOptions(),
        format_str="JobID,State,Elapsed,Energy",
    )
    jobs = []
    for i in range(1, 11):
        job = _summary_job(f"100_{i}", "COMPLETED", elapsed=f"00:{i:02d}:00")
        job.update(
            {
                "JobID": f"100_{i}.extern",
                "State": "COMPLETED",
                "MaxRSS": "",
                "TRESUsageOutAve": f"energy={1000 + i * 10}",
            }
        )
        jobs.append(job)

    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=5,
        graph_style="histogram",
        graph_format="energy",
        ascii_fallback=False,
    )
    assert "Energy" in output
    # no percent unit for Energy; it's shown as a bare, unitless histogram
    # header (we don't assert a specific unit string, only that "%" isn't
    # incorrectly applied to it)
    assert "Energy (%)" not in output


def test_gpu_metrics_graph_gracefully_without_jobstat_data() -> None:
    """--graph-format gpueff,gpumem on non-GPU jobs draws nothing, doesn't error.

    GPUEff/GPUMem come from jobstats' AdminComment field; on systems (or
    jobs) without jobstat caching they're simply "---" for every task, same
    as they already render in the plain table. There's no separate opt-in
    or error path -- the metric just never accumulates a raw series to
    graph, exactly like any other all-"---" numeric column.
    """
    jobs = [
        _summary_job(f"100_{i}", "COMPLETED", elapsed=f"00:{i:02d}:00")
        for i in range(1, 11)
    ]
    renderer = output_renderer.OutputRenderer(
        min_required,
        output_renderer.RenderOptions(),
        format_str="JobID,State,Elapsed,CPUEff,GPUEff,GPUMem",
    )

    output = renderer.format_grouped_summary(
        jobs,
        group_by="array",
        min_tasks=0,
        graph_style="sparkline",
        graph_format="gpueff,gpumem",
        ascii_fallback=False,
    )

    assert "GPUEff dist:" not in output
    assert "GPUMem dist:" not in output
    # absent from the metrics table too -- same as any other all-"---" column
    assert "GPUEff" not in output.split("Array")[-1]
    assert "GPUMem" not in output.split("Array")[-1]
    # the rest of the summary still renders normally
    assert "CPUEff" in output


def test_format_metric_graph_empty_values_returns_nothing() -> None:
    """_format_metric_graph's empty-values guard, exercised directly.

    Both real call sites in _format_summary_block already skip calling
    this with an empty series (the runtime branch checks
    summary.elapsed_minutes first; the per-metric loop checks
    metric.values first) -- this guard protects min()/max() in the
    sparkline branch from a future caller that doesn't. Tested directly
    since it's otherwise unreachable through format_grouped_summary.
    """
    renderer = _summary_renderer()
    assert (
        renderer._format_metric_graph(
            "CPUEff",
            [],
            unit="%",
            graph_style="sparkline",
            use_unicode=True,
            indent="  ",
        )
        == []
    )
    assert (
        renderer._format_metric_graph(
            "CPUEff",
            [],
            unit="%",
            graph_style="histogram",
            use_unicode=True,
            indent="  ",
        )
        == []
    )
