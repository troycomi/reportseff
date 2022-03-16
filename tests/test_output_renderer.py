"""Test operation of output renderer object."""
import re

import click
import pytest

from reportseff import job as job_module
from reportseff import output_renderer


min_required = (
    "JobID,State,Elapsed,JobIDRaw,State,TotalCPU,AllocCPUS,"
    "REQMEM,NNodes,MaxRSS,Timelimit"
).split(",")


@pytest.fixture
def renderer():
    """Default renderer with valid names for only default string."""
    return output_renderer.OutputRenderer(min_required)


@pytest.fixture
def some_jobs():
    """A few test jobs for generating an output table."""
    jobs = []
    job = job_module.Job("24371655", "24371655", None)
    job.update(
        {
            "JobID": "24371655",
            "State": "COMPLETED",
            "AllocCPUS": "1",
            "REQMEM": "1Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:10:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = job_module.Job("24371656", "24371656", None)
    job.update(
        {
            "JobID": "24371656",
            "State": "PENDING",
            "AllocCPUS": "1",
            "REQMEM": "1Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:10:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = job_module.Job("24371657", "24371657", None)
    job.update(
        {
            "JobID": "24371657",
            "State": "RUNNING",
            "AllocCPUS": "1",
            "REQMEM": "1Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:10:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = job_module.Job("24371658", "24371658", None)
    job.update(
        {
            "JobID": "24371658",
            "State": "CANCELLED",
            "AllocCPUS": "1",
            "REQMEM": "1Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:00:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = job_module.Job("24371659", "24371659", None)
    job.update(
        {
            "JobID": "24371659",
            "State": "TIMEOUT",
            "AllocCPUS": "1",
            "REQMEM": "2Gn",
            "TotalCPU": "00:04:00",
            "Elapsed": "00:21:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    jobs.append(job)
    job = job_module.Job("24371660", "24371660", None)
    job.update(
        {
            "JobID": "24371660",
            "State": "OTHER",
            "AllocCPUS": "1",
            "REQMEM": "2Gn",
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
def some_multi_core_jobs(
    single_core, multi_node, single_gpu, multi_gpu, multi_node_multi_gpu, short_job
):
    """A collection of jobs with multiple cores/gpus."""
    jobs = []

    job = job_module.Job("8205464", "8205464", None)
    for line in short_job:
        job.update(line)
    jobs.append(job)

    job = job_module.Job("8189521", "8189521", None)
    for line in multi_node_multi_gpu:
        job.update(line)
    jobs.append(job)

    job = job_module.Job("8189521", "8189521", None)
    for line in multi_gpu:
        job.update(line)
    jobs.append(job)

    job = job_module.Job("8197399", "8197399", None)
    for line in single_gpu:
        job.update(line)
    jobs.append(job)

    job = job_module.Job("8205048", "8205048", None)
    for line in multi_node:
        job.update(line)
    jobs.append(job)

    job = job_module.Job("39895850", "39889258_1426", None)
    for line in single_core:
        job.update(line)
    jobs.append(job)

    return jobs


def test_renderer_init(renderer):
    """Initialized renderer produces correct columns."""
    assert renderer.formatters == [
        output_renderer.ColumnFormatter("JobID%>"),
        output_renderer.ColumnFormatter("State"),
        output_renderer.ColumnFormatter("Elapsed%>"),
        output_renderer.ColumnFormatter("CPUEff"),
        output_renderer.ColumnFormatter("MemEff"),
    ]
    assert sorted(renderer.query_columns) == sorted(
        (
            "JobID JobIDRaw State Elapsed TotalCPU "
            "AllocCPUS REQMEM NNodes MaxRSS AdminComment"
        ).split()
    )

    renderer = output_renderer.OutputRenderer(min_required, "")
    assert renderer.formatters == []
    assert sorted(renderer.query_columns) == sorted(
        ("JobID JobIDRaw State AdminComment").split()
    )

    renderer = output_renderer.OutputRenderer(min_required, "TotalCPU%<5")
    assert renderer.formatters == [output_renderer.ColumnFormatter("TotalCPU%<5")]
    assert sorted(renderer.query_columns) == sorted(
        ("JobID JobIDRaw State TotalCPU AdminComment").split()
    )


def test_renderer_build_formatters():
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


def test_renderer_validate_formatters(renderer):
    """Can validate formatters as members of a provided collection, normalizing name."""
    renderer.formatters = output_renderer.build_formatters("JobID,JOBid,jObId")
    assert renderer.validate_formatters(["JobID"]) == "JobID JobID JobID".split()
    assert renderer.formatters == "JobID JobID JobID".split()

    renderer.formatters = output_renderer.build_formatters("JobID,GPU%>10")
    assert (
        renderer.validate_formatters(["JobID", "GPU", "GPUEff", "GPUMem"])
        == "JobID GPU".split()
    )
    assert renderer.formatters == "JobID GPUEff GPUMem".split()
    # other params are copied from GPU to GPUEff and GPUMem
    assert renderer.formatters[1].alignment == ">"
    assert renderer.formatters[2].alignment == ">"
    assert renderer.formatters[1].width == 10
    assert renderer.formatters[2].width == 10


def test_renderer_validate_formatters_with_node(renderer):
    """Validating formatters with GPUs can alter formatters."""
    min_gpu = min_required + ["GPU", "GPUEff", "GPUMem"]
    # normal function
    renderer.node = False
    renderer.gpu = False
    renderer.formatters = output_renderer.build_formatters("State")
    assert renderer.validate_formatters(min_gpu) == ["State"]
    assert renderer.formatters == ["State"]

    # add in job id
    renderer.node = True
    renderer.gpu = False
    renderer.formatters = output_renderer.build_formatters("State")
    assert renderer.validate_formatters(min_gpu) == ["State"]
    assert renderer.formatters == ["JobID", "State"]

    # add in both gpus, gpu implies node
    renderer.node = True
    renderer.gpu = True
    renderer.formatters = output_renderer.build_formatters("State")
    assert renderer.validate_formatters(min_gpu) == ["State"]
    assert renderer.formatters == ["JobID", "State", "GPUEff", "GPUMem"]
    assert renderer.formatters[0].alignment == "<"  # switched by node reporting

    # since format alread has jobid and gpumem, will not override
    renderer.node = True
    renderer.gpu = True
    renderer.formatters = output_renderer.build_formatters("GPUMEM,State,JobID:>")
    assert renderer.validate_formatters(min_gpu) == "GPUMem State JobID".split()
    assert renderer.formatters == ["GPUMem", "State", "JobID"]
    assert renderer.formatters[2].alignment == "<"  # switched by node reporting


def test_renderer_correct_columns(renderer):
    """Corrected columns include required entries and derived values."""
    renderer.query_columns = ["JobID"]
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == sorted(
        "JobID JobIDRaw State AdminComment".split()
    )

    renderer.query_columns = "JobID CPUEff MemEff TimeEff".split()
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == sorted(
        (
            "JobID TotalCPU Elapsed REQMEM"
            " JobIDRaw State AdminComment"
            " NNodes AllocCPUS MaxRSS Timelimit"
        ).split()
    )

    renderer.query_columns = "JobID JobID JobID".split()
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == sorted(
        "JobID JobIDRaw State AdminComment".split()
    )


def test_renderer_format_jobs(some_jobs):
    """Can render output as table with colored entries."""
    renderer = output_renderer.OutputRenderer(
        min_required, "JobID,State,Elapsed,CPUEff,REQMEM,TimeEff"
    )
    result = renderer.format_jobs(some_jobs)
    ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
    # check removed codes
    codes = ansi_escape.findall(result)
    for code in codes[1::2]:  # normal
        assert code == "\x1b[0m"
    for code in codes[0:10:20]:
        assert code == "\x1b[1m"  # bold
    for i in (22, 24, 26, 28):
        assert codes[i] == "\x1b[31m"  # red
    for i in (12, 14, 30):
        assert codes[i] == "\x1b[32m"  # green
    for i in (20,):
        assert codes[i] == "\x1b[33m"  # yellow
    for i in (16,):
        assert codes[i] == "\x1b[34m"  # blue
    for i in (18,):
        assert codes[i] == "\x1b[36m"  # cyan
    # remove color codes
    result = ansi_escape.sub("", result)
    lines = result.split("\n")
    assert lines[0].split() == "JobID State Elapsed CPUEff REQMEM TimeEff".split()
    assert lines[1].split() == "24371655 COMPLETED 00:10:00 90.0% 1Gn 50.0%".split()
    assert lines[2].split() == "24371656 PENDING --- --- --- ---".split()
    assert lines[3].split() == "24371657 RUNNING 00:10:00 --- 1Gn 50.0%".split()
    assert lines[4].split() == "24371658 CANCELLED 00:00:00 --- 1Gn 0.0%".split()
    assert lines[5].split() == "24371659 TIMEOUT 00:21:00 19.0% 2Gn 105.0%".split()
    assert lines[6].split() == "24371660 OTHER 00:12:05 74.5% 2Gn 60.4%".split()


def test_renderer_format_jobs_multi_node(some_multi_core_jobs):
    """Can render output as table with colored entries."""
    renderer = output_renderer.OutputRenderer(
        min_required,
        "JobID,State,CPUEff,TimeEff,MemEff,GPU",
    )
    result = renderer.format_jobs(some_multi_core_jobs)
    ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
    result = ansi_escape.sub("", result)
    lines = result.split("\n")
    assert lines[0].split() == "JobID State CPUEff TimeEff MemEff GPUEff GPUMem".split()
    assert lines[1].split() == "8205464 FAILED 6.2% 0.0% 0.0% --- ---".split()
    assert lines[2].split() == "8189521 CANCELLED 10.5% 83.0% 26.0% 5.5% 30.1%".split()
    assert lines[3].split() == "8189521 CANCELLED 10.5% 83.0% 26.3% 3.5% 30.1%".split()
    assert lines[4].split() == "8197399 COMPLETED 95.4% 21.1% 9.5% 29.4% 99.8%".split()
    assert lines[5].split() == "8205048 COMPLETED 4.6% 4.1% 1.1% --- ---".split()
    assert (
        lines[6].split() == "39889258_1426 COMPLETED 99.7% 76.7% 3.6% --- ---".split()
    )


def test_renderer_format_jobs_multi_node_with_nodes(some_multi_core_jobs):
    """Can render output as table with colored entries."""
    renderer = output_renderer.OutputRenderer(
        min_required,
        "JobID,State,CPUEff,TimeEff,MemEff,GPU",
        node=True,
    )
    result = renderer.format_jobs(some_multi_core_jobs)
    ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
    result = ansi_escape.sub("", result)
    lines = result.split("\n")
    assert lines[0].split() == "JobID State CPUEff TimeEff MemEff GPUEff GPUMem".split()
    assert lines[1].split() == "8205464 FAILED 6.2% 0.0% 0.0% --- ---".split()
    assert lines[2].split() == "8189521 CANCELLED 10.5% 83.0% 26.0% 5.5% 30.1%".split()
    assert lines[3].split() == "tiger-i19g10 10.5% 25.8% 7.5% 30.1%".split()
    assert lines[4].split() == "tiger-i19g9 10.5% 26.3% 3.5% 30.1%".split()
    assert lines[5].split() == "8189521 CANCELLED 10.5% 83.0% 26.3% 3.5% 30.1%".split()
    assert lines[6].split() == "8197399 COMPLETED 95.4% 21.1% 9.5% 29.4% 99.8%".split()
    assert lines[7].split() == "8205048 COMPLETED 4.6% 4.1% 1.1% --- ---".split()
    assert lines[8].split() == "tiger-h19c1n15 18.6% 4.5%".split()
    assert lines[9].split() == "tiger-h26c2n13 0.0% 0.0%".split()
    assert lines[10].split() == "tiger-i26c2n11 0.0% 0.0%".split()
    assert lines[11].split() == "tiger-i26c2n15 0.0% 0.0%".split()
    assert (
        lines[12].split() == "39889258_1426 COMPLETED 99.7% 76.7% 3.6% --- ---".split()
    )


def test_renderer_format_jobs_multi_node_with_nodes_and_gpu(some_multi_core_jobs):
    """Can render output as table with colored entries."""
    renderer = output_renderer.OutputRenderer(
        min_required, "JobID,State,CPUEff,TimeEff,MemEff,GPU", node=True, gpu=True
    )
    result = renderer.format_jobs(some_multi_core_jobs)
    ansi_escape = re.compile(r"\x1B[@-_][0-?]*[ -/]*[@-~]")
    result = ansi_escape.sub("", result)
    lines = result.split("\n")
    assert lines[0].split() == "JobID State CPUEff TimeEff MemEff GPUEff GPUMem".split()
    assert lines[1].split() == "8205464 FAILED 6.2% 0.0% 0.0% --- ---".split()
    assert lines[2].split() == "8189521 CANCELLED 10.5% 83.0% 26.0% 5.5% 30.1%".split()
    assert lines[3].split() == "tiger-i19g10 10.5% 25.8% 7.5% 30.1%".split()
    assert lines[4].split() == "0 7.5% 30.1%".split()
    assert lines[5].split() == "1 7.5% 30.1%".split()
    assert lines[6].split() == "2 7.2% 30.1%".split()
    assert lines[7].split() == "3 7.8% 30.1%".split()
    assert lines[8].split() == "tiger-i19g9 10.5% 26.3% 3.5% 30.1%".split()
    assert lines[9].split() == "0 3.5% 30.1%".split()
    assert lines[10].split() == "1 3.5% 30.1%".split()
    assert lines[11].split() == "2 3.2% 30.1%".split()
    assert lines[12].split() == "3 3.8% 30.1%".split()
    assert lines[13].split() == "8189521 CANCELLED 10.5% 83.0% 26.3% 3.5% 30.1%".split()
    assert lines[14].split() == "tiger-i19g9 10.5% 26.3% 3.5% 30.1%".split()
    assert lines[15].split() == "0 3.5% 30.1%".split()
    assert lines[16].split() == "1 3.5% 30.1%".split()
    assert lines[17].split() == "2 3.2% 30.1%".split()
    assert lines[18].split() == "3 3.8% 30.1%".split()
    assert lines[19].split() == "8197399 COMPLETED 95.4% 21.1% 9.5% 29.4% 99.8%".split()
    assert lines[20].split() == "tiger-i23g14 95.4% 9.5% 29.4% 99.8%".split()
    assert lines[21].split() == "3 29.4% 99.8%".split()
    assert lines[22].split() == "8205048 COMPLETED 4.6% 4.1% 1.1% --- ---".split()
    assert lines[23].split() == "tiger-h19c1n15 18.6% 4.5%".split()
    assert lines[24].split() == "tiger-h26c2n13 0.0% 0.0%".split()
    assert lines[25].split() == "tiger-i26c2n11 0.0% 0.0%".split()
    assert lines[26].split() == "tiger-i26c2n15 0.0% 0.0%".split()
    assert (
        lines[27].split() == "39889258_1426 COMPLETED 99.7% 76.7% 3.6% --- ---".split()
    )


def test_format_jobs_empty(some_jobs):
    """Empty format string produces empty outputs."""
    renderer = output_renderer.OutputRenderer(min_required, "")
    result = renderer.format_jobs(some_jobs)
    assert result == ""


def test_format_jobs_single_str(some_jobs):
    """A single format string left aligns and supresses title for piping."""
    renderer = output_renderer.OutputRenderer(min_required, "JobID%>")
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


def test_formatter_init():
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
    with pytest.raises(ValueError) as exception:
        result = output_renderer.ColumnFormatter("test%1<0")
    assert "Unable to parse format token 'test%1<0'" in str(exception)

    # empty
    with pytest.raises(ValueError) as exception:
        result = output_renderer.ColumnFormatter("")
    assert "Unable to parse format token ''" in str(exception)

    # if unable to parse with %, recommend using ""
    with pytest.raises(ValueError) as exception:
        result = output_renderer.ColumnFormatter("test%a")
    assert (
        "Unable to parse format token 'test%a', " "did you forget to wrap in quotes?"
    ) in str(exception)

    # if unable to parse with %, recommend using "" even when matching
    with pytest.raises(ValueError) as exception:
        result = output_renderer.ColumnFormatter("test%")
    assert (
        "Unable to parse format token 'test%', " "did you forget to wrap in quotes?"
    ) in str(exception)

    # end without width is an error
    with pytest.raises(ValueError) as exception:
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


def test_formatter_eq():
    """Can test for equality and with a string."""
    fmt = output_renderer.ColumnFormatter("Name")
    fmt2 = output_renderer.ColumnFormatter("Name")
    fmt3 = output_renderer.ColumnFormatter("Name>")
    assert fmt == fmt2
    assert fmt != fmt3

    assert fmt != list()
    assert repr(fmt) == "Name%^None"

    assert "Name" == fmt
    assert fmt == "Name"
    assert fmt != "NaMe"

    formatters = [fmt, fmt2, fmt3]
    assert "Name" in formatters
    assert "NAME" not in formatters


def test_formatter_validate_title():
    """Can validate titles against a column formatter."""
    fmt = output_renderer.ColumnFormatter("NaMe")

    with pytest.raises(ValueError) as exception:
        fmt.validate_title(["JobID", "State"])
    assert "'NaMe' is not a valid title" in str(exception)

    fmt.title = "jOBid"
    assert fmt.validate_title(["other", "JobID", "State"]) == "JobID"
    assert fmt.title == "JobID"


def test_formatter_compute_width():
    """Can determine width of table entries."""
    fmt = output_renderer.ColumnFormatter("JobID")
    # matches title
    jobs = [
        job_module.Job("job", "tes", None),
        job_module.Job("job", "tin", None),
        job_module.Job("job", "g", None),
    ]
    fmt.compute_width(jobs)
    assert fmt.width == 7

    # already set
    jobs = [
        job_module.Job("job", "aLongEntry", None),
        job_module.Job("job", "addAnother", None),
    ]
    fmt.compute_width(jobs)
    assert fmt.width == 7

    fmt = output_renderer.ColumnFormatter("JobID")
    fmt.compute_width(jobs)
    assert fmt.width == 12


def test_formatter_format_entry():
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
