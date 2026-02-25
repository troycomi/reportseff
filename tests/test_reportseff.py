"""Test cli usage."""

from __future__ import annotations

import shlex
import subprocess
from typing import TYPE_CHECKING, Any

import pytest
from click.testing import CliRunner

from reportseff import console
from reportseff.db_inquirer import SacctInquirer
from reportseff.job_collection import JobCollection
from reportseff.output_renderer import OutputRenderer

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from .typings import strip_js_func


@pytest.fixture
def _mock_inquirer(mocker: MockerFixture) -> None:
    """Override valid formats to prevent calls to shell."""

    def mock_valid(_self: SacctInquirer) -> list[str]:
        return [
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
            "MaxDiskReadNode",
        ]

    def mock_partition_timelimits(_self: SacctInquirer) -> dict[str, str]:
        return {}

    mocker.patch.object(SacctInquirer, "get_valid_formats", new=mock_valid)
    mocker.patch.object(
        SacctInquirer, "get_partition_timelimits", new=mock_partition_timelimits
    )


@pytest.mark.usefixtures("_mock_inquirer")
def test_directory_input(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Able to get jobs from directory calls."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24418435"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)

    def set_jobs(self: JobCollection, _directory: Any) -> None:
        self.set_jobs(("24418435",))

    mocker.patch.object(JobCollection, "set_out_dir", new=set_jobs)
    result = runner.invoke(
        console.main,
        ["--no-color", "--slurm-format", "%A.out"],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    assert output[0].split() == [
        "24418435",
        "COMPLETED",
        "01:27:42",
        "48.7%",
        "99.8%",
        "47.6%",
    ]


@pytest.mark.usefixtures("_mock_inquirer")
def test_directory_input_exception(
    mocker: MockerFixture,
    console_jobs: dict[str, str],
) -> None:
    """Catch exceptions in setting jobs from directory."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24418435"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)

    def set_jobs(_self: JobCollection, _directory: Any) -> None:
        msg = "Testing EXCEPTION"
        raise ValueError(msg)

    mocker.patch.object(JobCollection, "set_out_dir", new=set_jobs)
    result = runner.invoke(console.main, ["--no-color"])

    assert result.exit_code == 1
    assert "Testing EXCEPTION" in result.output


@pytest.mark.usefixtures("_mock_inquirer")
def test_debug_option(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Setting debug prints subprocess result."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["23000233"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        ["--no-color", "--debug", "23000233"],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")
    assert output[0] == console_jobs["23000233"].strip("\n")
    assert output[3].split() == [
        "23000233",
        "CANCELLED",
        "00:00:00",
        "0.0%",
        "---",
        "0.0%",
    ]


@pytest.mark.usefixtures("_mock_inquirer")
def test_process_failure(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Catch exceptions in process_entry by printing the offending entry."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["23000233"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    mocker.patch.object(
        JobCollection, "process_entry", side_effect=Exception("TESTING")
    )
    result = runner.invoke(
        console.main,
        ["--no-color", "23000233"],
    )

    assert result.exit_code != 0
    output = result.output.split("\n")
    assert output[0] == (
        "Error processing entry: "
        "{'AdminComment': '', 'AllocCPUS': '16', "
        "'Elapsed': '00:00:00', 'JobID': '23000233', "
        "'JobIDRaw': '23000233', 'MaxRSS': '', 'NNodes': '1', "
        "'NTasks': '1', 'ReqMem': '4000Mc', 'State': 'CANCELLED by 129319', "
        "'Timelimit': '6-00:00:00', 'TotalCPU': '00:00:00'}"
    )


@pytest.mark.usefixtures("_mock_inquirer")
def test_short_output(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Outputs with 20 or fewer entries are directly printed."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["23000233"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    mocker.patch("reportseff.console.len", return_value=20)
    mocker.patch.object(OutputRenderer, "format_jobs", return_value="output")

    mock_click = mocker.patch("reportseff.console.click.echo")
    result = runner.invoke(console.main, ["23000233"])

    assert result.exit_code == 0
    mock_click.assert_called_once_with("output", color=None)


@pytest.mark.usefixtures("_mock_inquirer")
def test_long_output(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Outputs with more than 20 entries are echoed via pager."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["23000233"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    mocker.patch("reportseff.console.len", return_value=21)
    mocker.patch.object(OutputRenderer, "format_jobs", return_value="output")
    mock_click = mocker.patch("reportseff.console.click.echo_via_pager")
    result = runner.invoke(console.main, ["23000233"])

    assert result.exit_code == 0
    mock_click.assert_called_once_with("output", color=None)


@pytest.mark.usefixtures("_mock_inquirer")
def test_simple_job(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can get efficiency from a single job."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24418435_notime"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        ["--no-color", "24418435", "--format", "JobID%>,State,Elapsed%>,CPUEff,MemEff"],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    assert output[0].split() == ["24418435", "COMPLETED", "01:27:42", "99.8%", "47.6%"]


@pytest.mark.usefixtures("_mock_inquirer")
def test_simple_user(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can limit outputs by user."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        console_jobs["24418435_notime"] + console_jobs["25569410_notime"]
    )
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "--user",
            "test",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    assert output[0].split() == ["24418435", "COMPLETED", "01:27:42", "99.8%", "47.6%"]
    assert output[1].split() == ["25569410", "COMPLETED", "21:14:48", "91.7%", "1.5%"]


@pytest.mark.usefixtures("_mock_inquirer")
def test_simple_partition(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can limit outputs by partition and cluster."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        console_jobs["24418435_notime"] + console_jobs["25569410_notime"]
    )
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "--partition",
            "partition",
            "--cluster",
            "cluster",
            "24418435",
            "25569410",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    assert output[0].split() == ["24418435", "COMPLETED", "01:27:42", "99.8%", "47.6%"]
    assert output[1].split() == ["25569410", "COMPLETED", "21:14:48", "91.7%", "1.5%"]


@pytest.mark.usefixtures("_mock_inquirer")
def test_format_add(mocker: MockerFixture) -> None:
    """Can add to format specifier."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    mock_jobs = mocker.patch("reportseff.console.get_jobs", return_value=("Testing", 1))
    result = runner.invoke(console.main, ["--no-color", "--format=test"])

    assert result.exit_code == 0
    assert mock_jobs.call_args[0][0].format_str == "test"

    # test adding onto end
    result = runner.invoke(console.main, ["--no-color", "--format=+test"])

    assert result.exit_code == 0
    assert (
        mock_jobs.call_args[0][0].format_str
        == "JobID%>,State,Elapsed%>,TimeEff,CPUEff,MemEff,test"
    )


@pytest.mark.usefixtures("_mock_inquirer")
def test_since(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can limit outputs by time since argument."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        console_jobs["24418435_notime"] + console_jobs["25569410_notime"]
    )
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "--since",
            "200406",
            "24418435",
            "25569410",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    assert output[0].split() == ["24418435", "COMPLETED", "01:27:42", "99.8%", "47.6%"]
    assert output[1].split() == ["25569410", "COMPLETED", "21:14:48", "91.7%", "1.5%"]


@pytest.mark.usefixtures("_mock_inquirer")
def test_since_all_users(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can limit outputs by time since argument."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        console_jobs["24418435_notime"] + console_jobs["25569410_notime"]
    )
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=sub_result
    )
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "--since",
            "200406",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    assert output[0].split() == ["24418435", "COMPLETED", "01:27:42", "99.8%", "47.6%"]
    assert output[1].split() == ["25569410", "COMPLETED", "21:14:48", "91.7%", "1.5%"]

    mock_sub.assert_called_once_with(
        args=[
            "sacct",
            "--parsable",
            "-n",
            "--delimiter=^|^",
            (
                "--format=AdminComment,AllocCPUS,Elapsed,JobID,JobIDRaw,"
                "MaxRSS,NNodes,NTasks,ReqMem,State,TotalCPU"
            ),
            "--allusers",  # all users is added since no jobs/files were specified
            "--starttime=200406",
        ],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        text=True,
        shell=False,
    )


@pytest.mark.usefixtures("_mock_inquirer")
def test_since_all_users_partition(
    mocker: MockerFixture,
    console_jobs: dict[str, str],
) -> None:
    """Can limit outputs by time since and partition argument."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        console_jobs["24418435_notime"] + console_jobs["25569410_notime"]
    )
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=sub_result
    )
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "--since",
            "200406",
            "--partition=partition",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    assert output[0].split() == ["24418435", "COMPLETED", "01:27:42", "99.8%", "47.6%"]
    assert output[1].split() == ["25569410", "COMPLETED", "21:14:48", "91.7%", "1.5%"]

    mock_sub.assert_called_once_with(
        args=[
            "sacct",
            "--parsable",
            "-n",
            "--delimiter=^|^",
            (
                "--format=AdminComment,AllocCPUS,Elapsed,JobID,"
                "JobIDRaw,MaxRSS,NNodes,NTasks,ReqMem,State,TotalCPU"
            ),
            "--allusers",  # all users is added since no jobs/files were specified
            "--starttime=200406",
            "--partition=partition",
        ],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        text=True,
        shell=False,
    )


@pytest.mark.usefixtures("_mock_inquirer")
def test_parsable(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can display output as parsable format."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24418435_notime"] + console_jobs[
        "25569410_notime"
    ].replace("COMPLETED", "RUNNING")
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--parsable",
            "25569410",
            "24418435",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff%^10,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    # no color/bold codes and ^|^ delimited
    assert output[0].split("|") == ["24418435", "COMPLETED", "01:27:42", "99.8", "47.6"]
    assert output[1].split("|") == ["25569410", "RUNNING", "21:14:48", "---", "---"]


@pytest.mark.usefixtures("_mock_inquirer")
def test_simple_state(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can limit outputs by filtering state."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24418435_notime"] + console_jobs[
        "25569410_notime"
    ].replace("COMPLETED", "RUNNING")
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "--state",
            "completed",
            "25569410",
            "24418435",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    assert output[0].split() == ["24418435", "COMPLETED", "01:27:42", "99.8%", "47.6%"]
    # other is suppressed by state filter
    assert output[1].split() == []


@pytest.mark.usefixtures("_mock_inquirer")
def test_simple_not_state(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can limit outputs by removing state."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24418435_notime"] + console_jobs[
        "25569410_notime"
    ].replace("COMPLETED", "RUNNING")
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "--not-state",
            "Running",
            "25569410",
            "24418435",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:]
    assert output[0].split() == ["24418435", "COMPLETED", "01:27:42", "99.8%", "47.6%"]
    # other is suppressed by state filter
    assert output[1].split() == []


@pytest.mark.usefixtures("_mock_inquirer")
def test_invalid_not_state(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """When not state isn't found, return all jobs."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24418435_notime"] + console_jobs[
        "25569410_notime"
    ].replace("COMPLETED", "RUNNING")
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "--not-state",
            "cunning",
            "25569410",
            "24418435",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")
    assert output[0] == "Unknown state CUNNING"
    assert output[1] == "No valid states provided"
    # output 2 is header
    assert output[3].split() == ["24418435", "COMPLETED", "01:27:42", "99.8%", "47.6%"]
    assert output[4].split() == ["25569410", "RUNNING", "21:14:48", "---", "---"]
    assert output[5].split() == []


@pytest.mark.usefixtures("_mock_inquirer")
def test_no_state(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Unknown states produce empty output."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["23000381"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(console.main, ["--no-color", "--state", "ZZ", "23000381"])

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")
    assert output[0] == "Unknown state ZZ"
    assert output[1] == "No valid states provided"
    assert output[2].split() == [
        "JobID",
        "State",
        "Elapsed",
        "TimeEff",
        "CPUEff",
        "MemEff",
    ]
    assert output[3] == ""


@pytest.mark.usefixtures("_mock_inquirer")
def test_array_job_raw_id(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can find job array by base id."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24221219"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        ["--no-color", "24221219", "--format", "JobID%>,State,Elapsed%>,CPUEff,MemEff"],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "24220929_421",
        "COMPLETED",
        "00:09:34",
        "99.0%",
        "34.6%",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_array_job_single(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Can get single array job element."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24221219"] + console_jobs["24221220"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "24220929_421",
            "--format",
            "JobID%>,State,Elapsed%>,CPUEff,MemEff",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "24220929_421",
        "COMPLETED",
        "00:09:34",
        "99.0%",
        "34.6%",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_array_job_base(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Base array job id gets all elements."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["24221219"] + console_jobs["24221220"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        ["--no-color", "24220929", "--format", "JobID%>,State,Elapsed%>,CPUEff,MemEff"],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "24220929_421",
        "COMPLETED",
        "00:09:34",
        "99.0%",
        "34.6%",
    ]
    assert output[1].split() == ["24220929_431", "PENDING", "---", "---", "---"]
    assert len(output) == 2


@pytest.mark.usefixtures("_mock_inquirer")
def test_sacct_error(mocker: MockerFixture) -> None:
    """Subprocess errors in sacct are reported."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "test"),
    )
    result = runner.invoke(console.main, ["--no-color", "9999999"])

    assert result.exit_code == 1
    assert "Error running sacct!" in result.output


@pytest.mark.usefixtures("_mock_inquirer")
def test_empty_sacct(mocker: MockerFixture) -> None:
    """Empty sacct results produce just the header line."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = ""
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(console.main, ["--no-color", "9999999"])

    assert result.exit_code == 0
    output = result.output.split("\n")[:-1]
    assert output[0].split() == [
        "JobID",
        "State",
        "Elapsed",
        "TimeEff",
        "CPUEff",
        "MemEff",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_failed_no_mem(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Empty memory entries produce valid output."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["23000381"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(console.main, ["--no-color", "23000381"])

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == ["23000381", "FAILED", "00:00:12", "---", "---", "0.0%"]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_canceled_by_other(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Canceled states are correctly handled."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["23000233"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(console.main, ["--no-color", "23000233", "--state", "CA"])

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "23000233",
        "CANCELLED",
        "00:00:00",
        "0.0%",
        "---",
        "0.0%",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_zero_runtime(mocker: MockerFixture, console_jobs: dict[str, str]) -> None:
    """Entries with zero runtime produce reasonable timeeff."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = console_jobs["23000210"]
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(console.main, ["--no-color", "23000210"])

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == ["23000210", "FAILED", "00:00:00", "---", "---", "0.0%"]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_no_systems(mocker: MockerFixture) -> None:
    """When no scheduling system is found, raise error."""
    mocker.patch("reportseff.console.which", return_value=None)
    runner = CliRunner()
    result = runner.invoke(console.main, ["--no-color", "23000210"])

    assert result.exit_code == 1
    # remove header
    output = result.output.split("\n")
    assert output[0] == "No supported scheduling systems found!"


@pytest.mark.usefixtures("_mock_inquirer")
def test_issue_16(mocker: MockerFixture) -> None:
    """Incorrect memory usage for multi-node jobs."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = """
^|^16^|^07:36:03^|^65638294^|^65638294^|^^|^1^|^2^|^32G\
^|^COMPLETED^|^6-23:59:00^|^4-23:56:21^|^
^|^1^|^07:36:03^|^65638294.batch^|^65638294.batch\
^|^1147220K^|^1^|^1^|^^|^COMPLETED^|^^|^07:30:20^|^
^|^16^|^07:36:03^|^65638294.extern^|^65638294.extern\
^|^0^|^1^|^2^|^^|^COMPLETED^|^^|^00:00.001^|^
^|^15^|^00:00:11^|^65638294.0^|^65638294.0^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:11.830^|^
^|^15^|^00:02:15^|^65638294.1^|^65638294.1^|^4455540K\
^|^1^|^1^|^^|^COMPLETED^|^^|^31:09.458^|^
^|^15^|^00:00:10^|^65638294.2^|^65638294.2^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:00:04^|^
^|^15^|^00:00:08^|^65638294.3^|^65638294.3^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:09.602^|^
^|^15^|^00:00:07^|^65638294.4^|^65638294.4^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:56.827^|^
^|^15^|^00:00:06^|^65638294.5^|^65638294.5^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:03.512^|^
^|^15^|^00:00:08^|^65638294.6^|^65638294.6^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:08.520^|^
^|^15^|^00:00:13^|^65638294.7^|^65638294.7^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^01:02.013^|^
^|^15^|^00:00:02^|^65638294.8^|^65638294.8^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:03.639^|^
^|^15^|^00:00:06^|^65638294.9^|^65638294.9^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:08.683^|^
^|^15^|^00:00:08^|^65638294.10^|^65638294.10^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:57.438^|^
^|^15^|^00:00:06^|^65638294.11^|^65638294.11^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:03.642^|^
^|^15^|^00:00:09^|^65638294.12^|^65638294.12^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:10.271^|^
^|^15^|^00:01:24^|^65638294.13^|^65638294.13^|^4149700K\
^|^1^|^1^|^^|^COMPLETED^|^^|^17:18.067^|^
^|^15^|^00:00:01^|^65638294.14^|^65638294.14^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:03.302^|^
^|^15^|^00:00:10^|^65638294.15^|^65638294.15^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:14.615^|^
^|^15^|^00:06:45^|^65638294.16^|^65638294.16^|^4748052K\
^|^1^|^1^|^^|^COMPLETED^|^^|^01:36:40^|^
^|^15^|^00:00:10^|^65638294.17^|^65638294.17^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:03.864^|^
^|^15^|^00:00:09^|^65638294.18^|^65638294.18^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:48.987^|^
^|^15^|^01:32:53^|^65638294.19^|^65638294.19^|^7734356K\
^|^1^|^1^|^^|^COMPLETED^|^^|^23:09:33^|^
^|^15^|^00:00:01^|^65638294.20^|^65638294.20^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:03.520^|^
^|^15^|^00:00:07^|^65638294.21^|^65638294.21^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:50.015^|^
^|^15^|^00:55:17^|^65638294.22^|^65638294.22^|^8074500K\
^|^1^|^1^|^^|^COMPLETED^|^^|^13:45:29^|^
^|^15^|^00:00:13^|^65638294.23^|^65638294.23^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:04.413^|^
^|^15^|^00:00:12^|^65638294.24^|^65638294.24^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:49.100^|^
^|^15^|^00:57:41^|^65638294.25^|^65638294.25^|^7883152K\
^|^1^|^1^|^^|^COMPLETED^|^^|^14:20:36^|^
^|^15^|^00:00:01^|^65638294.26^|^65638294.26^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:03.953^|^
^|^15^|^00:00:05^|^65638294.27^|^65638294.27^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:47.223^|^
^|^15^|^01:00:17^|^65638294.28^|^65638294.28^|^7715752K\
^|^1^|^1^|^^|^COMPLETED^|^^|^14:59:40^|^
^|^15^|^00:00:06^|^65638294.29^|^65638294.29^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:04.341^|^
^|^15^|^00:00:07^|^65638294.30^|^65638294.30^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:50.416^|^
^|^15^|^01:22:31^|^65638294.31^|^65638294.31^|^7663264K\
^|^1^|^1^|^^|^COMPLETED^|^^|^20:33:59^|^
^|^15^|^00:00:05^|^65638294.32^|^65638294.32^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:04.199^|^
^|^15^|^00:00:08^|^65638294.33^|^65638294.33^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:50.009^|^
^|^15^|^01:32:23^|^65638294.34^|^65638294.34^|^7764884K\
^|^1^|^1^|^^|^COMPLETED^|^^|^23:01:52^|^
^|^15^|^00:00:06^|^65638294.35^|^65638294.35^|^0^|^1^|^1^|^^|^COMPLETED^|^^|^00:04.527^|^
"""
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main, ["--format", "+totalcpu", "--no-color", "65638294"]
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "65638294",
        "COMPLETED",
        "07:36:03",
        "4.5%",
        "98.6%",
        "24.1%",
        "4-23:56:21",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_energy_reporting(mocker: MockerFixture) -> None:
    """Include energy reporting with the `energy` format code."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        "^|^32^|^00:01:09^|^37403870_1^|^37403937^|^^|^1^|^1^|^32000M^|^"
        "COMPLETED^|^^|^00:02:00^|^00:47.734^|^\n"
        "^|^32^|^00:01:09^|^37403870_1.batch^|^37403937.batch^|^6300K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=33,fs/disk=0^|^^|^00:47.733^|^\n"
        "^|^32^|^00:01:09^|^37403870_1.extern^|^37403937.extern^|^4312K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=33,fs/disk=0^|^^|^00:00.001^|^\n"
        "^|^32^|^00:01:21^|^37403870_2^|^37403938^|^^|^1^|^1^|^32000M^|^"
        "COMPLETED^|^^|^00:02:00^|^00:41.211^|^\n"
        "^|^32^|^00:01:21^|^37403870_2.batch^|^37403938.batch^|^6316K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=32,fs/disk=0^|^^|^00:41.210^|^\n"
        "^|^32^|^00:01:21^|^37403870_2.extern^|^37403938.extern^|^4312K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=32,fs/disk=0^|^^|^00:00:00^|^\n"
        "^|^32^|^00:01:34^|^37403870_3^|^37403939^|^^|^1^|^1^|^32000M^|^"
        "COMPLETED^|^^|^00:02:00^|^00:51.669^|^\n"
        "^|^32^|^00:01:34^|^37403870_3.batch^|^37403939.batch^|^6184K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=30,fs/disk=0^|^^|^00:51.667^|^\n"
        "^|^32^|^00:01:35^|^37403870_3.extern^|^37403939.extern^|^4312K^|^1^|^1^|^^|^"
        "COMPLETED^|^fs/disk=0,energy=30^|^^|^00:00.001^|^\n"
        "^|^32^|^00:01:11^|^37403870_4^|^37403870^|^^|^1^|^1^|^32000M^|^"
        "COMPLETED^|^^|^00:02:00^|^01:38.184^|^\n"
        "^|^32^|^00:01:11^|^37403870_4.batch^|^37403870.batch^|^6300K^|^1^|^1^|^^|^"
        "COMPLETED^|^fs/disk=0^|^^|^01:38.183^|^\n"
        "^|^32^|^00:01:11^|^37403870_4.extern^|^37403870.extern^|^4312K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=27,fs/disk=0^|^^|^00:00.001^|^\n"
    )
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(console.main, ["--no-color", "--format=+energy", "37403870"])
    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[:-1]
    assert output[0].split() == [
        "JobID",
        "State",
        "Elapsed",
        "TimeEff",
        "CPUEff",
        "MemEff",
        "Energy",
    ]
    assert output[1].split() == [
        "37403870_1",
        "COMPLETED",
        "00:01:09",
        "57.5%",
        "2.1%",
        "0.0%",
        "33",
    ]
    assert output[2].split() == [
        "37403870_2",
        "COMPLETED",
        "00:01:21",
        "67.5%",
        "1.6%",
        "0.0%",
        "32",
    ]
    assert output[3].split() == [
        "37403870_3",
        "COMPLETED",
        "00:01:34",
        "78.3%",
        "1.7%",
        "0.0%",
        "30",
    ]
    assert output[4].split() == [
        "37403870_4",
        "COMPLETED",
        "00:01:11",
        "59.2%",
        "4.3%",
        "0.0%",
        "27",
    ]
    assert len(output) == 5


def test_extra_args(mocker: MockerFixture) -> None:
    """Can add extra arguments for sacct."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    mocker.patch("reportseff.console.get_jobs", return_value=("Testing", 1))
    result = runner.invoke(
        console.main,
        shlex.split("--no-color --extra-args='-D --units M --nodelist=node1,node2'"),
    )

    assert result.exit_code == 0


@pytest.mark.usefixtures("_mock_inquirer")
def test_filter_by_array_size(mocker: MockerFixture) -> None:
    """Include energy reporting with the `energy` format code."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        "^|^32^|^00:01:09^|^37403870_1^|^37403937^|^^|^1^|^1^|^32000M^|^"
        "COMPLETED^|^^|^00:02:00^|^00:47.734^|^\n"
        "^|^32^|^00:01:09^|^37403870_1.batch^|^37403937.batch^|^6300K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=33,fs/disk=0^|^^|^00:47.733^|^\n"
        "^|^32^|^00:01:09^|^37403870_1.extern^|^37403937.extern^|^4312K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=33,fs/disk=0^|^^|^00:00.001^|^\n"
        "^|^32^|^00:01:21^|^37403870_2^|^37403938^|^^|^1^|^1^|^32000M^|^"
        "COMPLETED^|^^|^00:02:00^|^00:41.211^|^\n"
        "^|^32^|^00:01:21^|^37403870_2.batch^|^37403938.batch^|^6316K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=32,fs/disk=0^|^^|^00:41.210^|^\n"
        "^|^32^|^00:01:21^|^37403870_2.extern^|^37403938.extern^|^4312K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=32,fs/disk=0^|^^|^00:00:00^|^\n"
        "^|^32^|^00:01:34^|^37403870_3^|^37403939^|^^|^1^|^1^|^32000M^|^"
        "COMPLETED^|^^|^00:02:00^|^00:51.669^|^\n"
        "^|^32^|^00:01:34^|^37403870_3.batch^|^37403939.batch^|^6184K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=30,fs/disk=0^|^^|^00:51.667^|^\n"
        "^|^32^|^00:01:35^|^37403870_3.extern^|^37403939.extern^|^4312K^|^1^|^1^|^^|^"
        "COMPLETED^|^fs/disk=0,energy=30^|^^|^00:00.001^|^\n"
        "^|^32^|^00:01:11^|^37403870_4^|^37403870^|^^|^1^|^1^|^32000M^|^"
        "COMPLETED^|^^|^00:02:00^|^01:38.184^|^\n"
        "^|^32^|^00:01:11^|^37403870_4.batch^|^37403870.batch^|^6300K^|^1^|^1^|^^|^"
        "COMPLETED^|^fs/disk=0^|^^|^01:38.183^|^\n"
        "^|^32^|^00:01:11^|^37403870_4.extern^|^37403870.extern^|^4312K^|^1^|^1^|^^|^"
        "COMPLETED^|^energy=27,fs/disk=0^|^^|^00:00.001^|^\n"
    )
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "--array-min-size",
            "10",
            "--format=+energy",
            "37403870",
        ],
    )
    assert result.exit_code == 0
    output = result.output.split("\n")[:-1]
    assert output[0].split() == [
        "JobID",
        "State",
        "Elapsed",
        "TimeEff",
        "CPUEff",
        "MemEff",
        "Energy",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_issue_58(mocker: MockerFixture) -> None:
    """Incorrect memory usage when filtering by state."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = """
^|^2^|^01:00:29^|^1234^|^1234^|^^|^1^|^^|^40G^|^TIMEOUT^|^01:00:00^|^01:01:06^|^
^|^2^|^01:00:33^|^1234.batch^|^1234.batch^|^19855760K^|^1^|^1^|^^|^CANCELLED^|^^|^01:01:06^|^
"""
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(console.main, ["--no-color", "1234"])

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "1234",
        "TIMEOUT",
        "01:00:29",
        "100.8%",
        "50.5%",
        "47.3%",
    ]
    assert len(output) == 1

    result = runner.invoke(console.main, ["--state", "TIMEOUT", "--no-color", "1234"])

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "1234",
        "TIMEOUT",
        "01:00:29",
        "100.8%",
        "50.5%",
        "47.3%",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_issue_73_maxrss(mocker: MockerFixture) -> None:
    """Incorrect maxrss with multi-step jobs."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = """^|^128^|^1234^|^1234^|^^|^1^|^^|^655G^|^COMPLETED^|^
^|^128^|^1234.batch^|^1234.batch^|^6044K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.extern^|^1234.extern^|^1330K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.0^|^1234.0^|^39503310K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.1^|^1234.1^|^39538757K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.2^|^1234.2^|^38352353K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.3^|^1234.3^|^38842343K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.4^|^1234.4^|^38445087K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.5^|^1234.5^|^38637714K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.6^|^1234.6^|^38573618K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.7^|^1234.7^|^38505592K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.8^|^1234.8^|^38951143K^|^1^|^1^|^^|^COMPLETED^|^
"""
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main, ["--format", "JobID,ReqMem,MaxRSS,MemEff", "--no-color", "1234"]
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "1234",
        "655G",
        "38G",
        "5.8%",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_issue_73_maxdiskreadnode(mocker: MockerFixture) -> None:
    """Incorrect maxrss with multi-step jobs."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = """^|^128^|^1234^|^1234^|^^|^^|^1^|^^|^655G^|^COMPLETED^|^
^|^128^|^1234.batch^|^1234.batch^|^node-l1g2^|^6044K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.extern^|^1234.extern^|^won't update^|^1330K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.0^|^1234.0^|^keepfirst^|^39503310K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.1^|^1234.1^|^node-asdf^|^39538757K^|^1^|^1^|^^|^COMPLETED^|^
"""
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--format",
            "JobID,ReqMem,MaxRSS,MemEff,MaxDiskReadNode",
            "--no-color",
            "1234",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "1234",
        "655G",
        "38G",
        "5.8%",
        "node-l1g2",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_issue_73_totals(mocker: MockerFixture) -> None:
    """Incorrect maxrss with multi-step jobs."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = """^|^128^|^1234^|^1234^|^^|^^|^1^|^^|^655G^|^COMPLETED^|^
^|^128^|^1234.batch^|^1234.batch^|^node-l1g2^|^6044K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.extern^|^1234.extern^|^won't update^|^1330K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.0^|^1234.0^|^keepfirst^|^39503310K^|^1^|^1^|^^|^COMPLETED^|^
^|^128^|^1234.1^|^1234.1^|^node-asdf^|^39538757K^|^1^|^1^|^^|^COMPLEteD^|^
"""
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--format",
            "JobID,ReqMem,TotalMaxRSS,MemEff,TotalMaxDiskReadNode,TotalState",
            "--no-color",
            "1234",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "1234",
        "655G",
        "75G",
        "5.8%",
        "node-l1g2",
        "COMPLETED",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_issue_84_empty_used_memory(mocker: MockerFixture) -> None:
    """Crash when the used memory in JS entry is empty."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    js_string = (
        "JS1:H4sIAJnmcGkC/13OTQ4CIQwF4LuwVtKiQ8hcZoJSDQk/hikLZ8LdRdSN274v73UX"
        "KTtaxbyLO4CS11rYyo2cZXmrxwuV4JN0NMCjLpzZhiVSzOX5vikxIxpE1AD63A4D1ZX"
        "cn4HpZNAY8xPsg98s+5y+Alrr2aeefeyDatID9+ewvQBAtRncqAAAAA=="
    )
    base_output = (
        "^|^1^|^00:01:17^|^12345678^|^12345678^|^^|^1^|^^|^4000M^|^COMPLETED^|^01:00:00^|^00:02.833^|^\n"
        "^|^1^|^00:01:17^|^12345678.batch^|^12345678.batch^|^138120K^|^1^|^1^|^^|^COMPLETED^|^^|^00:02.833^|^\n"
        "^|^1^|^00:01:17^|^12345678.extern^|^12345678.extern^|^^|^1^|^1^|^^|^COMPLETED^|^^|^00:00:00^|^\n"
        "^|^1^|^00:00:43^|^12345678.0^|^12345678.0^|^2200M^|^1^|^1^|^^|^COMPLETED^|^^|^00:00:00^|^\n"
    )
    sub_result.stdout = f"{js_string}{base_output}"
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "12345678",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "12345678",
        "COMPLETED",
        "00:01:17",
        "2.1%",
        "2.6%",
        "55.0%",
    ]
    assert len(output) == 1


@pytest.mark.usefixtures("_mock_inquirer")
def test_nonempty_used_memory(mocker: MockerFixture, strip_js: strip_js_func) -> None:
    """Check admin comment is used when available."""
    mocker.patch("reportseff.console.which", return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    js_string = (
        "JS1:H4sIANsKfWkC/13MQQqDMBCF4bvMOi0zmREaLyOSDFJITNG4KJK7m1pw4fb/eG+"
        "HOQddod8haIzjY2EvM8kvlFzGOCRNeflCL+SEURDRwLZquIAcd8iWbYP/pLyTto4v97"
        "QG/Gdr/1TrjYmdgelErAceV8ooiAAAAA=="
    )
    base_output = (
        "^|^1^|^00:18:59^|^4336165^|^4336165^|^^|^1^|^^|^4000M^|^COMPLETED^|^01:30:00^|^18:41.934^|^\n"
        "^|^1^|^00:18:59^|^4336165.batch^|^4336165.batch^|^4094320K^|^1^|^1^|^^|^COMPLETED^|^^|^18:41.934^|^\n"
        "^|^1^|^00:18:59^|^4336165.extern^|^4336165.extern^|^^|^1^|^1^|^^|^COMPLETED^|^^|^00:00:00^|^\n"
    )
    sub_result.stdout = f"{js_string}{base_output}"
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "4336165",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "4336165",
        "COMPLETED",
        "00:18:59",
        "21.1%",
        "95.6%",
        "46.1%",
    ]
    assert len(output) == 1

    # this will return a different value for only the memory eff
    no_memory = strip_js(js_string, ["used_memory", "total_memory"])
    sub_result.stdout = f"{no_memory}{base_output}"
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "4336165",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "4336165",
        "COMPLETED",
        "00:18:59",
        "21.1%",
        "95.6%",
        "100.0%",
    ]
    assert len(output) == 1

    # this will return a different value for only the time eff
    no_time = strip_js(js_string, ["total_time"])
    sub_result.stdout = f"{no_time}{base_output}"
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "4336165",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "4336165",
        "COMPLETED",
        "00:18:59",
        "21.1%",
        "98.4%",
        "46.1%",
    ]
    assert len(output) == 1

    # this will return a different value for both the time and mem eff
    no_time_mem = strip_js(js_string, ["used_memory", "total_memory", "total_time"])
    sub_result.stdout = f"{no_time_mem}{base_output}"
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=sub_result)
    result = runner.invoke(
        console.main,
        [
            "--no-color",
            "4336165",
        ],
    )

    assert result.exit_code == 0
    # remove header
    output = result.output.split("\n")[1:-1]
    assert output[0].split() == [
        "4336165",
        "COMPLETED",
        "00:18:59",
        "21.1%",
        "98.4%",
        "100.0%",
    ]
    assert len(output) == 1
