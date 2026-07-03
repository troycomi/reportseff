"""Test sacct implementation of db inqurirer."""

import datetime
import subprocess

import pytest
from pytest_mock import MockerFixture

import reportseff.db_inquirer as _dbi_mod
from reportseff.db_inquirer import (
    SacctInquirer,
    _check_jobstats_available,
    augment_with_jobstats,
)

STANDARD_ARGS = ["sacct", "--parsable", "-n", "--delimiter=^|^"]


@pytest.fixture
def sacct() -> SacctInquirer:
    """Default sacct inquirer."""
    return SacctInquirer()


def test_sacct_init(sacct: SacctInquirer) -> None:
    """Check default options on new object."""
    assert sacct.default_args == ["sacct", "--parsable", "-n", "--delimiter=^|^"]
    assert sacct.user is None


def test_sacct_get_valid_formats(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Check valid parsing of help format."""
    mock_sacct = mocker.MagicMock
    mock_sacct.returncode = 1
    # these are the values for 18.08.7
    mock_sacct.stdout = (
        "Account             AdminComment        AllocCPUS           AllocGRES"
        "\nAllocNodes          AllocTRES           AssocID             AveCPU"
        "\nAveCPUFreq          AveDiskRead         AveDiskWrite        "
        "AvePages           \nAveRSS              AveVMSize           BlockID "
        "Cluster            \nComment             ConsumedEnergy      "
        "ConsumedEnergyRaw   CPUTime            \nCPUTimeRAW "
        "DerivedExitCode     Elapsed             ElapsedRaw         \nEligible"
        " End                 ExitCode            GID                \nGroup"
        " JobID               JobIDRaw            JobName            \nLayout"
        " MaxDiskRead         MaxDiskReadNode     MaxDiskReadTask    "
        "\nMaxDiskWrite        MaxDiskWriteNode    MaxDiskWriteTask    "
        "MaxPages           \nMaxPagesNode        MaxPagesTask        MaxRSS"
        " MaxRSSNode         \nMaxRSSTask          MaxVMSize           "
        "MaxVMSizeNode       MaxVMSizeTask      \nMcsLabel            "
        "MinCPU              MinCPUNode          MinCPUTask         \nNCPUS"
        " NNodes              NodeList            NTasks             "
        "\nPriority Partition           QOS                 QOSRAW           "
        "\nReqCPUFreq          ReqCPUFreqMin       ReqCPUFreqMax       "
        "ReqCPUFreqGov      \nReqCPUS             ReqGRES             ReqMem "
        "ReqNodes           \nReqTRES             Reservation         "
        "ReservationId       Reserved           \nResvCPU             "
        "ResvCPURAW          Start               State              \nSubmit "
        "Suspended           SystemCPU           SystemComment      "
        "\nTimelimit           TimelimitRaw        TotalCPU            "
        "TRESUsageInAve     \nTRESUsageInMax      TRESUsageInMaxNode "
        "TRESUsageInMaxTask  TRESUsageInMin     \nTRESUsageInMinNode "
        "TRESUsageInMinTask  TRESUsageInTot      TRESUsageOutAve "
        "\nTRESUsageOutMax     TRESUsageOutMaxNode TRESUsageOutMaxTask "
        "TRESUsageOutMin    \nTRESUsageOutMinNode TRESUsageOutMinTask "
        "TRESUsageOutTot     UID                \nUser                "
        "UserCPU             WCKey               WCKeyID            "
        "\nWorkDir            \n"
    )
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_sacct)
    with pytest.raises(
        Exception, match="Error retrieving sacct options with --helpformat"
    ):
        sacct.get_valid_formats()

    mock_sacct.returncode = 0
    result = [
        "Account",
        "AdminComment",
        "AllocCPUS",
        "AllocGRES",
        "AllocNodes",
        "AllocTRES",
        "AssocID",
        "AveCPU",
        "AveCPUFreq",
        "AveDiskRead",
        "AveDiskWrite",
        "AvePages",
        "AveRSS",
        "AveVMSize",
        "BlockID",
        "Cluster",
        "Comment",
        "ConsumedEnergy",
        "ConsumedEnergyRaw",
        "CPUTime",
        "CPUTimeRAW",
        "DerivedExitCode",
        "Elapsed",
        "ElapsedRaw",
        "Eligible",
        "End",
        "ExitCode",
        "GID",
        "Group",
        "JobID",
        "JobIDRaw",
        "JobName",
        "Layout",
        "MaxDiskRead",
        "MaxDiskReadNode",
        "MaxDiskReadTask",
        "MaxDiskWrite",
        "MaxDiskWriteNode",
        "MaxDiskWriteTask",
        "MaxPages",
        "MaxPagesNode",
        "MaxPagesTask",
        "MaxRSS",
        "MaxRSSNode",
        "MaxRSSTask",
        "MaxVMSize",
        "MaxVMSizeNode",
        "MaxVMSizeTask",
        "McsLabel",
        "MinCPU",
        "MinCPUNode",
        "MinCPUTask",
        "NCPUS",
        "NNodes",
        "NodeList",
        "NTasks",
        "Priority",
        "Partition",
        "QOS",
        "QOSRAW",
        "ReqCPUFreq",
        "ReqCPUFreqMin",
        "ReqCPUFreqMax",
        "ReqCPUFreqGov",
        "ReqCPUS",
        "ReqGRES",
        "ReqMem",
        "ReqNodes",
        "ReqTRES",
        "Reservation",
        "ReservationId",
        "Reserved",
        "ResvCPU",
        "ResvCPURAW",
        "Start",
        "State",
        "Submit",
        "Suspended",
        "SystemCPU",
        "SystemComment",
        "Timelimit",
        "TimelimitRaw",
        "TotalCPU",
        "TRESUsageInAve",
        "TRESUsageInMax",
        "TRESUsageInMaxNode",
        "TRESUsageInMaxTask",
        "TRESUsageInMin",
        "TRESUsageInMinNode",
        "TRESUsageInMinTask",
        "TRESUsageInTot",
        "TRESUsageOutAve",
        "TRESUsageOutMax",
        "TRESUsageOutMaxNode",
        "TRESUsageOutMaxTask",
        "TRESUsageOutMin",
        "TRESUsageOutMinNode",
        "TRESUsageOutMinTask",
        "TRESUsageOutTot",
        "UID",
        "User",
        "UserCPU",
        "WCKey",
        "WCKeyID",
        "WorkDir",
    ]
    assert sacct.get_valid_formats() == result


def test_sacct_get_db_output(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """get_db_output returns subprocess output as dictionary."""
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "test"),
    )
    with pytest.raises(RuntimeError) as exception:
        sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"])
    assert "Error running sacct!" in str(exception)

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    result = sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"])
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=[*STANDARD_ARGS, "--format=c1,c2", "--jobs=j1,j2,j3"],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        shell=False,
        text=True,
    )

    debug: list[str] = []
    sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"], debug.append)
    assert debug[0] == ("c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n")


def test_sacct_get_db_output_no_newline(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Can process output without newlines."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        "16^|^00:00:00^|^23000233^|^23000233^|^^|^1^|^4000Mc^|^CANCELLED by 129319^|^"
        "6-00:00:00^|^00:00:00"
    )
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    debug: list[str] = []
    result = sacct.get_db_output(
        [
            "AllocCPUS",
            "Elapsed",
            "JobID",
            "JobIDRaw",
            "MaxRSS",
            "NNodes",
            "ReqMem",
            "State",
            "Timelimit",
            "TotalCPU",
        ],
        ["23000233"],
        debug.append,
    )
    assert result == [
        {
            "AllocCPUS": "16",
            "Elapsed": "00:00:00",
            "JobID": "23000233",
            "JobIDRaw": "23000233",
            "MaxRSS": "",
            "NNodes": "1",
            "ReqMem": "4000Mc",
            "State": "CANCELLED by 129319",
            "Timelimit": "6-00:00:00",
            "TotalCPU": "00:00:00",
        }
    ]
    mock_sub.assert_called_once()

    assert debug[0] == (
        "16^|^00:00:00^|^23000233^|^23000233^|^^|^1^|^4000Mc^|^CANCELLED by 129319^|^"
        "6-00:00:00^|^00:00:00"
    )


def test_sacct_set_user(sacct: SacctInquirer) -> None:
    """Can set user."""
    sacct.set_user("user")
    assert sacct.user == "user"


def test_sacct_get_db_output_user(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """User and since affects subprocess call."""
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "test"),
    )
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.date(2018, 1, 20)
    mock_date.side_effect = datetime.date
    mocker.patch("reportseff.db_inquirer.datetime.date", mock_date)
    with pytest.raises(Exception, match="Error running sacct!"):
        sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"])

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_user("user")
    result = sacct.get_db_output(["c1", "c2"], [])
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=[*STANDARD_ARGS, "--format=c1,c2", "--user=user", "--starttime=011318"],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        text=True,
        shell=False,
    )

    debug: list[str] = []
    sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"], debug.append)
    assert debug[0] == ("c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n")


def test_sacct_set_partition(sacct: SacctInquirer) -> None:
    """Can set partition."""
    sacct.set_partition("partition")
    assert sacct.partition == "partition"


def test_sacct_set_cluster(sacct: SacctInquirer) -> None:
    """Can set cluster."""
    sacct.set_cluster("cluster")
    assert sacct.cluster == "cluster"


def test_sacct_get_db_output_partition(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Subprocess call is affected by partition argument."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_partition("partition")
    result = sacct.get_db_output(["c1", "c2"], [])
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=[*STANDARD_ARGS, "--format=c1,c2", "--jobs=", "--partition=partition"],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        text=True,
        shell=False,
    )

    debug: list[str] = []
    sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"], debug.append)
    assert debug[0] == ("c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n")


def test_sacct_get_db_output_since(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Subprocess call is affected by since argument."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_since("time")
    result = sacct.get_db_output(["c1", "c2"], [])
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=[*STANDARD_ARGS, "--format=c1,c2", "--jobs=", "--starttime=time"],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        text=True,
        shell=False,
    )

    debug: list[str] = []
    sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"], debug.append)
    assert debug[0] == ("c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n")


def test_sacct_get_db_output_until(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Subprocess call is affected by until argument."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_until("time")
    result = sacct.get_db_output(["c1", "c2"], [])
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=[*STANDARD_ARGS, "--format=c1,c2", "--jobs=", "--endtime=time"],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        text=True,
        shell=False,
    )

    debug: list[str] = []
    sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"], debug.append)
    assert debug[0] == ("c1j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n")


def test_sacct_set_state(
    sacct: SacctInquirer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Decodes state properly and sets to upper."""
    sacct.set_state("BF,ca,cD,Dl,F,NF,OOM,PD,PR,R,RQ,RS,RV,S,TO")
    assert sacct.state == {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PENDING",
        "PREEMPTED",
        "RUNNING",
        "REQUEUED",
        "RESIZING",
        "REVOKED",
        "SUSPENDED",
        "TIMEOUT",
    }

    # sets to upper and removes duplicates
    sacct.set_state("TiMeOuT,running,FAILED,failed")
    assert sacct.state == {"TIMEOUT", "RUNNING", "FAILED"}

    # sets while warning of missing
    sacct.set_state("unknown,r,F")
    assert sacct.state == {"RUNNING", "FAILED"}
    assert capsys.readouterr().err == "Unknown state UNKNOWN\n"

    sacct.set_state("unknown,z")
    assert sacct.state == {""}
    assert set(capsys.readouterr().err.split("\n")) == {
        "Unknown state UNKNOWN",
        "Unknown state Z",
        "No valid states provided",
        "",
    }

    # remove duplicate unknowns
    sacct.set_state("unknown,z,z,z")
    assert sacct.state == {""}
    assert set(capsys.readouterr().err.split("\n")) == {
        "Unknown state UNKNOWN",
        "Unknown state Z",
        "No valid states provided",
        "",
    }


def test_sacct_set_until(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Can set since with various formats."""
    # no equal sign, retain argument
    sacct.set_until("022399")
    assert sacct.until == "022399"
    # also no error checking
    sacct.set_until("asdf")
    assert sacct.until == "asdf"

    # has an equal sign, handles year, month, day, hour, minute
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.datetime(2018, 1, 20, 10, 15, 20)  # noqa: DTZ001
    mock_date.side_effect = datetime.date
    mocker.patch("reportseff.db_inquirer.datetime.datetime", mock_date)

    sacct.set_until("w=2")
    assert sacct.until == "2018-01-06T10:15"

    sacct.set_until("W=2")
    assert sacct.until == "2018-01-06T10:15"

    sacct.set_until("weeks=2")
    assert sacct.until == "2018-01-06T10:15"

    sacct.set_until("d=2")
    assert sacct.until == "2018-01-18T10:15"

    sacct.set_until("D=2")
    assert sacct.until == "2018-01-18T10:15"

    sacct.set_until("days=2")
    assert sacct.until == "2018-01-18T10:15"

    sacct.set_until("H=-4")
    assert sacct.until == "2018-01-20T14:15"

    sacct.set_until("h=4")
    assert sacct.until == "2018-01-20T06:15"

    sacct.set_until("hours=4")
    assert sacct.until == "2018-01-20T06:15"

    sacct.set_until("M=3")
    assert sacct.until == "2018-01-20T10:12"

    sacct.set_until("m=3")
    assert sacct.until == "2018-01-20T10:12"

    sacct.set_until("minutes=3")
    assert sacct.until == "2018-01-20T10:12"

    # unknown code, don't add
    sacct.set_until("z=3")
    assert sacct.until == "2018-01-20T10:15"

    # can't parse arg to int, don't add
    sacct.set_until("M=z")
    assert sacct.until == "2018-01-20T10:15"

    # can't parse args without =, ignore
    sacct.set_until("a,M=3,z")
    assert sacct.until == "2018-01-20T10:12"

    # handle multiple
    sacct.set_until("w=2,d=1,h=3,m=4,z,H=a")
    assert sacct.until == "2018-01-05T07:11"

    # last repeat wins
    sacct.set_until("m=300,mInUtes=3")
    assert sacct.until == "2018-01-20T10:12"


def test_sacct_set_since(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Can set since with various formats."""
    # no equal sign, retain argument
    sacct.set_since("022399")
    assert sacct.since == "022399"
    # also no error checking
    sacct.set_since("asdf")
    assert sacct.since == "asdf"

    # has an equal sign, handles year, month, day, hour, minute
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.datetime(2018, 1, 20, 10, 15, 20)  # noqa: DTZ001
    mock_date.side_effect = datetime.date
    mocker.patch("reportseff.db_inquirer.datetime.datetime", mock_date)

    sacct.set_since("w=2")
    assert sacct.since == "2018-01-06T10:15"

    sacct.set_since("W=2")
    assert sacct.since == "2018-01-06T10:15"

    sacct.set_since("weeks=2")
    assert sacct.since == "2018-01-06T10:15"

    sacct.set_since("d=2")
    assert sacct.since == "2018-01-18T10:15"

    sacct.set_since("D=2")
    assert sacct.since == "2018-01-18T10:15"

    sacct.set_since("days=2")
    assert sacct.since == "2018-01-18T10:15"

    sacct.set_since("H=-4")
    assert sacct.since == "2018-01-20T14:15"

    sacct.set_since("h=4")
    assert sacct.since == "2018-01-20T06:15"

    sacct.set_since("hours=4")
    assert sacct.since == "2018-01-20T06:15"

    sacct.set_since("M=3")
    assert sacct.since == "2018-01-20T10:12"

    sacct.set_since("m=3")
    assert sacct.since == "2018-01-20T10:12"

    sacct.set_since("minutes=3")
    assert sacct.since == "2018-01-20T10:12"

    # unknown code, don't add
    sacct.set_since("z=3")
    assert sacct.since == "2018-01-20T10:15"

    # can't parse arg to int, don't add
    sacct.set_since("M=z")
    assert sacct.since == "2018-01-20T10:15"

    # can't parse args without =, ignore
    sacct.set_since("a,M=3,z")
    assert sacct.since == "2018-01-20T10:12"

    # handle multiple
    sacct.set_since("w=2,d=1,h=3,m=4,z,H=a")
    assert sacct.since == "2018-01-05T07:11"

    # last repeat wins
    sacct.set_since("m=300,mInUtes=3")
    assert sacct.since == "2018-01-20T10:12"


def test_sacct_get_db_output_user_state(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Can set user and state at the same time."""
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "test"),
    )
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.date(2018, 1, 20)
    mock_date.side_effect = datetime.date
    mocker.patch("reportseff.db_inquirer.datetime.date", mock_date)
    with pytest.raises(Exception, match="Error running sacct!"):
        sacct.get_db_output(["JobID", "c2"], ["j1", "j2", "j3"])

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        "c1j1^|^c2j1^|^RUNNING^|^\n"
        "c1j2^|^c2j2^|^RUNNING^|^\n"
        "c1j3^|^c2j3^|^COMPLETED^|^\n"
    )
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_user("user")
    sacct.set_state("R")
    result = sacct.get_db_output(["JobID", "c2", "State"], [])
    assert result == [
        {"JobID": "c1j1", "c2": "c2j1", "State": "RUNNING"},
        {"JobID": "c1j2", "c2": "c2j2", "State": "RUNNING"},
    ]
    mock_sub.assert_called_once_with(
        args=[
            *STANDARD_ARGS,
            "--format=JobID,c2,State",
            "--user=user",
            "--starttime=011318",
        ],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        text=True,
        shell=False,
    )

    # debug is not affected by state
    debug: list[str] = []
    sacct.get_db_output(["JobID", "c2", "State"], ["j1", "j2", "j3"], debug.append)
    assert debug[0] == (
        "c1j1^|^c2j1^|^RUNNING^|^\nc1j2^|^c2j2^|^RUNNING^|^\nc1j3^|^c2j3^|^COMPLETED^|^\n"
    )


def test_partition_timelimit_failure(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Get error when scontrol fails."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 1
    mock_sacct.stdout = ""
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_sacct)

    with pytest.raises(RuntimeError) as exception:
        sacct.get_partition_timelimits()

    assert "Error retrieving information from scontrol" in str(exception.value)


def test_partition_timelimit(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Can process scontrol output."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        "PartitionName=cpu\n"
        "   AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL\n"
        "   MaxNodes=UNLIMITED MaxTime=15-00:00:00 MinNodes=0\n"
        "\n"
        "PartitionName=datascience\n"
        "   AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL\n"
        "   MaxNodes=UNLIMITED MaxTime=MAXTIME MinNodes=0\n"
        "\n"
        "PartitionName=gpu\n"
        "   AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL\n"
        "   MaxNodes=UNLIMITED MaxTime=12-00:00:00 MinNodes=0\n"
    )
    mock_run = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )

    limits = sacct.get_partition_timelimits()
    assert limits == {
        "cpu": "15-00:00:00",
        "datascience": "MAXTIME",
        "gpu": "12-00:00:00",
    }

    assert mock_run.call_args.kwargs["args"] == ["scontrol", "show", "partition"]


def test_partition_timelimit_with_cluster(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Can process scontrol output."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        "PartitionName=cpu\n"
        "   AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL\n"
        "   MaxNodes=UNLIMITED MaxTime=15-00:00:00 MinNodes=0\n"
        "\n"
        "PartitionName=datascience\n"
        "   AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL\n"
        "   MaxNodes=UNLIMITED MaxTime=MAXTIME MinNodes=0\n"
        "\n"
        "PartitionName=gpu\n"
        "   AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL\n"
        "   MaxNodes=UNLIMITED MaxTime=12-00:00:00 MinNodes=0\n"
    )
    mock_run = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )

    sacct.set_cluster("Testing")
    limits = sacct.get_partition_timelimits()
    assert limits == {
        "cpu": "15-00:00:00",
        "datascience": "MAXTIME",
        "gpu": "12-00:00:00",
    }

    assert mock_run.call_args.kwargs["args"] == [
        "scontrol",
        "--cluster",
        "Testing",
        "show",
        "partition",
    ]


def test_partition_timelimit_issue_11(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Can process scontrol output from issue 11."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        "PartitionName=mainqueue\n"
        "   AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL\n"
        "   AllocNodes=ALL Default=YES QoS=N/A\n"
        "   DefaultTime=NONE DisableRootJobs=NO ExclusiveUser=NO GraceTime=0\n"
        "   MaxNodes=UNLIMITED MaxTime=UNLIMITED MinNodes=0 LLN=NO\n"
        "   Nodes=bignode\n"
        "   PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO\n"
        "   OverTimeLimit=NONE PreemptMode=OFF\n"
        "   State=UP TotalCPUs=128 TotalNodes=1 SelectTypeParameters=NONE\n"
        "   JobDefaults=(null)\n"
        "   DefMemPerNode=UNLIMITED MaxMemPerNode=UNLIMITED\n"
        "PartitionName=mainqueue2\n"
        "   AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL\n"
        "   AllocNodes=ALL Default=YES QoS=N/A\n"
        "   DefaultTime=NONE DisableRootJobs=NO ExclusiveUser=NO GraceTime=0\n"
        "   MaxNodes=UNLIMITED MaxTime=10-00:00:00 MinNodes=0 LLN=NO\n"
        "   Nodes=bignode\n"
        "   PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO\n"
        "   OverTimeLimit=NONE PreemptMode=OFF\n"
        "   State=UP TotalCPUs=128 TotalNodes=1 SelectTypeParameters=NONE\n"
        "   JobDefaults=(null)\n"
        "   DefMemPerNode=UNLIMITED MaxMemPerNode=UNLIMITED\n"
    )
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_sacct)

    limits = sacct.get_partition_timelimits()
    assert limits == {
        "mainqueue": "UNLIMITED",
        "mainqueue2": "10-00:00:00",
    }


def test_extra_args_setting(sacct: SacctInquirer) -> None:
    """Setting extra args are properly handled."""
    sacct.set_extra_args('-D --units M --nodelist "node1 node2"')
    assert sacct.extra_args == '-D --units M --nodelist "node1 node2"'
    assert sacct.set_sacct_args(["123"]) == [
        "--jobs=123",
        "-D",
        "--units",
        "M",
        "--nodelist",
        "node1 node2",
    ]


def test_sacct_get_db_output_issue_30(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Handle cases where jobname has a pipe."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1 | j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    result = sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"])
    assert result == [
        {"c1": "c1 | j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=[*STANDARD_ARGS, "--format=c1,c2", "--jobs=j1,j2,j3"],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        shell=False,
        text=True,
    )

    debug: list[str] = []
    sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"], debug.append)
    assert debug[0] == ("c1 | j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n")


def test_sacct_newline_jobs_issue_63(
    sacct: SacctInquirer,
    mocker: MockerFixture,
) -> None:
    """Handle cases when the job name contains a newline."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1 \n j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    result = sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"])
    assert result == [
        {"c1": "c1 \\n j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=[*STANDARD_ARGS, "--format=c1,c2", "--jobs=j1,j2,j3"],
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        shell=False,
        text=True,
    )

    debug: list[str] = []
    sacct.get_db_output(["c1", "c2"], ["j1", "j2", "j3"], debug.append)
    assert debug[0] == ("c1 \\n j1^|^c2j1^|^\nc1j2^|^c2j2^|^\nc1j3^|^c2j3^|^\n")


# ---------------------------------------------------------------------------
# Tests for _check_jobstats_available
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=False)
def reset_jobstats_cache():
    """Reset the module-level availability cache before and after each test."""
    _dbi_mod._JOBSTATS_AVAILABLE = None
    yield
    _dbi_mod._JOBSTATS_AVAILABLE = None


def test_check_jobstats_not_on_path(mocker: MockerFixture, reset_jobstats_cache) -> None:
    """Return False when jobstats is not found on PATH."""
    mocker.patch("reportseff.db_inquirer.shutil.which", return_value=None)
    assert _check_jobstats_available() is False


def test_check_jobstats_found_with_base64_flag(
    mocker: MockerFixture, reset_jobstats_cache
) -> None:
    """Return True when jobstats help text includes -b / --base64."""
    mocker.patch("reportseff.db_inquirer.shutil.which", return_value="/usr/bin/jobstats")
    mock_result = mocker.MagicMock()
    mock_result.stdout = "usage: jobstats [-h] [-j] [-b] [--base64] jobid\n"
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_result)
    assert _check_jobstats_available() is True


def test_check_jobstats_found_without_base64_flag(
    mocker: MockerFixture, reset_jobstats_cache
) -> None:
    """Return False when jobstats exists but help text lacks -b / --base64."""
    mocker.patch("reportseff.db_inquirer.shutil.which", return_value="/usr/bin/jobstats")
    mock_result = mocker.MagicMock()
    mock_result.stdout = "usage: jobstats [-h] [-j] jobid\n"
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_result)
    assert _check_jobstats_available() is False


def test_check_jobstats_subprocess_exception(
    mocker: MockerFixture, reset_jobstats_cache
) -> None:
    """Return False when the jobstats subprocess itself raises."""
    mocker.patch("reportseff.db_inquirer.shutil.which", return_value="/usr/bin/jobstats")
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run", side_effect=OSError("exec failed")
    )
    assert _check_jobstats_available() is False


def test_check_jobstats_result_is_cached(
    mocker: MockerFixture, reset_jobstats_cache
) -> None:
    """Availability check is cached; subprocess is only called once."""
    mocker.patch("reportseff.db_inquirer.shutil.which", return_value="/usr/bin/jobstats")
    mock_result = mocker.MagicMock()
    mock_result.stdout = "usage: jobstats [-b] jobid\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_result
    )
    _check_jobstats_available()
    _check_jobstats_available()
    # subprocess.run should only have been called once (for the -h check)
    assert mock_sub.call_count == 1


# ---------------------------------------------------------------------------
# Tests for augment_with_jobstats
# ---------------------------------------------------------------------------

def _make_row(
    jobid: str,
    jobidraw: str,
    admin_comment: str = "",
) -> dict:
    return {"JobID": jobid, "JobIDRaw": jobidraw, "AdminComment": admin_comment}


def test_augment_no_missing_rows(mocker: MockerFixture) -> None:
    """Rows that already have AdminComment are not passed to jobstats."""
    mock_sub = mocker.patch("reportseff.db_inquirer.subprocess.run")
    rows = [_make_row("123", "123", "JS1:abc123def456ghi7")]
    result = augment_with_jobstats(rows)
    assert result[0]["AdminComment"] == "JS1:abc123def456ghi7"
    mock_sub.assert_not_called()


def test_augment_injects_valid_payload(mocker: MockerFixture) -> None:
    """Valid base64 lines are prefixed with JS1: and injected."""
    fake_b64 = "YWJjZGVmZ2hpamtsbW5vcA=="  # arbitrary valid base64
    mock_result = mocker.MagicMock()
    mock_result.stdout = fake_b64 + "\n"
    mock_result.returncode = 0
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_result)

    rows = [_make_row("456", "456")]
    result = augment_with_jobstats(rows)
    assert result[0]["AdminComment"] == f"JS1:{fake_b64}"


def test_augment_skips_none_sentinel(mocker: MockerFixture) -> None:
    """Lines equal to 'None' (no Prometheus data) are silently skipped."""
    mock_result = mocker.MagicMock()
    mock_result.stdout = "None\n"
    mock_result.returncode = 0
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_result)

    rows = [_make_row("789", "789")]
    result = augment_with_jobstats(rows)
    assert result[0]["AdminComment"] == ""


def test_augment_skips_short_sentinel(mocker: MockerFixture) -> None:
    """Lines equal to 'Short' (job too brief) are silently skipped."""
    mock_result = mocker.MagicMock()
    mock_result.stdout = "Short\n"
    mock_result.returncode = 0
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_result)

    rows = [_make_row("321", "321")]
    result = augment_with_jobstats(rows)
    assert result[0]["AdminComment"] == ""


def test_augment_partial_failure(mocker: MockerFixture) -> None:
    """On partial failure only the rows matching stdout lines are updated."""
    fake_b64 = "dGVzdHBheWxvYWQ="
    mock_result = mocker.MagicMock()
    # jobstats processed only the first ID before failing
    mock_result.stdout = fake_b64 + "\n"
    mock_result.returncode = 1
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_result)

    rows = [_make_row("1", "1"), _make_row("2", "2")]
    result = augment_with_jobstats(rows)
    assert result[0]["AdminComment"] == f"JS1:{fake_b64}"
    assert result[1]["AdminComment"] == ""  # not reached


def test_augment_skips_substep_rows(mocker: MockerFixture) -> None:
    """Rows with a '.' in JobID (.batch, .extern) are never sent to jobstats."""
    mock_sub = mocker.patch("reportseff.db_inquirer.subprocess.run")
    rows = [_make_row("123.batch", "123", "")]
    augment_with_jobstats(rows)
    mock_sub.assert_not_called()


def test_augment_subprocess_exception(mocker: MockerFixture) -> None:
    """If the subprocess itself raises, rows are returned unchanged."""
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run", side_effect=OSError("exec failed")
    )
    rows = [_make_row("555", "555")]
    result = augment_with_jobstats(rows)
    assert result[0]["AdminComment"] == ""


def test_augment_batch_array_job(mocker: MockerFixture) -> None:
    """All array task rows are batched into a single subprocess call."""
    b64_1 = "cGF5bG9hZDE="
    b64_2 = "cGF5bG9hZDI="
    mock_result = mocker.MagicMock()
    mock_result.stdout = f"{b64_1}\n{b64_2}\n"
    mock_result.returncode = 0
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_result
    )

    rows = [_make_row("100_1", "9001"), _make_row("100_2", "9002")]
    result = augment_with_jobstats(rows)

    assert result[0]["AdminComment"] == f"JS1:{b64_1}"
    assert result[1]["AdminComment"] == f"JS1:{b64_2}"
    # only one subprocess call with both raw IDs
    assert mock_sub.call_count == 1
    call_args = mock_sub.call_args[0][0]
    assert "9001" in call_args
    assert "9002" in call_args


def test_augment_debug_message(mocker: MockerFixture) -> None:
    """debug_cmd is called with an informational message when rows are missing."""
    mock_result = mocker.MagicMock()
    mock_result.stdout = "None\n"
    mock_result.returncode = 0
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_result)

    debug_messages: list[str] = []
    rows = [_make_row("42", "42")]
    augment_with_jobstats(rows, debug_cmd=debug_messages.append)
    assert any("jobstats fallback" in m for m in debug_messages)
