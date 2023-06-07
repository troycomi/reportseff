"""Test sacct implementation of db inqurirer."""
import datetime
import subprocess

import pytest

from reportseff import db_inquirer


@pytest.fixture
def sacct():
    """Default sacct inquirer."""
    return db_inquirer.SacctInquirer()


def test_sacct_init(sacct):
    """Check default options on new object."""
    assert sacct.default_args == ["sacct", "-P", "-n", "--delimiter=^|^"]
    assert sacct.user is None


def test_sacct_get_valid_formats(sacct, mocker):
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
    with pytest.raises(Exception) as exception:
        sacct.get_valid_formats()
    assert "Error retrieving sacct options with --helpformat" in str(exception)

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


def test_sacct_get_db_output(sacct, mocker):
    """get_db_output returns subprocess output as dictionary."""
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "test"),
    )
    with pytest.raises(RuntimeError) as exception:
        sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split())
    assert "Error running sacct!" in str(exception)

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    result = sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split())
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args="sacct -P -n --delimiter=^|^ --format=c1,c2 --jobs=j1,j2,j3".split(),
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        shell=False,
        universal_newlines=True,
    )

    debug = []
    sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split(), debug.append)
    assert debug[0] == ("c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n")


def test_sacct_get_db_output_no_newline(sacct, mocker):
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
    debug = []
    result = sacct.get_db_output(
        [
            "AllocCPUS",
            "Elapsed",
            "JobID",
            "JobIDRaw",
            "MaxRSS",
            "NNodes",
            "REQMEM",
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
            "REQMEM": "4000Mc",
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


def test_sacct_set_user(sacct):
    """Can set user."""
    sacct.set_user("user")
    assert sacct.user == "user"


def test_sacct_get_db_output_user(sacct, mocker):
    """User and since affects subprocess call."""
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "test"),
    )
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.date(2018, 1, 20)
    mock_date.side_effect = datetime.date
    mocker.patch("reportseff.db_inquirer.datetime.date", mock_date)
    with pytest.raises(Exception) as exception:
        sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split())
    assert "Error running sacct!" in str(exception)

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_user("user")
    result = sacct.get_db_output("c1 c2".split(), {})
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=(
            "sacct -P -n --delimiter=^|^ --format=c1,c2 --user=user --starttime=011318"
        ).split(),
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        universal_newlines=True,
        shell=False,
    )

    debug = []
    sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split(), debug.append)
    assert debug[0] == ("c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n")


def test_sacct_set_partition(sacct):
    """Can set partition."""
    sacct.set_partition("partition")
    assert sacct.partition == "partition"


def test_sacct_get_db_output_partition(sacct, mocker):
    """Subprocess call is affected by partition argument."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_partition("partition")
    result = sacct.get_db_output("c1 c2".split(), {})
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=(
            "sacct -P -n --delimiter=^|^ --format=c1,c2 --jobs= --partition=partition"
        ).split(),
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        universal_newlines=True,
        shell=False,
    )

    debug = []
    sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split(), debug.append)
    assert debug[0] == ("c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n")


def test_sacct_get_db_output_since(sacct, mocker):
    """Subprocess call is affected by since argument."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_since("time")
    result = sacct.get_db_output("c1 c2".split(), {})
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=(
            "sacct -P -n --delimiter=^|^ --format=c1,c2 --jobs= --starttime=time "
        ).split(),
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        universal_newlines=True,
        shell=False,
    )

    debug = []
    sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split(), debug.append)
    assert debug[0] == ("c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n")


def test_sacct_get_db_output_until(sacct, mocker):
    """Subprocess call is affected by until argument."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_until("time")
    result = sacct.get_db_output("c1 c2".split(), {})
    assert result == [
        {"c1": "c1j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args=(
            "sacct -P -n --delimiter=^|^ --format=c1,c2 --jobs= --endtime=time "
        ).split(),
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        universal_newlines=True,
        shell=False,
    )

    debug = []
    sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split(), debug.append)
    assert debug[0] == ("c1j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n")


def test_sacct_set_state(sacct, capsys):
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
    assert sacct.state == {None}
    assert {string for string in capsys.readouterr().err.split("\n")} == {
        "Unknown state UNKNOWN",
        "Unknown state Z",
        "No valid states provided to include",
        "",
    }

    # remove duplicate unknowns
    sacct.set_state("unknown,z,z,z")
    assert sacct.state == {None}
    assert {string for string in capsys.readouterr().err.split("\n")} == {
        "Unknown state UNKNOWN",
        "Unknown state Z",
        "No valid states provided to include",
        "",
    }


def test_sacct_set_until(sacct, mocker):
    """Can set since with various formats."""
    # no equal sign, retain argument
    sacct.set_until("022399")
    assert sacct.until == "022399"
    # also no error checking
    sacct.set_until("asdf")
    assert sacct.until == "asdf"

    # has an equal sign, handles year, month, day, hour, minute
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.datetime(2018, 1, 20, 10, 15, 20)
    mock_date.side_effect = lambda *args, **kw: datetime.date(*args, **kw)
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


def test_sacct_set_since(sacct, mocker):
    """Can set since with various formats."""
    # no equal sign, retain argument
    sacct.set_since("022399")
    assert sacct.since == "022399"
    # also no error checking
    sacct.set_since("asdf")
    assert sacct.since == "asdf"

    # has an equal sign, handles year, month, day, hour, minute
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.datetime(2018, 1, 20, 10, 15, 20)
    mock_date.side_effect = lambda *args, **kw: datetime.date(*args, **kw)
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


def test_sacct_get_db_output_user_state(sacct, mocker):
    """Can set user and state at the same time."""
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "test"),
    )
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.date(2018, 1, 20)
    mock_date.side_effect = datetime.date
    mocker.patch("reportseff.db_inquirer.datetime.date", mock_date)
    with pytest.raises(Exception) as exception:
        sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split())
    assert "Error running sacct!" in str(exception)

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        "c1j1^|^c2j1^|^RUNNING\nc1j2^|^c2j2^|^RUNNING\nc1j3^|^c2j3^|^COMPLETED\n"
    )
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    sacct.set_user("user")
    sacct.set_state("R")
    result = sacct.get_db_output("c1 c2 State".split(), {})
    assert result == [
        {"c1": "c1j1", "c2": "c2j1", "State": "RUNNING"},
        {"c1": "c1j2", "c2": "c2j2", "State": "RUNNING"},
    ]
    mock_sub.assert_called_once_with(
        args=(
            "sacct -P -n --delimiter=^|^ --format=c1,c2,State"
            " --user=user --starttime=011318"
        ).split(),
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        universal_newlines=True,
        shell=False,
    )

    # debug is not affected by state
    debug = []
    sacct.get_db_output("c1 c2 State".split(), "j1 j2 j3".split(), debug.append)
    assert debug[0] == (
        "c1j1^|^c2j1^|^RUNNING\nc1j2^|^c2j2^|^RUNNING\nc1j3^|^c2j3^|^COMPLETED\n"
    )


def test_partition_timelimit_failure(sacct, mocker):
    """Get error when scontrol fails."""
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 1
    mock_sacct.stdout = ""
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_sacct)

    with pytest.raises(RuntimeError) as exception:
        sacct.get_partition_timelimits()

    assert "Error retrieving information from scontrol" in str(exception.value)


def test_partition_timelimit(sacct, mocker):
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
    mocker.patch("reportseff.db_inquirer.subprocess.run", return_value=mock_sacct)

    limits = sacct.get_partition_timelimits()
    assert limits == {
        "cpu": "15-00:00:00",
        "datascience": "MAXTIME",
        "gpu": "12-00:00:00",
    }


def test_partition_timelimit_issue_11(sacct, mocker):
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


def test_extra_args_setting(sacct):
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


def test_sacct_get_db_output_issue_30(sacct, mocker):
    """Handle cases where jobname has a pipe."""
    mocker.patch(
        "reportseff.db_inquirer.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "test"),
    )

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = "c1 | j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n"
    mock_sub = mocker.patch(
        "reportseff.db_inquirer.subprocess.run", return_value=mock_sacct
    )
    result = sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split())
    assert result == [
        {"c1": "c1 | j1", "c2": "c2j1"},
        {"c1": "c1j2", "c2": "c2j2"},
        {"c1": "c1j3", "c2": "c2j3"},
    ]
    mock_sub.assert_called_once_with(
        args="sacct -P -n --delimiter=^|^ --format=c1,c2 --jobs=j1,j2,j3".split(),
        stdout=mocker.ANY,
        encoding=mocker.ANY,
        check=mocker.ANY,
        shell=False,
        universal_newlines=True,
    )

    debug = []
    sacct.get_db_output("c1 c2".split(), "j1 j2 j3".split(), debug.append)
    assert debug[0] == ("c1 | j1^|^c2j1\nc1j2^|^c2j2\nc1j3^|^c2j3\n")
