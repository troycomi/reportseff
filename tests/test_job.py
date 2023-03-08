"""Test job object."""
import pytest

from reportseff import job as job_module


@pytest.fixture
def job():
    """Default job ."""
    return job_module.Job("job", "jobid", "filename")


def test_eq():
    """Jobs must have all matching values to equal."""
    job1 = job_module.Job("j1", "j1", "filename")
    job2 = job_module.Job("j1", "j1", "filename")
    assert job1 == job2

    job2 = job_module.Job("j2", "j1", "filename")
    assert job1 != job2
    job2 = dict()
    assert job1 != job2


def test_repr():
    """Representation builds constructor."""
    job1 = job_module.Job("j1", "jid1", "filename")
    assert repr(job1) == "Job(job=j1, jobid=jid1, filename=filename)"

    job2 = job_module.Job("j2", "jid2", None)
    assert repr(job2) == "Job(job=j2, jobid=jid2, filename=None)"


def test_job_init(job):
    """Blank job has expected defaults."""
    assert job.job == "job"
    assert job.jobid == "jobid"
    assert job.filename == "filename"
    assert job.stepmem == 0
    assert job.totalmem is None
    assert job.time == "---"
    assert job.cpu == "---"
    assert job.mem == "---"
    assert job.state is None


def test_update_main_job():
    """Updating jobs changes expected properties."""
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
    assert job.state == "COMPLETED"
    assert job.time == "00:10:00"
    assert job.time_eff == 50.0
    assert job.cpu == 90.0
    assert job.totalmem == 1 * 1024**2

    job = job_module.Job("24371655", "24371655", None)
    job.update(
        {
            "JobID": "24371655",
            "State": "COMPLETED",
            "AllocCPUS": "1",
            "REQMEM": "1G",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:10:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    assert job.state == "COMPLETED"
    assert job.time == "00:10:00"
    assert job.time_eff == 50.0
    assert job.cpu == 90.0
    assert job.totalmem == 1 * 1024**2

    job = job_module.Job("24371655", "24371655", None)
    job.update(
        {
            "JobID": "24371655",
            "State": "COMPLETED",
            "AllocCPUS": "1",
            "REQMEM": "1G",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:10:00",
            "Timelimit": "00:20:00",
            "MaxRSS": "",
        }
    )
    assert job.state == "COMPLETED"
    assert job.time == "00:10:00"
    assert job.time_eff == 50.0
    assert job.cpu == 90.0
    assert job.totalmem is None

    job = job_module.Job("24371655", "24371655", None)
    job.update(
        {
            "JobID": "24371655",
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
    assert job.state == "PENDING"
    assert job.time == "---"
    assert job.time_eff == "---"
    assert job.cpu == "---"
    assert job.totalmem is None

    job = job_module.Job("24371655", "24371655", None)
    job.update(
        {
            "JobID": "24371655",
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
    job.update(
        {
            "JobID": "24371655.batch",
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
    assert job.state == "RUNNING"
    assert job.time == "00:10:00"
    assert job.time_eff == 50.0
    assert job.cpu == "---"
    assert job.totalmem is None

    job = job_module.Job("24371655", "24371655", None)
    job.update(
        {
            "JobID": "24371655",
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
    assert job.state == "CANCELLED"
    assert job.time == "00:00:00"
    assert job.time_eff == 0.0
    assert job.cpu is None
    assert job.totalmem == 1024**2


def test_update_main_job_unlimited():
    """Updating jobs changes expected properties."""
    job = job_module.Job("11741520", "11741520", None)
    job.update(
        {
            "AdminComment": "",
            "AllocCPUS": "4",
            "Elapsed": "03:22:47",
            "JobID": "11741520",
            "JobIDRaw": "11741520",
            "MaxRSS": "",
            "NNodes": "1",
            "REQMEM": "7Gc",
            "State": "COMPLETED",
            "Timelimit": "UNLIMITED",
            "TotalCPU": "01:15:11",
        }
    )
    assert job.state == "COMPLETED"
    assert job.time == "03:22:47"
    assert job.time_eff == "---"
    assert job.cpu == 9.3
    assert job.totalmem == 4 * 7 * 1024**2


def test_update_main_job_partition_limit():
    """Updating jobs changes expected properties."""
    job = job_module.Job("341", "341", None)
    job.update(
        {
            "AdminComment": "",
            "AllocCPUS": "1",
            "Elapsed": "00:00:00",
            "JobID": "341",
            "JobIDRaw": "341",
            "MaxRSS": "",
            "NNodes": "1",
            "Partition": "mainqueue",
            "QOS": "normal",
            "REQMEM": "4000G",
            "State": "CANCELLED by 1001",
            "Timelimit": "Partition_Limit",
            "TotalCPU": "00:00:00",
        }
    )
    assert job.state == "CANCELLED"
    assert job.time == "00:00:00"
    assert job.time_eff == "---"
    assert job.cpu is None
    assert job.totalmem == 4 * 1000 * 1024**2


def test_update_part_job():
    """Can update job with batch to add to stepmem."""
    job = job_module.Job("24371655", "24371655", None)
    job.update(
        {
            "JobID": "24371655.batch",
            "State": "COMPLETED",
            "AllocCPUS": "1",
            "REQMEM": "1Gn",
            "TotalCPU": "00:09:00",
            "Elapsed": "00:10:00",
            "MaxRSS": "495644K",
            "NNodes": "1",
            "NTasks": "",
        }
    )
    assert job.state is None
    assert job.time == "---"
    assert job.cpu == "---"
    assert job.totalmem is None
    assert job.stepmem == 495644


def test_parse_bug():
    """Can handle job id mismatches."""
    job = job_module.Job("24371655", "24371655", None)
    job.update(
        {
            "AllocCPUS": "1",
            "Elapsed": "00:00:19",
            "JobID": "34853801.extern",
            "JobIDRaw": "34853801.extern",
            "JobName": "extern",
            "MaxRSS": "0",
            "NNodes": "1",
            "REQMEM": "4Gn",
            "State": "COMPLETED",
            "Timelimit": "",
            "TotalCPU": "00:00:00",
        }
    )


def test_name(job):
    """Name is either filename or the jobid."""
    assert job.name() == "filename"
    job = job_module.Job("job", "jobid", None)
    assert job.name() == "jobid"


def test_get_entry(job):
    """State is read properly and updated by main job entry."""
    job.state = "TEST"
    assert job.get_entry("MemEff") == "---"
    assert job.get_entry("TimeEff") == "---"
    assert job.get_entry("CPUEff") == "---"
    assert job.get_entry("undefined") == "---"

    job = job_module.Job("24371655", "24371655", None)
    job.update(
        {
            "JobID": "24371655",
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
    assert job.get_entry("JobID") == "24371655"
    assert job.get_entry("State") == "CANCELLED"
    assert job.get_entry("MemEff") == 0.0
    assert job.get_entry("TimeEff") == 0.0
    assert job.get_entry("CPUEff") == "---"
    assert job.get_entry("undefined") == "---"
    assert job.get_entry("Elapsed") == "00:00:00"


def test_parse_slurm_timedelta():
    """Can parse all types of time formats."""
    timestamps = ["01-03:04:02", "03:04:02", "04:02.123"]
    expected_seconds = [97442, 11042, 242]
    for timestamp, seconds in zip(timestamps, expected_seconds):
        assert job_module._parse_slurm_timedelta(timestamp) == seconds

    with pytest.raises(ValueError) as exception:
        job_module._parse_slurm_timedelta("asdf")
    assert 'Failed to parse time "asdf"' in str(exception)


def test_parsemem_nodes():
    """Can parse memory entries with nodes provided."""
    for mem in (1, 2, 4):
        for exp, multiple in enumerate("K M G T E".split()):
            for alloc in (1, 2, 4):
                assert (
                    job_module.parsemem(f"{mem}{multiple}n", alloc, -1)
                    == mem * 1024**exp * alloc
                ), f"{mem}{multiple}n"


def test_parsemem_cpus():
    """Can parse memory entries with cpus provided."""
    for mem in (1, 2, 4):
        for exp, multiple in enumerate("K M G T E".split()):
            for alloc in (1, 2, 4):
                assert (
                    job_module.parsemem(f"{mem}{multiple}c", -1, alloc)
                    == mem * 1024**exp * alloc
                ), f"{mem}{multiple}c"


def test_parsememstep():
    """Can parse memory for steps and handle odd formats."""
    for exp, multiple in enumerate("K M G T E".split()):
        for mem in (2, 4, 6):
            assert job_module.parsemem(f"{mem}{multiple}") == mem * 1024**exp

    with pytest.raises(ValueError) as e:
        job_module.parsemem("18GG")
    assert 'Failed to parse memory "18GG"' in str(e)

    assert job_module.parsemem("") == 0
    assert job_module.parsemem("0") == 0
    assert job_module.parsemem("5") == 5
    assert job_module.parsemem("1084.50M") == 1084.5 * 1024


def test_unknown_admin_comment(job):
    """Unknown comment types raise informative errors."""
    with pytest.raises(ValueError, match="Unknown comment type 'JS0'"):
        job._parse_admin_comment("JS0:asdfasdf")

    # invalid gzip file
    with pytest.raises(ValueError, match="Cannot decode comment 'JS1:asdfasdf'"):
        job._parse_admin_comment("JS1:asdfasdf")

    # invalid base 64
    with pytest.raises(ValueError, match="Cannot decode comment 'JS1:asd&asdf'"):
        job._parse_admin_comment("JS1:asd&asdf")

    # valid, not updated
    job._parse_admin_comment('\'{"arrayTaskId":4294967294...')


def test_single_core(single_core):
    """Job with single node is updated properly."""
    job = job_module.Job("39895850", "39889258_1426", None)
    for line in single_core:
        job.update(line)

    assert job.cpu == 99.7
    assert job.mem_eff == 3.6
    assert job.gpu is None
    assert job.gpu_mem is None
    # single nodes are not printed
    assert list(job.get_node_entries("JobID")) == ["39889258_1426"]
    assert list(job.get_node_entries("CPUEff")) == [99.7]


def test_multi_node(multi_node):
    """Job with multiple nodes is updated properly."""
    job = job_module.Job("8205048", "8205048", None)
    for line in multi_node:
        job.update(line)

    assert job.cpu == 4.6
    assert job.mem_eff == 1.1
    assert job.gpu is None
    assert job.gpu_mem is None

    assert list(job.get_node_entries("JobID")) == [
        "8205048",
        "  tiger-h19c1n15",
        "  tiger-h26c2n13",
        "  tiger-i26c2n11",
        "  tiger-i26c2n15",
    ]

    assert list(job.get_node_entries("CPUEff")) == [
        4.6,
        18.6,
        0.0,
        0.0,
        0.0,
    ]

    assert list(job.get_node_entries("MemEff")) == [
        1.1,
        4.5,
        0.0,
        0.0,
        0.0,
    ]

    assert list(job.get_node_entries("GPUEff")) == [
        "---",
        "",
        "",
        "",
        "",
    ]


def test_single_gpu(single_gpu):
    """Jobs with GPUs are reported properly."""
    job = job_module.Job("8197399", "8197399", None)
    for line in single_gpu:
        job.update(line)

    assert job.cpu == 95.4
    assert job.mem_eff == 9.5
    assert job.gpu == 29.4
    assert job.gpu_mem == 99.8

    # without forcing GPU output
    assert list(job.get_node_entries("JobID")) == ["8197399"]
    assert list(job.get_node_entries("CPUEff")) == [95.4]
    assert list(job.get_node_entries("MemEff")) == [9.5]
    assert list(job.get_node_entries("GPUEff")) == [29.4]
    assert list(job.get_node_entries("GPUMem")) == [99.8]

    # with forced GPU output
    assert list(job.get_node_entries("JobID", True)) == [
        "8197399",
        "  tiger-i23g14",
        "    3",
    ]

    assert list(job.get_node_entries("CPUEff", True)) == [
        95.4,
        95.4,
        "",
    ]

    assert list(job.get_node_entries("MemEff", True)) == [
        9.5,
        9.5,
        "",
    ]

    assert list(job.get_node_entries("GPUEff", True)) == [
        29.4,
        29.4,
        29.4,
    ]

    assert list(job.get_node_entries("GPUMem", True)) == [
        99.8,
        99.8,
        99.8,
    ]


def test_multi_gpu(multi_gpu):
    """Single core, multi gpu jobs are updated properly."""
    job = job_module.Job("8189521", "8189521", None)
    for line in multi_gpu:
        job.update(line)

    assert job.cpu == 10.5
    assert job.mem_eff == 26.3
    assert job.gpu == 3.5
    assert job.gpu_mem == 30.1

    # without forcing GPU output
    assert list(job.get_node_entries("JobID")) == ["8189521"]
    assert list(job.get_node_entries("CPUEff")) == [10.5]
    assert list(job.get_node_entries("MemEff")) == [26.3]
    assert list(job.get_node_entries("GPUEff")) == [3.5]
    assert list(job.get_node_entries("GPUMem")) == [30.1]

    # with forced GPU output
    assert list(job.get_node_entries("JobID", True)) == [
        "8189521",
        "  tiger-i19g9",
        "    0",
        "    1",
        "    2",
        "    3",
    ]

    assert list(job.get_node_entries("CPUEff", True)) == [
        10.5,
        10.5,
        "",
        "",
        "",
        "",
    ]

    assert list(job.get_node_entries("MemEff", True)) == [
        26.3,
        26.3,
        "",
        "",
        "",
        "",
    ]

    assert list(job.get_node_entries("GPUEff", True)) == [
        3.5,
        3.5,
        3.5,
        3.5,
        3.2,
        3.8,
    ]

    assert list(job.get_node_entries("GPUMem", True)) == [
        30.1,
        30.1,
        30.1,
        30.1,
        30.1,
        30.1,
    ]


def test_multi_node_multi_gpu(multi_node_multi_gpu):
    """Multiple nodes with multiple gpus are updated properly."""
    job = job_module.Job("8189521", "8189521", None)
    for line in multi_node_multi_gpu:
        job.update(line)

    assert job.cpu == 10.5
    assert job.mem_eff == 26.0
    assert job.gpu == 5.5
    assert job.gpu_mem == 30.1

    assert list(job.get_node_entries("JobID")) == [
        "8189521",
        "  tiger-i19g10",
        "  tiger-i19g9",
    ]

    assert list(job.get_node_entries("CPUEff")) == [
        10.5,
        10.5,
        10.5,
    ]

    assert list(job.get_node_entries("MemEff")) == [
        26.0,
        25.8,
        26.3,
    ]

    assert list(job.get_node_entries("GPUEff")) == [
        5.5,
        7.5,
        3.5,
    ]

    assert list(job.get_node_entries("GPUMem")) == [
        30.1,
        30.1,
        30.1,
    ]

    assert list(job.get_node_entries("JobID", True)) == [
        "8189521",
        "  tiger-i19g10",
        "    0",
        "    1",
        "    2",
        "    3",
        "  tiger-i19g9",
        "    0",
        "    1",
        "    2",
        "    3",
    ]

    assert list(job.get_node_entries("CPUEff", True)) == [
        10.5,
        10.5,
        "",
        "",
        "",
        "",
        10.5,
        "",
        "",
        "",
        "",
    ]

    assert list(job.get_node_entries("MemEff", True)) == [
        26.0,
        25.8,
        "",
        "",
        "",
        "",
        26.3,
        "",
        "",
        "",
        "",
    ]

    assert list(job.get_node_entries("GPUEff", True)) == [
        5.5,
        7.5,
        7.5,
        7.5,
        7.2,
        7.8,
        3.5,
        3.5,
        3.5,
        3.2,
        3.8,
    ]

    assert list(job.get_node_entries("GPUMem", True)) == [
        30.1,
        30.1,
        30.1,
        30.1,
        30.1,
        30.1,
        30.1,
        30.1,
        30.1,
        30.1,
        30.1,
    ]


def test_short_job(short_job):
    """Jobs with JS1:Short are handled with sacct info instead."""
    job = job_module.Job("8205464", "8205464", None)
    for line in short_job:
        job.update(line)

    assert job.cpu == 6.2
    assert job.mem_eff is None
    assert job.gpu is None
    assert job.gpu_mem is None

    assert list(job.get_node_entries("JobID")) == ["8205464"]
    assert list(job.get_node_entries("JobID", True)) == ["8205464"]
    assert list(job.get_node_entries("CPUEff")) == [6.2]
    assert list(job.get_node_entries("State")) == ["FAILED"]


def test_bad_gpu(bad_gpu):
    """Jobs failing due to gpu are parsed properly."""
    job = job_module.Job("45352405", "45352405", None)
    for line in bad_gpu:
        job.update(line)

    assert job.cpu == 99.5
    assert job.mem_eff == 39.1
    assert job.gpu == 0
    assert job.gpu_mem == 1.0

    assert list(job.get_node_entries("JobID")) == ["45352405"]
    assert list(job.get_node_entries("CPUEff")) == [99.5]
    assert list(job.get_node_entries("State")) == ["CANCELLED"]


def test_bad_gpu_utilization(bad_gpu_used):
    """Jobs with no gpu utilization are parsed properly."""
    job = job_module.Job("46044267", "46044267", None)
    for line in bad_gpu_used:
        job.update(line)

    assert job.cpu == 96.2
    assert job.mem_eff == 86.4
    assert job.gpu == 4.8
    assert job.gpu_mem == 11.1

    assert list(job.get_node_entries("JobID")) == ["46044267"]
    assert list(job.get_node_entries("CPUEff")) == [96.2]
    assert list(job.get_node_entries("State")) == ["TIMEOUT"]
