"""Test job object."""
import pytest
import json
import gzip
import base64

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
    assert job.totalmem == 1 * 1024 ** 2

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
    assert job.totalmem == 1 * 1024 ** 2

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
    assert job.totalmem == 1024 ** 2


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
    assert job.get_entry("JobID") == "filename"
    assert job.get_entry("State") == "TEST"
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
                    == mem * 1024 ** exp * alloc
                ), f"{mem}{multiple}n"


def test_parsemem_cpus():
    """Can parse memory entries with cpus provided."""
    for mem in (1, 2, 4):
        for exp, multiple in enumerate("K M G T E".split()):
            for alloc in (1, 2, 4):
                assert (
                    job_module.parsemem(f"{mem}{multiple}c", -1, alloc)
                    == mem * 1024 ** exp * alloc
                ), f"{mem}{multiple}c"


def test_parsememstep():
    """Can parse memory for steps and handle odd formats."""
    for exp, multiple in enumerate("K M G T E".split()):
        for mem in (2, 4, 6):
            assert job_module.parsemem(f"{mem}{multiple}") == mem * 1024 ** exp

    with pytest.raises(ValueError) as e:
        job_module.parsemem("18GG")
    assert 'Failed to parse memory "18GG"' in str(e)

    assert job_module.parsemem("") == 0
    assert job_module.parsemem("0") == 0
    assert job_module.parsemem("5") == 5
    assert job_module.parsemem("1084.50M") == 1084.5 * 1024


def to_comment(info: dict) -> str:
    """Convert jobstats dict to compressed base64 to match AdminComment"""
    return "JS1:" + base64.b64encode(
        gzip.compress(json.dumps(info, sort_keys=True, indent=4).encode("ascii"))
    ).decode("ascii")


def to_sacct_dict(sacct_line: str) -> dict:
    columns = (
        "AdminComment",
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
    )
    return dict(zip(columns, sacct_line.split("|")))


@pytest.fixture
def single_core():
    """single core 8206163"""
    comment = to_comment(
        {
            "gpus": False,
            "nodes": {
                "tiger-h26c1n19": {
                    "cpus": 1,
                    "total_memory": 16106127360,
                    "total_time": 54515.3,
                    "used_memory": 582283264,
                }
            },
            "total_time": 54677,
        }
    )

    # sacct info is from another job!
    return [
        to_sacct_dict(
            f"{comment}|2|02:14:11|39889258_1426|39895850||1|8G|COMPLETED|02:55:00|03:43:27"
        ),
        to_sacct_dict(
            "|2|02:14:11|39889258_1426.batch|39895850.batch|4224K|1||COMPLETED||00:00.081"
        ),
        to_sacct_dict(
            "|2|02:14:11|39889258_1426.extern|39895850.extern|0|1||COMPLETED||00:00.001"
        ),
        to_sacct_dict(
            "|2|02:14:11|39889258_1426.0|39895850.0|3501608K|1||COMPLETED||03:43:27"
        ),
    ]


@pytest.fixture
def multi_node():
    """multiple nodes with 20 cpus, 80 GB, 9 minutes 8205048"""
    comment = to_comment(
        {
            "gpus": False,
            "nodes": {
                "tiger-h19c1n15": {
                    "cpus": 20,
                    "total_memory": 83886080000,
                    "total_time": 542.0,
                    "used_memory": 3790352384,
                },
                "tiger-h26c2n13": {
                    "cpus": 20,
                    "total_memory": 83886080000,
                    "total_time": 0.0,
                    "used_memory": 0,
                },
                "tiger-i26c2n11": {
                    "cpus": 20,
                    "total_memory": 83886080000,
                    "total_time": 0.0,
                    "used_memory": 0,
                },
                "tiger-i26c2n15": {
                    "cpus": 20,
                    "total_memory": 83886080000,
                    "total_time": 0.0,
                    "used_memory": 0,
                },
            },
            "total_time": 146,
        }
    )

    return [
        to_sacct_dict(
            f"{comment}|80|00:02:26|8205048|8205048||4|312.50G|COMPLETED|01:00:00|16:01.272"
        ),
        to_sacct_dict(
            "|20|00:02:26|8205048.batch|8205048.batch|1764224K|1||COMPLETED||16:01.268"
        ),
        to_sacct_dict(
            "|80|00:02:26|8205048.extern|8205048.extern|0|4||COMPLETED||00:00.004"
        ),
    ]


@pytest.fixture
def single_gpu():
    """one gpu, used all 16 GB, 30% eff 8197399"""
    comment = to_comment(
        {
            "gpus": True,
            "nodes": {
                "tiger-i23g14": {
                    "cpus": 1,
                    "gpu_total_memory": {"3": 17071734784},
                    "gpu_used_memory": {"3": 17040539648},
                    "gpu_utilization": {"3": 29.4},
                    "total_memory": 34359738368,
                    "total_time": 17368.0,
                    "used_memory": 3250450432,
                }
            },
            "total_time": 18203,
        }
    )

    return [
        to_sacct_dict(
            f"{comment}|1|05:03:23|8197399|8197399||1|32G|COMPLETED|23:59:00|04:49:38"
        ),
        to_sacct_dict(
            "|1|05:03:23|8197399.batch|8197399.batch|3132024K|1||COMPLETED||04:49:38"
        ),
        to_sacct_dict(
            "|1|05:03:23|8197399.extern|8197399.extern|0|1||COMPLETED||00:00:00"
        ),
    ]


@pytest.fixture
def multi_gpu():
    """4 gpus, 30% mem eff, 3% util 8189521"""
    comment = to_comment(
        {
            "gpus": True,
            "nodes": {
                "tiger-i19g9": {
                    "cpus": 28,
                    "gpu_total_memory": {
                        "0": 17071734784,
                        "1": 17071734784,
                        "2": 17071734784,
                        "3": 17071734784,
                    },
                    "gpu_used_memory": {
                        "0": 5146542080,
                        "1": 5146542080,
                        "2": 5146542080,
                        "3": 5146542080,
                    },
                    "gpu_utilization": {"0": 3.5, "1": 3.5, "2": 3.2, "3": 3.8},
                    "total_memory": 117440512000,
                    "total_time": 201481.2,
                    "used_memory": 30866018304,
                }
            },
            "total_time": 68687,
        }
    )
    return [
        to_sacct_dict(
            f"{comment}|28|19:04:47|8189521|8189521||1|112000M|CANCELLED by 129276|23:00:00|2-07:51:43"
        ),
        to_sacct_dict(
            "|28|19:04:48|8189521.batch|8189521.batch|29036860K|1||CANCELLED||2-07:51:43"
        ),
        to_sacct_dict(
            "|28|19:04:47|8189521.extern|8189521.extern|0|1||COMPLETED||00:00:00"
        ),
    ]


@pytest.fixture
def multi_node_multi_gpu():
    """made up job with multiple nodes and gpus"""
    comment = to_comment(
        {
            "gpus": True,
            "nodes": {
                "tiger-i19g9": {
                    "cpus": 28,
                    "gpu_total_memory": {
                        "0": 17071734784,
                        "1": 17071734784,
                        "2": 17071734784,
                        "3": 17071734784,
                    },
                    "gpu_used_memory": {
                        "0": 5146542080,
                        "1": 5146542080,
                        "2": 5146542080,
                        "3": 5146542080,
                    },
                    "gpu_utilization": {"0": 3.5, "1": 3.5, "2": 3.2, "3": 3.8},
                    "total_memory": 117440512000,
                    "total_time": 201481.2,
                    "used_memory": 30866018304,
                },
                "tiger-i19g10": {
                    "cpus": 28,
                    "gpu_total_memory": {
                        "0": 17071734783,
                        "1": 17071734783,
                        "2": 17071734783,
                        "3": 17071734783,
                    },
                    "gpu_used_memory": {
                        "0": 5146542380,
                        "1": 5146542380,
                        "2": 5146542322,
                        "3": 5146542380,
                    },
                    "gpu_utilization": {"0": 7.5, "1": 7.5, "2": 7.2, "3": 7.8},
                    "total_memory": 117440512000,
                    "total_time": 201411.2,
                    "used_memory": 30266018304,
                },
            },
            "total_time": 68687,
        }
    )
    return [
        to_sacct_dict(
            f"{comment}|28|19:04:47|8189521|8189521||1|112000M|CANCELLED by 129276|23:00:00|2-07:51:43"
        ),
        to_sacct_dict(
            "|28|19:04:48|8189521.batch|8189521.batch|29036860K|1||CANCELLED||2-07:51:43"
        ),
        to_sacct_dict(
            "|28|19:04:47|8189521.extern|8189521.extern|0|1||COMPLETED||00:00:00"
        ),
    ]


@pytest.fixture
def short_job():
    """used for jobs which don't last long enough 8205464"""
    comment = "JS1:Short"
    return [
        to_sacct_dict(
            f"{comment}|8|00:00:02|8205464|8205464||1|64G|FAILED|1-00:00:00|00:01.608"
        ),
        to_sacct_dict("|8|00:00:02|8205464.batch|8205464.batch|0|1||FAILED||00:00.020"),
        to_sacct_dict(
            "|8|00:00:02|8205464.extern|8205464.extern|0|1||COMPLETED||00:00:00"
        ),
        to_sacct_dict("|8|00:00:02|8205464.0|8205464.0|0|1||FAILED||00:01.587"),
    ]


def test_unknown_admin_comment(job):
    # unknown comment type
    with pytest.raises(ValueError, match="Unknown comment type 'JX1'"):
        job._parse_admin_comment("JX1:asdfasdf")

    # invalid gzip file
    with pytest.raises(ValueError, match="Cannot decode comment 'JS1:asdfasdf'"):
        job._parse_admin_comment("JS1:asdfasdf")

    # invalid base 64
    with pytest.raises(ValueError, match="Cannot decode comment 'JS1:asd&asdf'"):
        job._parse_admin_comment("JS1:asd&asdf")


def test_update_single_core(single_core):
    job = job_module.Job("39895850", "39889258_1426", None)
    for line in single_core:
        job.update(line)

    assert job.cpu == 99.7
    assert job.mem_eff == 3.6
    assert job.gpu is None
    assert job.gpu_mem is None


def test_update_multi_node(multi_node):
    job = job_module.Job("8205048", "8205048", None)
    for line in multi_node:
        job.update(line)

    assert job.cpu == 4.6
    assert job.mem_eff == 1.1
    assert job.gpu is None
    assert job.gpu_mem is None


def test_update_single_gpu(single_gpu):
    job = job_module.Job("8197399", "8197399", None)
    for line in single_gpu:
        job.update(line)

    assert job.cpu == 95.4
    assert job.mem_eff == 9.5
    assert job.gpu == 29.4
    assert job.gpu_mem == 99.8


def test_update_multi_gpu(multi_gpu):
    job = job_module.Job("8189521", "8189521", None)
    for line in multi_gpu:
        job.update(line)

    assert job.cpu == 10.5
    assert job.mem_eff == 26.3
    assert job.gpu == 3.5
    assert job.gpu_mem == 30.1


def test_update_multi_node_multi_gpu(multi_node_multi_gpu):
    job = job_module.Job("8189521", "8189521", None)
    for line in multi_node_multi_gpu:
        job.update(line)

    assert job.cpu == 10.5
    assert job.mem_eff == 26.0
    assert job.gpu == 5.5
    assert job.gpu_mem == 30.1


def test_update_short_job(short_job):
    job = job_module.Job("8205464", "8205464", None)
    for line in short_job:
        job.update(line)

    assert job.cpu == 6.2
    assert job.mem_eff is None
    assert job.gpu is None
    assert job.gpu_mem is None
