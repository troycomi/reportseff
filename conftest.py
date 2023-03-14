"""Collect common fixtures"""
import json
import gzip
import base64
import pytest


def to_comment(info: dict) -> str:
    """Convert jobstats dict to compressed base64 to match AdminComment"""
    return "JS1:" + base64.b64encode(
        gzip.compress(json.dumps(info, sort_keys=True, indent=4).encode("ascii"))
    ).decode("ascii")


@pytest.fixture
def get_jobstats():
    return to_comment


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


@pytest.fixture
def bad_gpu():
    """job with a failure due to bad gpu."""
    return [
        to_sacct_dict(
            "JS1:H4sIAMMP3WMC/12NQQrDIBRE7/LXthj1q9/LhFAlFLSGRhdFvHuTphTS5TDz3jR4ZB9WcA18iHG6RC5n3GPJZYpjCik/X+CUIEXaCNIM6hr8rxi0sUSEKNgXKfcUwAmjub4Sg9tSN/3AYF7qeJY2kOAsbrxUSIL3Y3TyHxuDUiuJovf+f2Ok/WD7SX8DnGNK388AAAA=|1|07:42:18|45352405|45352405||1|4G|CANCELLED by 349394|23:00:00|07:40:23"
        ),
        to_sacct_dict(
            "|1|07:42:20|45352405.batch|45352405.batch|1644460K|1||CANCELLED||07:40:23"
        ),
        to_sacct_dict(
            "|1|07:42:19|45352405.extern|45352405.extern|104K|1||COMPLETED||00:00:00"
        ),
    ]


@pytest.fixture
def bad_gpu_used():
    """job with a failure due to gpu with no utilization."""
    return [
        to_sacct_dict(
            "JS1:H4sIAN7HCGQC/1WOQQ6DIBBF7zJrawYGRvAyxlRiSFBMi4vWcPeitk1c/sy8//4GcxzcE9oNBhdCfwuoR7nHFFMfuslN8fGCViORYGUQsYL16Yb/RZGyDbNhNNUXSn5y0FpGZFNzBfdlLQYhKxiXtbsWb1BsRhtrSWkrSztdcj6hi/JkGk2sSMuDEEYSSqFY849IPvh3n3ycd6L82FrnnK8jDQmpDqAslPkDhnD5Hg8BAAA=|12|23:05:24|46044267|46044267||1|48000M|TIMEOUT|23:00:00|11-02:46:01"
        ),
        to_sacct_dict(
            "|12|23:05:55|46044267.batch|46044267.batch|42475564K|1||CANCELLED||11-02:46:01"
        ),
        to_sacct_dict(
            "|12|23:05:24|46044267.extern|46044267.extern|0|1||COMPLETED||00:00:00"
        ),
    ]
