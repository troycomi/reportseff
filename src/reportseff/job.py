"""Module for representing scheduler jobs."""

from __future__ import annotations

import base64
import contextlib
import gzip
import json
import math
import re
from datetime import timedelta
from typing import Any, Generator

multiple_map = {
    "K": 1024**1,
    "M": 1024**2,
    "G": 1024**3,
    "T": 1024**4,
    "E": 1024**5,
}

state_colors = {
    "FAILED": "red",
    "TIMEOUT": "red",
    "OUT_OF_MEMORY": "red",
    "RUNNING": "cyan",
    "CANCELLED": "yellow",
    "COMPLETED": "green",
    "PENDING": "blue",
}

#: Regex for DDHHMMSS style timestamps
DDHHMMSS_RE = re.compile(
    r"(?P<days>\d+)-(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})"
)
#: Regex for HHMMSS style timestamps
HHMMSS_RE = re.compile(r"(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})")
#: Regex for HHMMmmm style timestamps
MMSSMMM_RE = re.compile(
    r"(?P<minutes>\d{2}):(?P<seconds>\d{2}).(?P<milliseconds>\d{3})"
)
#: Regex for maxRSS and reqmem
MEM_RE = re.compile(
    r"(?P<memory>[-+]?\d*\.\d+|\d+)(?P<multiple>[KMGTE]?)(?P<type>[nc]?)"
)
ADMIN_COMMENT_MIN_LENGTH = 10


class Job:
    """Representation of scheduler job."""

    def __init__(self, job: str, jobid: str, filename: str | None) -> None:
        """Initialize new job.

        Args:
            job: the base job number
            jobid: same as job unless an array job
            filename: the output file associated with this job
        """
        self.job = job
        self.jobid = jobid
        self.filename = filename
        self.stepmem = 0.0
        self.totalmem: float | None = None
        self.time: str | None = "---"
        self.time_eff: str | float = "---"
        self.cpu: str | float | None = "---"
        self.state: str | None = None
        self.mem_eff: float | None = None
        self.gpu: float | None = None
        self.gpu_mem: float | None = None
        self.energy: int = 0
        self.totals: dict[str, Any] = {}
        self.other_entries: dict[str, Any] = {}
        # safe to cache now
        self.other_entries["JobID"] = self.name()
        self.comment_data: dict = {}

    def __eq__(self, other: Any) -> bool:
        """Test for equality.

        Args:
            other: the other object

        Returns:
            true if the other object is a Job and all attributes match
        """
        if not isinstance(other, Job):
            return False

        return self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        """Job representation.

        Returns:
            The job string representation
        """
        return f"Job(job={self.job}, jobid={self.jobid}, filename={self.filename})"

    def update(self, entry: dict) -> None:
        """Update the job properties based on the db_inquirer entry.

        Args:
            entry: the db_inquirer entry for the matching job
        """
        if "." not in entry["JobID"]:
            self.state = entry["State"].split()[0]

        if self.state == "PENDING":
            self._cache_entries()
            return

        # main job id
        if self.jobid == entry["JobID"]:
            self._update_main_job(entry)
            self._cache_entries()

        elif self.state != "RUNNING":
            for k, value in entry.items():
                # record first non-empty job step
                if k not in self.other_entries or not self.other_entries[k]:
                    self.other_entries[k] = value

                # record max of all steps
                elif k.casefold().startswith("max"):
                    with contextlib.suppress(ValueError):
                        # will convert numerics to floats as well
                        new_value = parsemem(value)
                        old_value = parsemem(self.other_entries[k])
                        if new_value > old_value:
                            self.other_entries[k] = value

                # record totals
                total_val = value
                with contextlib.suppress(ValueError):
                    total_val = parsemem(value)

                if k in self.totals:
                    if isinstance(total_val, float) and isinstance(
                        self.totals[k], float
                    ):
                        self.totals[k] += total_val
                elif total_val not in (0, ""):
                    self.totals[k] = total_val

            mem = parsemem(entry["MaxRSS"]) if "MaxRSS" in entry else 0
            tasks = int(entry.get("NTasks", 1))
            self.stepmem = max(self.stepmem, mem * tasks)

            if "TRESUsageOutAve" in entry:
                self.energy = max(
                    self.energy,
                    _parse_energy(entry["TRESUsageOutAve"]),
                )

    def _update_main_job(self, entry: dict) -> None:
        """Update properties for the main job.

        Args:
            entry: the entry where the jobid matches exactly, e.g. not batch or ex
        """
        for k, value in entry.items():
            if k not in self.other_entries or not self.other_entries[k]:
                self.other_entries[k] = value
        self.time = entry.get("Elapsed")

        requested = 0
        if "Timelimit" in entry and entry["Timelimit"] not in (
            "UNLIMITED",
            "Partition_Limit",
        ):
            requested = _parse_slurm_timedelta(entry["Timelimit"])

        wall = _parse_slurm_timedelta(entry["Elapsed"]) if "Elapsed" in entry else 0

        if requested != 0:
            self.time_eff = round(wall / requested * 100, 1)

        if self.state == "RUNNING":
            return

        total_cpu = _parse_slurm_timedelta(entry.get("TotalCPU", "00:00.000"))
        alloc_cpus = int(entry.get("AllocCPUS", 0))

        cpu_time = total_cpu / alloc_cpus if alloc_cpus != 0 else 0.0

        if wall == 0:
            self.cpu = None
        else:
            self.cpu = round(cpu_time / wall * 100, 1)

        if "ReqMem" in entry and "NNodes" in entry and "AllocCPUS" in entry:
            self.totalmem = parsemem(
                entry["ReqMem"], int(entry["NNodes"]), int(entry["AllocCPUS"])
            )

        if (
            "AdminComment" in entry
            and len(entry["AdminComment"]) > ADMIN_COMMENT_MIN_LENGTH
        ):
            self._parse_admin_comment(entry["AdminComment"])

    def _parse_admin_comment(self, comment: str) -> None:
        """Use admin command to override efficiency values.

        Decodes and parses admincommand from jobstats.

        Args:
            comment: The AdminComment field.
        """
        data = _parse_admin_comment_to_dict(comment)

        if data is None:
            return

        for node, node_data in data["nodes"].items():
            self.comment_data[node] = _get_node_data(data, node_data)

        self.cpu = _average_nested_dict("CPUEff", self.comment_data)
        self.mem_eff = _average_nested_dict("MemEff", self.comment_data)
        if data["gpus"]:
            self.gpu = _average_nested_dict("GPUEff", self.comment_data)
            self.gpu_mem = _average_nested_dict("GPUMem", self.comment_data)

    def _cache_entries(self) -> None:
        self.other_entries["State"] = self.state
        self.other_entries["TimeEff"] = self.time_eff
        self.other_entries["CPUEff"] = self.cpu if self.cpu else "---"
        self.other_entries["GPUEff"] = self.gpu if self.gpu is not None else "---"
        if self.gpu_mem is not None:
            self.other_entries["GPUMem"] = self.gpu_mem
        else:
            self.other_entries["GPUMem"] = "---"

    def name(self) -> str:
        """The name of the job.

        Returns:
            The filename (if set) or jobid
        """
        if self.filename:
            return self.filename
        return self.jobid

    def get_entry(self, key: str) -> Any:
        """Get an attribute by name.

        Args:
            key: the attribute to query

        Returns:
            The value of that attribute or "---" if not found
        """
        if key == "MemEff":
            return self._get_mem_entry()

        if key == "Energy":
            return self.energy

        if key.casefold().startswith("max"):
            # need to render as human readable
            entry = self.other_entries.get(key, "---")
            with contextlib.suppress(ValueError):
                entry = parsemem(entry)  # if encoded
            return render_num(entry)

        if key.casefold().startswith("total") and key not in self.other_entries:
            # need to render as human readable
            entry = self.totals.get(key.removeprefix("Total"), "---")
            return render_num(entry)

        return self.other_entries.get(key, "---")

    def _get_mem_entry(self) -> Any:
        """Get the memory entry."""
        if self.mem_eff:  # set by admin comment
            return self.mem_eff
        if self.totalmem:
            return round(self.stepmem / self.totalmem * 100, 1)
        return "---"

    def get_node_entries(
        self, key: str, *, gpu: bool = False
    ) -> Generator[Any, None, None]:
        """Get an attribute by name for each node in job.

        Args:
            key: the attribute to query
            gpu: True if gpu values should also be split

        Yields:
            A generator with values for provided attribute.  If the key does
            not change in nodes/gpus it will yield an empty string
        """
        yield self.get_entry(key)
        if len(self.comment_data) > 1 or (gpu and self.gpu is not None):
            for node, data in self.comment_data.items():
                # get node-level data
                if key == "JobID":
                    yield f"  {node}"
                else:
                    to_yield = data.get(key, "")
                    to_yield = (
                        to_yield if isinstance(to_yield, str) else round(to_yield, 1)
                    )
                    yield to_yield
                if (
                    gpu and self.gpu is not None and "gpus" in data
                ):  # has gpus to report
                    for gpu_name, gpu_data in data["gpus"].items():
                        if key == "JobID":
                            yield f"    {gpu_name}"
                        else:
                            to_yield = gpu_data.get(key, "")
                            to_yield = (
                                to_yield
                                if isinstance(to_yield, str)
                                else round(to_yield, 1)
                            )
                            yield to_yield


def _parse_slurm_timedelta(delta: str) -> int:
    """Parse one of the three formats used in TotalCPU.

    Based on which regex matches, convert into a timedelta
    and return total seconds.

    Args:
        delta: The time duration

    Returns:
        Number of seconds elapsed during the delta

    Raises:
        ValueError: if unable to parse delta
    """
    match = re.match(DDHHMMSS_RE, delta)
    if match:
        return int(
            timedelta(
                days=int(match.group("days")),
                hours=int(match.group("hours")),
                minutes=int(match.group("minutes")),
                seconds=int(match.group("seconds")),
            ).total_seconds()
        )
    match = re.match(HHMMSS_RE, delta)
    if match:
        return int(
            timedelta(
                hours=int(match.group("hours")),
                minutes=int(match.group("minutes")),
                seconds=int(match.group("seconds")),
            ).total_seconds()
        )
    match = re.match(MMSSMMM_RE, delta)
    if match:
        return int(
            timedelta(
                minutes=int(match.group("minutes")),
                seconds=int(match.group("seconds")),
                milliseconds=int(match.group("milliseconds")),
            ).total_seconds()
        )
    msg = f"Failed to parse time {delta!r}"
    raise ValueError(msg)


def parsemem(mem: str, nodes: int = 1, cpus: int = 1) -> float:
    """Parse memory representations of reqmem and maxrss.

    Args:
        mem: the memory representation
        nodes: the number of nodes in the job
        cpus: the number of cpus in the job

    Returns:
        The number of bytes for the job.
        if mem is empty, return 0.
        if mem ends with n or c, scale by the provided nodes or cpus respectively
        the multiple of memory (e.g. M or G) is always scaled if provided

    Raises:
        ValueError: if unable to parse mem
    """
    if mem in ("", "0"):
        return 0
    match = re.fullmatch(MEM_RE, mem)
    if not match:
        msg = f"Failed to parse memory {mem!r}"
        raise ValueError(msg)
    memory = float(match.group("memory"))

    if match.group("multiple") != "":
        memory *= multiple_map[match.group("multiple")]

    if match.group("type") != "":
        if match.group("type") == "n":
            memory *= nodes
        else:
            memory *= cpus
    return memory


def render_num(value: Any) -> str:
    """Render a number in human readable form.

    Args:
        value: the value to render

    Returns:
        A human readable value.
        Values less than 1024 are left as is, above is scaled by K, M, etc
    """
    if not isinstance(value, (int, float)):
        return str(value)

    if value == 0:
        return "0"

    scale = int(math.log2(abs(value)) // 10)
    suffix = ""
    if scale > 0:
        suffix = " KMGTE"[scale]

    return f"{round(value / 1024**scale)}{suffix}"


def _parse_energy(tres: str) -> int:
    """Parse energy usage from tres entry.

    Args:
        tres: the tres entry from sacct

    Returns:
        The energy usage for the job.  If missing, will return 0.
    """
    for entry in tres.split(","):
        tokens = entry.split("=")
        if tokens[0] == "energy":
            return int(tokens[1])
    return 0


def _parse_admin_comment_to_dict(comment: str) -> dict | None:
    """Attempt to parse AdminComment.

    Args:
        comment: The AdminComment field.

    Returns:
        the decoded dict
        None if the comment isn't recognized but can be ignored

    Raises:
        ValueError: if the comment doesn't start with JS1.
        ValueError: if the comment can't be decoded.
    """
    comment_type = comment[:3]

    if not comment_type.startswith("JS"):
        # ignore comments that aren't from jobstats (JS)
        return None

    if comment_type not in ("JS1",):
        msg = f"Unknown comment type {comment_type!r}"
        raise ValueError(msg)
    try:
        return json.loads(gzip.decompress(base64.b64decode(comment[4:])))
    except Exception as exception:
        msg = f"Cannot decode comment {comment!r}"
        raise ValueError(msg) from exception


def _get_node_data(comment_data: dict, node_data: dict) -> dict:
    """Parse node level data from admin comment values.

    Args:
        comment_data: The AdminComment field.
        node_data: Data for this node.

    Returns:
        the dict with efficiency information for this node
    """

    def get_gpu_value(comment_data: dict, key: str, gpu_number: int) -> float:
        if key in comment_data and gpu_number in comment_data[key]:
            return comment_data[key][gpu_number]
        return 0

    result = {
        "MemEff": node_data["used_memory"] / node_data["total_memory"] * 100,
    }

    if node_data["cpus"] == 0 or comment_data["total_time"] == 0:
        result["CPUEff"] = 0
    else:
        time_per_cpu = node_data["total_time"] / node_data["cpus"]
        result["CPUEff"] = time_per_cpu / comment_data["total_time"] * 100

    if comment_data["gpus"] and "gpu_total_memory" in node_data:
        result["gpus"] = {
            gpu: {
                "GPUEff": get_gpu_value(node_data, "gpu_utilization", gpu),
                "GPUMem": round(
                    get_gpu_value(node_data, "gpu_used_memory", gpu)
                    / get_gpu_value(node_data, "gpu_total_memory", gpu)
                    * 100,
                    1,
                ),
            }
            for gpu in node_data["gpu_total_memory"]
        }
        result["GPUEff"] = _average_nested_dict("GPUEff", result["gpus"])
        result["GPUMem"] = _average_nested_dict("GPUMem", result["gpus"])
    return result


def _average_nested_dict(nested_key: str, data: dict) -> float:
    """Average nested values in data dictionary.

    Args:
        nested_key: The key to key for averaging
        data: The dict to average.

    Returns:
        the mean value, rounded to one decimal
    """
    return round(
        sum(value[nested_key] for value in data.values() if nested_key in value)
        / len(data),
        1,
    )
