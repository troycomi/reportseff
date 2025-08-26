"""Module representing a collection of jobs."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

from .job import Job

if TYPE_CHECKING:  # pragma: no cover
    from .output_renderer import OutputRenderer


class JobCollection:
    """A group of jobs."""

    def __init__(self) -> None:
        """Create a new job collection with default options."""
        self.columns = [
            "JobIDRaw",
            "JobID",
            "State",
            "AllocCPUS",
            "TotalCPU",
            "Elapsed",
            "Timelimit",
            "REQMEM",
            "MaxRSS",
            "NNodes",
            "NTasks",
            "Partition",
        ]

        self.job_file_regex = re.compile(
            r"^.*?[_-](?P<jobid>(?P<job>[0-9]+)(_[0-9]+)?)(\.out)?$"
        )
        self.job_regex = re.compile(r"^(?P<jobid>(?P<job>[0-9]+)(_[][\-0-9]+)?)$")

        self.jobs: dict[str, Job] = {}
        self.renderer: OutputRenderer | None = None
        self.dir_name: Path | None = None
        self.partition_timelimits: dict = {}

    def get_columns(self) -> list[str]:
        """The list of columns requested from inquirer.

        Returns:
            The current columns of this collection.
        """
        return self.columns

    def get_jobs(self) -> list[str]:
        """List of jobs to get from inquirer.

        Returns:
            job names as a sorted list
        """
        return sorted({job.job for job in self.jobs.values()})

    def set_out_dir(self, directory: str) -> None:
        """Set this collection's directory to try parsing out jobs from output files.

        If directory is empty, will use the current directory, otherwise will
        try to get the full path name.

        Args:
            directory: the directory to search for job names.

        Raises:
            ValueError: if the directory does not exist
            ValueError: if the directory contains no files
            ValueError: if the directory contains no valid files
        """
        # set and validate working directory to full path
        working_directory = Path(directory).resolve() if directory else Path.cwd()

        if not working_directory.exists():
            msg = f"{working_directory} does not exist!"
            raise ValueError(msg)

        # get files from directory
        files = [file for file in working_directory.iterdir() if file.is_file()]
        if len(files) == 0:
            msg = f"{working_directory} contains no files!"
            raise ValueError(msg)

        for file in files:
            self.process_seff_file(file.name)

        if len(self.jobs) == 0:
            msg = (
                f"{working_directory} contains no valid output files!"
                "\nDo you need to set a custom format with `--slurm-format`?"
            )
            raise ValueError(msg)
        self.dir_name = working_directory

    def set_jobs(self, jobs: tuple) -> None:
        """Set the collection jobs to the provided job ids.

        if jobs is empty, use the current working directory
        if jobs is a singleton and a directory, set to that directory
        else, parse jobs as files or raw job ids

        Args:
            jobs: Tuple of job names to use for populating the collection

        Raises:
            ValueError: if no valid jobs are provided
        """
        if jobs == ():
            # look in current directory for slurm outputs
            self.set_out_dir("")
            return
        if len(jobs) == 1 and Path(jobs[0]).is_dir():
            self.set_out_dir(jobs[0])
            return
        for job_id in jobs:
            match = self.job_regex.match(job_id)

            if match:
                self.add_job(match.group("job"), match.group("jobid"))
            else:
                self.process_seff_file(job_id)

        if len(self.jobs) == 0:
            msg = "No valid jobs provided!"
            raise ValueError(msg)

    def process_seff_file(self, filename: str) -> None:
        """Try to parse out job information from the supplied filename.

        Args:
            filename: the filename to try and match
        """
        match = self.job_file_regex.match(filename)
        if match:
            self.add_job(match.group("job"), match.group("jobid"), filename)

    def set_custom_seff_format(self, filename_pattern: str) -> None:
        """Set the slurm output file parser to a custom value.

        Args:
            filename_pattern: the pattern passed to sbatch

        Raises:
            ValueError: the jobid cannot be determined from the provided pattern
        """
        pattern = re.escape(filename_pattern)
        # if %j is present, use that for jobid and job
        if "%j" in pattern:
            pattern = pattern.replace("%j", r"(?P<jobid>(?P<job>[0-9]+))")
        # if %a is present, it must follow %A to match expected slurm outputs
        elif "%A_%a" in pattern:
            pattern = pattern.replace(
                "%A_%a",
                r"(?P<jobid>(?P<job>[0-9]+)_[0-9]+)",
            )
        # if %A alone is present, use that for jobid and job
        elif "%A" in pattern:
            pattern = pattern.replace(
                "%A",
                r"(?P<jobid>(?P<job>[0-9]+))",
            )
        else:
            msg = (
                f"Unable to determine jobid from {filename_pattern}. "
                "Pattern should include one of ('%j', '%A', '%A_%a')"
            )
            raise ValueError(msg)

        tokens = re.split(r"(%[^%])", pattern)
        # combine sequential tokens
        processed_tokens = [""]
        for token in tokens:
            if not token:
                continue
            if token.startswith("%") and processed_tokens[-1].startswith(".*"):
                continue
            if token.startswith("%"):
                processed_tokens.append(".*")
            else:
                processed_tokens.append(token)
        self.job_file_regex = re.compile("^" + "".join(processed_tokens) + "$")

    def add_job(self, job: str, jobid: str, filename: str | None = None) -> None:
        """Add a job to the collection.

        Args:
            job: the 'base' job number
            jobid: equal to the job unless it is an array job
            filename: the filename of the out file this job is derived from
        """
        self.jobs[jobid] = Job(job, jobid, filename)

    def filter_by_array_size(self, min_size: int) -> None:
        """Filter jobs to only include array jobs above a minimum size threshold.

        Args:
            min_size: Minimum number of array tasks required to show job array.
                    If value is 0, then no filtering is applied.
                    Non-array jobs are always included regardless of this value.
        """
        if min_size <= 0:
            return

        # Group jobs by their base job number to count array tasks
        job_groups: dict[str, list[Job]] = {}
        for job in self.jobs.values():
            base_job = job.job
            if base_job not in job_groups:
                job_groups[base_job] = []
            job_groups[base_job].append(job)

        # Filter out array jobs that don't meet the minimum size
        jobs_to_keep = {}
        for job_list in job_groups.values():
            # Single job (not an array) - always keep
            if len(job_list) == 1 and "_" not in job_list[0].jobid:
                jobs_to_keep[job_list[0].jobid] = job_list[0]
            # Array job - keep only if it meets minimum size
            elif len(job_list) >= min_size:
                for job in job_list:
                    jobs_to_keep[job.jobid] = job

        self.jobs = jobs_to_keep

    def process_entry(self, entry: dict, *, add_job: bool = False) -> None:
        """Update the jobs collection with information from the provided entry.

        Args:
            entry: the account entry from a db inquirer
            add_job: if true, will add the job to the collection if it doesn't exist
        """
        job_id = entry["JobID"].split(".")[0]
        job_id_raw = entry["JobIDRaw"].split(".")[0]
        if job_id not in self.jobs:
            match = self.job_regex.match(job_id)
            # job is in jobs
            if match and (match.group("job") in self.jobs or add_job):
                self.add_job(match.group("job"), job_id)
            # check if the job_id is an array job
            elif job_id_raw in self.jobs:
                old_job = self.jobs.pop(job_id_raw)
                self.add_job(old_job.job, job_id, old_job.filename)
            else:
                return

        # handle partition limit for timelimit
        if (
            "Timelimit" in entry
            and entry["Timelimit"] == "Partition_Limit"
            and "Partition" in entry
            and entry["Partition"] in self.partition_timelimits
        ):
            entry["Timelimit"] = self.partition_timelimits[entry["Partition"]]

        self.jobs[job_id].update(entry)

    def get_sorted_jobs(self, *, change_sort: bool) -> list[Job]:
        """Sort the jobs.

        Args:
            change_sort: a switch to indicate the type of sorting
                if true will sort by the modified time of the file or job number
                if false will sort by the if the file exists,
                the length, then the content

        Returns:
            sorted list of jobs to display
        """

        def get_time(job: Job) -> float:
            # handle None and '', use numeric representation of name
            idnum = float(re.sub("[^0-9.]", "", job.jobid.replace("_", ".")))
            file = job.filename
            if file:
                path = Path(file)
                if self.dir_name:
                    path = self.dir_name / file
                if path.exists():
                    return path.stat().st_mtime
            return idnum

        def get_file_name(job: Job) -> tuple[bool, int, str]:
            file = Path(job.name())
            file = self.dir_name / file if self.dir_name else file
            return (not file.exists(), len(str(file)), str(file))

        if change_sort:
            return sorted(self.jobs.values(), key=get_time, reverse=True)

        return sorted(self.jobs.values(), key=get_file_name)

    def set_partition_limits(self, limits: dict) -> None:
        """Set partition limits from db inquirer.

        Args:
            limits: dict of partition to partition timelimit
        """
        self.partition_timelimits = limits
