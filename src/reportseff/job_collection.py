import os
import re
from typing import Dict, List

from .job import Job
from .output_renderer import Output_Renderer


class Job_Collection:
    """
    A group of jobs
    """

    def __init__(self):
        # TODO probably take in output renderer, and db_inquirer
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
        ]

        self.job_file_regex = re.compile(
            r"^.*?[_-](?P<jobid>(?P<job>[0-9]+)(_[0-9]+)?)(.out)?$"
        )
        self.job_regex = re.compile(r"^(?P<jobid>(?P<job>[0-9]+)(_[][\-0-9]+)?)$")

        self.jobs = {}  # type: Dict[str, Job]
        self.renderer = None  # type: Output_Renderer
        self.dir_name = ""

    def get_columns(self) -> str:
        """
        The list of columns requested from inquirer
        """
        return self.columns

    def get_jobs(self) -> str:
        """
        List of jobs to get from inquirer
        """
        return sorted(set([job.job for job in self.jobs.values()]))

    def set_out_dir(self, directory: str):
        """
        Set this collection's directory to try parsing out jobs from
        output files
        """
        # set and validate working directory to full path
        if directory == "":
            wd = os.getcwd()
        else:
            wd = os.path.realpath(directory)

        if not os.path.exists(wd):
            raise ValueError(f"{wd} does not exist!")

        # get files from directory
        files = os.listdir(wd)
        files = list(filter(lambda x: os.path.isfile(os.path.join(wd, x)), files))
        if len(files) == 0:
            raise ValueError(f"{wd} contains no files!")

        for f in files:
            self.process_seff_file(f)

        if len(self.jobs) == 0:
            raise ValueError(f"{wd} contains no valid output files!")
        self.dir_name = wd

    def set_jobs(self, jobs: tuple):
        """
        Set the collection jobs to the provided job ids
        """
        if jobs == ():
            # look in current directory for slurm outputs
            self.set_out_dir("")
            return
        if len(jobs) == 1 and os.path.isdir(jobs[0]):
            self.set_out_dir(jobs[0])
            return
        for job_id in jobs:
            match = self.job_regex.match(job_id)

            if match:
                self.add_job(match.group("job"), match.group("jobid"))
            else:
                self.process_seff_file(job_id)

        if len(self.jobs) == 0:
            raise ValueError("No valid jobs provided!")

    def process_seff_file(self, filename: str):
        """
        Try to parse out job information from the supplied filename
        """
        match = self.job_file_regex.match(filename)
        if match:
            self.add_job(match.group("job"), match.group("jobid"), filename)

    def add_job(self, job: str, jobid: str, filename: str = None):
        """
        Add a job to the collection.
        job: the 'base' job number
        jobid: equal to the job unless it is an array job
        filename: the filename of the out file this job is derived from
        """
        self.jobs[jobid] = Job(job, jobid, filename)

    def process_entry(self, entry: Dict, user_provided: bool = False):
        """
        Update the jobs collection with information from the provided line
        """
        job_id = entry["JobID"].split(".")[0]
        job_id_raw = entry["JobIDRaw"].split(".")[0]
        if job_id not in self.jobs:
            match = self.job_regex.match(job_id)
            # job is in jobs
            if match and (match.group("job") in self.jobs or user_provided):
                self.add_job(match.group("job"), job_id)
            # check if the job_id is an array job
            elif job_id_raw in self.jobs:
                old_job = self.jobs.pop(job_id_raw)
                self.add_job(old_job.job, job_id, old_job.filename)
            else:
                return

        self.jobs[job_id].update(entry)

    def get_sorted_jobs(self, change_sort: bool) -> List:
        if change_sort:

            def get_time(f):
                # handle None and '', use numeric representation of name
                idnum = float(re.sub("[^0-9.]", "", f.jobid.replace("_", ".")))
                f = f.filename
                if f and self.dir_name:
                    f = os.path.join(self.dir_name, f)
                if f and os.path.exists(f):
                    return os.path.getmtime(f)
                else:
                    return idnum

            return sorted(self.jobs.values(), key=get_time, reverse=True)
        else:

            def get_file_name(f):
                f = f.name()
                f = os.path.join(self.dir_name, f)
                return (not os.path.exists(f), len(f), f)

            return sorted(self.jobs.values(), key=get_file_name)
