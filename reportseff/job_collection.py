import re
from typing import Dict, List
from reportseff.job import Job
import os
import click


class Job_Collection():
    '''
    A group of slurm jobs
    '''

    def __init__(self):
        # formatting options

        self.slurm_format = [
            'JobIDRaw',
            'JobID',
            'State',
            'AllocCPUS',
            'REQMEM',
            'TotalCPU',
            'Elapsed',
            'MaxRSS',
            'NNodes',
            'NTasks'
        ]

        self.job_file_regex = re.compile(
            r'^.*?[-_](?P<jobid>(?P<job>[0-9]+)(_[0-9]+)?)(.out)?$')
        self.job_regex = re.compile(
            r'^(?P<jobid>(?P<job>[0-9]+)(_[0-9]+)?)$')

        self.jobs = {}  # type: Dict[str, Job]
        self.dir_name = ''

    def get_slurm_format(self) -> str:
        '''
        The string to pass to sacct to get formatting for correct parsing
        '''
        return ','.join(self.slurm_format)

    def get_slurm_jobs(self) -> str:
        '''
        Get the job ids to request from sacct as a comma separated list
        '''
        return ','.join(sorted(set([job.job for job in self.jobs.values()])))

    def set_slurm_out_dir(self, directory: str):
        '''
        Set this collection's directory to try parsing out jobs from slurm
        outputs
        '''
        # set and validate working directory to full path
        if directory == '':
            wd = os.getcwd()
        else:
            wd = os.path.realpath(directory)

        if not os.path.exists(wd):
            raise ValueError(f'{wd} does not exist!')

        # get files from directory
        files = os.listdir(wd)
        files = list(filter(lambda x: os.path.isfile(os.path.join(wd, x)),
                            files))

        if len(files) == 0:
            raise ValueError(f'{wd} contains no files!')

        for f in files:
            self.process_seff_file(f)

        if len(self.jobs) == 0:
            raise ValueError(f'{wd} contains no valid slurm outputs!')
        self.dir_name = wd

    def set_slurm_jobs(self, jobs: tuple):
        '''
        Set the collection jobs to the provided job ids
        '''
        for job_id in jobs:
            match = self.job_regex.match(job_id)

            if match:
                self.add_job(match.group('job'),
                             match.group('jobid'))
            else:
                self.process_seff_file(job_id)

        if len(self.jobs) == 0:
            raise ValueError('No valid slurm jobs provided!')

    def process_seff_file(self, filename: str):
        '''
        Try to parse out job information from the supplied filename
        '''
        match = self.job_file_regex.match(filename)
        if match:
            self.add_job(match.group('job'),
                         match.group('jobid'),
                         os.path.basename(filename))

    def add_job(self, job: str, jobid: str, filename: str = None):
        '''
        Add a job to the collection.
        job: the 'base' job number
        jobid: equal to the job unless it is an array job
        filename: the filename of the slurm out file this job is derived from
        '''
        self.jobs[jobid] = Job(job, jobid, filename)

    def process_line(self, line: str):
        '''
        Update the jobs collection with information from the provided line
        '''
        entry = dict(zip(self.slurm_format,  line.split('|')))
        job_id = entry['JobID'].split('.')[0]
        job_id_raw = entry['JobIDRaw'].split('.')[0]
        if job_id not in self.jobs:
            match = self.job_regex.match(job_id)
            # job is in jobs
            if match and match.group('job') in self.jobs:
                self.add_job(match.group('job'), job_id)
            # check if the job_id is an array job
            elif job_id_raw in self.jobs:
                old_job = self.jobs.pop(job_id_raw)
                self.add_job(old_job.job, job_id, old_job.filename)
            else:
                return

        self.jobs[job_id].update(entry)

    def get_output(self, change_sort: bool):
        max_width = max([len(job.name()) for job in self.jobs.values()]) + 3
        result = click.style(
            ('{:^' + str(max_width) + '}{:^16}{:^12}{:^9}{:^9}\n').format(
                'Name', 'State', 'Time', 'CPU', 'Memory'),
            bold=True)

        for job in self.get_sorted_jobs(change_sort):
            result += job.render(max_width)

        return result, len(self.jobs)

    def get_sorted_jobs(self, change_sort: bool) -> List:
        if change_sort and self.dir_name:
            def get_time(f):
                f = f.filename
                f = os.path.join(self.dir_name, f)
                if os.path.exists(f):
                    return os.path.getmtime(f)
                else:
                    return 0

            return sorted(self.jobs.values(),
                          key=get_time, reverse=True)
        else:
            def get_file_name(f):
                f = f.name()
                f = os.path.join(self.dir_name, f)
                return (not os.path.exists(f), len(f), f)

            return sorted(self.jobs.values(),
                          key=get_file_name)
