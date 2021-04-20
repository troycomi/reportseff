import re
from typing import Dict
from datetime import timedelta


multiple_map = {
        'K': 1024 ** 0,
        'M': 1024 ** 1,
        'G': 1024 ** 2,
        'T': 1024 ** 3,
        'E': 1024 ** 4,
}

state_colors = {
    'FAILED': 'red',
    'TIMEOUT': 'red',
    'OUT_OF_MEMORY': 'red',
    'RUNNING': 'cyan',
    'CANCELLED': 'yellow',
    'COMPLETED': 'green',
    'PENDING': 'blue',
}

#: Regex for DDHHMMSS style timestamps
DDHHMMSS_RE = re.compile(
    r'(?P<days>\d+)-(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})')
#: Regex for HHMMSS style timestamps
HHMMSS_RE = re.compile(
    r'(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})')
#: Regex for HHMMmmm style timestamps
MMSSMMM_RE = re.compile(
    r'(?P<minutes>\d{2}):(?P<seconds>\d{2}).(?P<milliseconds>\d{3})')


class Job():
    def __init__(self, job: str, jobid: str, filename: str):
        self.job = job
        self.jobid = jobid
        self.filename = filename
        self.stepmem = 0
        self.totalmem = None
        self.time = '---'
        self.time_eff = '---'
        self.cpu = '---'
        self.mem = '---'
        self.state = None
        self.other_entries = {}

    def __eq__(self, other):
        if not isinstance(other, Job):
            return False

        return self.__dict__ == other.__dict__

    def __repr__(self):
        return (f'Job(job={self.job}, jobid={self.jobid}, '
                f'filename={self.filename})')

    def update(self, entry: Dict):
        if '.' not in entry['JobID']:
            self.state = entry['State'].split()[0]

        if self.state == 'PENDING':
            return

        # master job id
        if self.jobid == entry['JobID']:
            self.other_entries = entry
            self.time = entry['Elapsed'] if 'Elapsed' in entry else None
            requested = _parse_slurm_timedelta(entry['Timelimit']) \
                if 'Timelimit' in entry else 1
            wall = _parse_slurm_timedelta(entry['Elapsed']) \
                if 'Elapsed' in entry else 0
            self.time_eff = round(wall / requested * 100, 1)
            if self.state == 'RUNNING':
                return
            cpus = (_parse_slurm_timedelta(entry['TotalCPU']) /
                    int(entry['AllocCPUS'])) \
                if 'TotalCPU' in entry and 'AllocCPUS' in entry else 0
            if wall == 0:
                self.cpu = None
            else:
                self.cpu = round(cpus / wall * 100, 1)
            self.totalmem = parsemem(entry['REQMEM'],
                                     int(entry['NNodes']),
                                     int(entry['AllocCPUS'])) \
                if 'REQMEM' in entry and 'NNodes' in entry \
                and 'AllocCPUS' in entry else None

        elif self.state != 'RUNNING':
            for k, v in entry.items():
                if k not in self.other_entries or not self.other_entries[k]:
                    self.other_entries[k] = v
            self.stepmem += parsememstep(entry['MaxRSS']) \
                if 'MaxRSS' in entry else 0

    def name(self):
        if self.filename:
            return self.filename
        else:
            return self.jobid

    def get_entry(self, key):
        if key == 'JobID':
            return self.name()
        if key == 'State':
            return self.state
        if key == 'MemEff':
            if self.totalmem:
                value = round(self.stepmem/self.totalmem*100, 1)
            else:
                value = '---'
            return value
        if key == 'TimeEff':
            return self.time_eff
        if key == 'CPUEff':
            return self.cpu if self.cpu else '---'
        else:
            return self.other_entries.get(key, '---')


def _parse_slurm_timedelta(delta: str) -> int:
    """Parse one of the three formats used in TotalCPU
    into a timedelta and return seconds."""
    match = re.match(DDHHMMSS_RE, delta)
    if match:
        return int(timedelta(
            days=int(match.group('days')),
            hours=int(match.group('hours')),
            minutes=int(match.group('minutes')),
            seconds=int(match.group('seconds'))
        ).total_seconds())
    match = re.match(HHMMSS_RE, delta)
    if match:
        return int(timedelta(
            hours=int(match.group('hours')),
            minutes=int(match.group('minutes')),
            seconds=int(match.group('seconds'))
        ).total_seconds())
    match = re.match(MMSSMMM_RE, delta)
    if match:
        return int(timedelta(
            minutes=int(match.group('minutes')),
            seconds=int(match.group('seconds')),
            milliseconds=int(match.group('milliseconds'))
        ).total_seconds())


def parsemem(mem: str, nodes: int, cpus: int):
    multiple = mem[-2]
    alloc = mem[-1]

    if mem[-2:-1].isdigit():
      mem=0.
    else:
      mem = float(mem[:-2]) * multiple_map[multiple]

    if alloc == 'n':
        return mem * nodes
    else:
        return mem * cpus


def parsememstep(mem: str):
    try:
        if mem == '':
            return 0
        multiple = mem[-1]

        mem = float(mem[:-1]) * multiple_map[multiple]

        return mem

    except ValueError:
        raise ValueError(f'Unexpected memstep format: {mem}')
