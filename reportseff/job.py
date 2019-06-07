import re
import click
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
        self.cpu = '---'
        self.mem = '---'
        self.state = None

    def __eq__(self, other):
        if not isinstance(other, Job):
            return False

        return self.__dict__ == other.__dict__

    def update(self, entry: Dict):
        if '.' not in entry['JobID']:
            self.state = entry['State'].split()[0]

        if self.state == 'PENDING':
            return

        # # if job is cancelled prior to starting
        # if self.state == 'CANCELLED' and 'Elapsed' not in entry:
        #     return

        # master job id
        if self.jobid == entry['JobID']:
            self.time = entry['Elapsed']
            if self.state == 'RUNNING':
                return
            wall = _parse_slurm_timedelta(entry['Elapsed'])
            cpus = (_parse_slurm_timedelta(entry['TotalCPU']) /
                    int(entry['AllocCPUS']))
            if wall != 0:
                self.cpu = round(cpus / wall * 100, 1)
            else:
                self.cpu = -1
            self.totalmem = parsemem(entry['REQMEM'],
                                     int(entry['NNodes']),
                                     int(entry['AllocCPUS']))

        elif self.state != 'RUNNING':
            self.stepmem += parsememstep(entry['MaxRSS'])

    def name(self):
        if self.filename:
            return self.filename
        else:
            return self.jobid

    def render(self, max_width: int) -> str:
        if self.state is None:
            return ''

        result = ('{:>' + str(max_width) + '}').format(self.name())
        result += click.style('{:^16}'.format(self.state),
                              fg=state_colors[self.state])
        if self.time == '---':
            result += '{:^12}'.format(self.time)
        else:
            result += '{:>11} '.format(self.time)

        result += render_eff(self.cpu, 'cpu')
        if self.totalmem:
            value = round(self.stepmem/self.totalmem*100, 1)
        else:
            value = '---'
        result += render_eff(value, 'mem')

        result += '\n'
        return result


def render_eff(value: float, color_type: str) -> str:
    '''
    Return a styled string for efficiency values
    '''
    color_maps = {
        'mem': color_memory,
        'cpu': color_cpu
    }
    if color_type not in color_maps:
        raise ValueError(f'Unsupported color type: {color_type}')
    if value == '---':
        color = None
    elif value == -1:
        value = '---'
        color = 'red'
    else:
        color = color_maps[color_type](value)
        value = f'{value}%'
    return click.style('{:^9}'.format(value), fg=color)


def color_memory(value: float) -> str:
    '''
    Convert the memory efficiency value to a color
    '''
    if value < 20 or value > 90:
        return 'red'
    elif value > 60:
        return 'green'
    else:
        return None


def color_cpu(value: float) -> str:
    '''
    Convert the cpu efficiency value to a color
    '''
    if value < 20:
        return 'red'
    elif value > 80:
        return 'green'
    else:
        return None


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
