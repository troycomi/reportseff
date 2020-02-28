import subprocess
from abc import ABC, abstractmethod
from typing import List, Dict
import datetime
import click


class Base_Inquirer(ABC):
    def __init__(self):
        '''
        Initialize a new inquirer
        '''

    @abstractmethod
    def get_valid_formats(self) -> List[str]:
        '''
        Get the valid formatting options supported by the inquirer.
        Return as list of strings
        '''

    @abstractmethod
    def get_db_output(self, columns: List[str],
                      jobs: List[str]) -> List[Dict[str, str]]:
        '''
        Query the databse with the supplied columns
        Format output to be a list of rows, where each row is a dictionary
        with the columns as keys and entries as values
        Output order is not garunteed to match the jobs list
        '''

    @abstractmethod
    def set_user(self, user: str):
        '''
        Set the collection of jobs based on the provided user
        '''

    @abstractmethod
    def set_state(self, state: str):
        '''
        Set the state to filter output jobs with
        '''


class Sacct_Inquirer(Base_Inquirer):
    '''
    Implementation of Base_Inquirer for the sacct slurm function
    '''
    def __init__(self):
        self.default_args = 'sacct -P -n'.split()
        self.user = None
        self.state = None

    def get_valid_formats(self):
        command_args = 'sacct --helpformat'.split()
        result = subprocess.run(
            args=command_args,
            stdout=subprocess.PIPE,
            encoding='utf8',
            check=True,
            universal_newlines=True)
        if result.returncode != 0:
            raise Exception('Error retrieving sacct options with --helpformat')
        result = result.stdout.split()
        return result

    def get_db_output(self, columns, jobs, debug=False):
        '''
        Assumes the columns have already been validated.
        if debug is set, returns the subprocess result as
        the second element of tuple
        '''
        args = self.default_args + [
            '--format=' + ','.join(columns)
        ]

        if self.user:
            start_date = datetime.date.today() - datetime.timedelta(days=7)
            args += [
                f'--user={self.user}',
                f'--starttime={start_date.strftime("%m%d%y")}'  # MMDDYY
            ]
        else:
            args += ['--jobs=' + ','.join(jobs)]

        result = subprocess.run(
            args=args,
            stdout=subprocess.PIPE,
            encoding='utf8',
            check=True,
            universal_newlines=True)

        if result.returncode != 0:
            raise Exception('Error running sacct!')

        lines = result.stdout.split('\n')
        result = [dict(zip(columns, line.split('|')))
                  for line in lines if line]

        if self.state:
            result = [r for r in result
                      if r['State'] in self.state]

        if debug:
            return result, '\n'.join(lines)

        return result

    def set_user(self, user: str):
        '''
        Set the collection of jobs based on the provided user
        '''
        self.user = user

    def set_state(self, state: str):
        '''
        state is a comma separated string with codes and states
        Need to convert codes to states and set to upper
        Add states to list for searching later
        '''
        codes_to_states = {
            'BF': 'BOOT_FAIL',
            'CA': 'CANCELLED',
            'CD': 'COMPLETED',
            'DL': 'DEADLINE',
            'F': 'FAILED',
            'NF': 'NODE_FAIL',
            'OOM': 'OUT_OF_MEMORY',
            'PD': 'PENDING',
            'PR': 'PREEMPTED',
            'R': 'RUNNING',
            'RQ': 'REQUEUED',
            'RS': 'RESIZING',
            'RV': 'REVOKED',
            'S': 'SUSPENDED',
            'TO': 'TIMEOUT',
        }
        possible_states = codes_to_states.values()
        self.state = set()
        for st in state.split(','):
            st = st.upper()
            if st in codes_to_states:
                st = codes_to_states[st]
            self.state.add(st)

        for st in self.state:
            if st not in possible_states:
                click.secho(f'Unknown state {st}', fg='yellow', err=True)

        self.state = {st for st in self.state if st in possible_states}
