from typing import List
import click
from reportseff.job import Job


class Output_Renderer():
    def __init__(self,
                 valid_titles: List,
                 format_str: str = 'JobID%>,State,Elapsed%>,CPUEff,MemEff'):
        '''
        Initialize renderer with format string and list of valid titles
        '''
        # values required for proper parsing, need not be included in output
        self.required = [
            'JobID',
            'JobIDRaw',
            'State'
        ]
        # values derived from other values, list includes all dependent values
        self.derived = {
            'CPUEff': ['TotalCPU', 'AllocCPUS', 'Elapsed'],
            'MemEff': ['REQMEM', 'NNodes', 'AllocCPUS', 'MaxRSS'],
            'TimeEff': ['Elapsed', 'Timelimit']
        }

        # build formatters
        self.formatters = self.build_formatters(format_str)

        # validate with titles and derived keys
        valid_titles += self.derived.keys()
        self.query_columns = self.validate_formatters(valid_titles)

        # build columns for sacct call
        self.correct_columns()

    def build_formatters(self, format_str: str) -> List:
        '''
        Generate list of formatters from comma separated list in format string
        Return list of formatters
        '''
        return [Column_Formatter(fmt)
                for fmt in format_str.split(',')
                if fmt != '']

    def validate_formatters(self, valid_titles: List) -> List:
        '''
        Validate all titles of formatters
        Return list of query columns
        '''
        return [fmt.validate_title(valid_titles)
                for fmt in self.formatters]

    def correct_columns(self):
        '''
        use derived values to update the list of query columns
        '''
        result = [self.derived[c] if c in self.derived else [c]
                  for c in self.query_columns]
        # flatten
        result = [item for sublist in result for item in sublist]

        # add in required values
        result += self.required

        # remove duplicates
        self.query_columns = list(sorted(set(result)))

    def format_jobs(self, jobs: List[Job]) -> str:
        '''
        Given list of jobs, build output table
        '''
        for fmt in self.formatters:
            fmt.compute_width(jobs)

        result = ''
        if len(self.formatters) == 0:
            return result

        if len(self.formatters) == 1:
            # if only one formatter is present, override the alignment
            self.formatters[0].alignment = '<'
            # skip adding the title

        else:
            result += ' '.join([fmt.format_title()
                                for fmt in self.formatters])
            if len(jobs) != 0:
                result += '\n'

        result += '\n'.join(
            ' '.join([fmt.format_job(job) for fmt in self.formatters]).rstrip()
            for job in jobs
        )

        return result


class Column_Formatter():
    def __init__(self, token):
        '''
        Build column entry from format string of the form
        NAME[%[ALIGNMENT][WIDTH]]
        '''
        tokens = token.split('%')
        self.title = tokens[0]
        self.alignment = '^'
        self.width = None  # must calculate later
        if len(tokens) > 1:
            format_str = tokens[1]
            if format_str[0] in '<^>':
                self.alignment = format_str[0]
                format_str = format_str[1:]
            if format_str:
                try:
                    self.width = int(format_str)
                except ValueError:
                    raise ValueError(f"Unable to parse format token '{token}'")

        self.color_function = lambda x: (x, None)
        fold_title = self.title.casefold()
        if fold_title == 'state':
            self.color_function = color_state
        elif fold_title == 'cpueff':
            self.color_function = lambda x: render_eff(x, 'high')
        elif fold_title == 'timeeff' or fold_title == 'memeff':
            self.color_function = lambda x: render_eff(x, 'mid')

    def __eq__(self, other):
        if isinstance(other, Column_Formatter):
            for k in self.__dict__:
                if k != 'color_function' and \
                        self.__dict__[k] != other.__dict__[k]:
                    return False
            return True
        if isinstance(other, str):
            return self.title == other
        return False

    def __repr__(self):
        return f'{self.title}%{self.alignment}{self.width}'

    def validate_title(self, valid_titles: List) -> str:
        '''
        Tries to find this formatter's title in the valid titles list
        case insensitive.  If found, replace with valid_title to correct
        capitalization to match valid_titles entry.
        If not found, raise value error
        Returns the valid title found
        '''
        fold_title = self.title.casefold()
        for title in valid_titles:
            if fold_title == title.casefold():
                self.title = title
                return title

        raise ValueError(f"'{self.title}' is not a valid title")

    def compute_width(self, jobs: List):
        '''
        Determine the max width of all entries if the width attribute is unset.
        Includes title in determination
        '''
        if self.width is not None:
            return

        self.width = len(self.title)
        for job in jobs:
            entry = job.get_entry(self.title)
            if isinstance(entry, str):
                width = len(entry)
                self.width = self.width if self.width > width else width

        self.width += 2  # add some boarder

    def format_title(self) -> str:
        result = self.format_entry(self.title)
        return click.style(result, bold=True)

    def format_job(self, job: Job) -> str:
        value = job.get_entry(self.title)
        return self.format_entry(
            *self.color_function(value)
        )

    def format_entry(self, entry: str, color: str = None) -> str:
        '''
        Format the entry to match width, alignment, and color request
        If no color is supplied, will just return string
        If supplied, use click.style to change fg
        If the entry is longer than self.width, truncate the end
        '''
        if self.width is None:
            raise ValueError(f'Attempting to format {self.title} '
                             'with unset width!')

        entry = entry[:self.width]
        result = (f'{{:{self.alignment}{self.width}}}').format(entry)
        if color:
            result = click.style(result, fg=color)
        return result


def color_state(value) -> str:
    state_colors = {
        'FAILED': 'red',
        'TIMEOUT': 'red',
        'OUT_OF_MEMORY': 'red',
        'RUNNING': 'cyan',
        'CANCELLED': 'yellow',
        'COMPLETED': 'green',
        'PENDING': 'blue',
    }
    return value, state_colors.get(value, None)


def render_eff(value: float, target_type: str) -> str:
    '''
    Return a styled string for efficiency values
    '''
    color_maps = {
        'mid': color_mid,
        'high': color_high
    }
    if value == '---':
        color = None
    else:
        color = color_maps[target_type](value)
        value = f'{value}%'
    return value, color


def color_mid(value: float) -> str:
    '''
    Determine color for efficiency value where "mid" values are the target
    '''
    if value < 20 or value > 90:
        return 'red'
    elif value > 60:
        return 'green'
    else:
        return None


def color_high(value: float) -> str:
    '''
    Determine color for efficiency value where "high" values are the target
    '''
    if value < 20:
        return 'red'
    elif value > 80:
        return 'green'
    else:
        return None
