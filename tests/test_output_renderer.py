from reportseff import output_renderer
from reportseff import job as job_module
import pytest
import click
import re


min_required = ('JobID,State,Elapsed,JobIDRaw,State,TotalCPU,AllocCPUS,'
                'REQMEM,NNodes,MaxRSS,Timelimit').split(',')


@pytest.fixture
def renderer():
    # default renderer with valid names for only default string
    return output_renderer.Output_Renderer(min_required)


def test_renderer_init(renderer):
    assert renderer.formatters == [
        output_renderer.Column_Formatter('JobID%>'),
        output_renderer.Column_Formatter('State'),
        output_renderer.Column_Formatter('Elapsed%>'),
        output_renderer.Column_Formatter('CPUEff'),
        output_renderer.Column_Formatter('MemEff'),
    ]
    assert sorted(renderer.query_columns) == sorted((
        'JobID JobIDRaw State Elapsed TotalCPU AllocCPUS REQMEM '
        'NNodes MaxRSS').split())

    renderer = output_renderer.Output_Renderer(min_required, '')
    assert renderer.formatters == []
    assert sorted(renderer.query_columns) == sorted((
        'JobID JobIDRaw State').split())

    renderer = output_renderer.Output_Renderer(min_required, 'TotalCPU%<5')
    assert renderer.formatters == [
        output_renderer.Column_Formatter('TotalCPU%<5'),
    ]
    assert sorted(renderer.query_columns) == sorted((
        'JobID JobIDRaw State TotalCPU').split())


def test_renderer_build_formatters(renderer):
    assert renderer.build_formatters('Name,Name%>,Name%10,Name%<10') == [
        output_renderer.Column_Formatter('Name'),
        output_renderer.Column_Formatter('Name%>'),
        output_renderer.Column_Formatter('Name%10'),
        output_renderer.Column_Formatter('Name%<10'),
    ]

    assert renderer.build_formatters('jobid,state,elapsed') == [
        'jobid', 'state', 'elapsed']

    assert renderer.build_formatters('') == []


def test_renderer_validate_formatters(renderer):
    renderer.formatters = renderer.build_formatters('JobID,JOBid,jObId')
    assert renderer.validate_formatters(['JobID']) == \
        'JobID JobID JobID'.split()
    assert renderer.formatters == 'JobID JobID JobID'.split()


def test_renderer_correct_columns(renderer):
    renderer.query_columns = ['JobID']
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == \
        sorted('JobID JobIDRaw State'.split())

    renderer.query_columns = 'JobID CPUEff MemEff TimeEff'.split()
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == sorted((
        'JobID TotalCPU Elapsed REQMEM'
        ' JobIDRaw State'
        ' NNodes AllocCPUS MaxRSS Timelimit').split())

    renderer.query_columns = 'JobID JobID JobID'.split()
    renderer.correct_columns()
    assert sorted(renderer.query_columns) == \
        sorted('JobID JobIDRaw State'.split())


def test_renderer_format_jobs():
    renderer = output_renderer.Output_Renderer(
        min_required,
        'JobID,State,Elapsed,CPUEff,REQMEM,TimeEff')
    jobs = []
    job = job_module.Job('24371655', '24371655', None)
    job.update({
        'JobID': '24371655',
        'State': 'COMPLETED',
        'AllocCPUS': '1',
        'REQMEM': '1Gn',
        'TotalCPU': '00:09:00',
        'Elapsed': '00:10:00',
        'Timelimit': '00:20:00',
        'MaxRSS': '',
        'NNodes': '1',
        'NTasks': ''
    })
    jobs.append(job)
    job = job_module.Job('24371656', '24371656', None)
    job.update({
        'JobID': '24371656',
        'State': 'PENDING',
        'AllocCPUS': '1',
        'REQMEM': '1Gn',
        'TotalCPU': '00:09:00',
        'Elapsed': '00:10:00',
        'Timelimit': '00:20:00',
        'MaxRSS': '',
        'NNodes': '1',
        'NTasks': ''
    })
    jobs.append(job)
    job = job_module.Job('24371657', '24371657', None)
    job.update({
        'JobID': '24371657',
        'State': 'RUNNING',
        'AllocCPUS': '1',
        'REQMEM': '1Gn',
        'TotalCPU': '00:09:00',
        'Elapsed': '00:10:00',
        'Timelimit': '00:20:00',
        'MaxRSS': '',
        'NNodes': '1',
        'NTasks': ''
    })
    jobs.append(job)
    job = job_module.Job('24371658', '24371658', None)
    job.update({
        'JobID': '24371658',
        'State': 'CANCELLED',
        'AllocCPUS': '1',
        'REQMEM': '1Gn',
        'TotalCPU': '00:09:00',
        'Elapsed': '00:00:00',
        'Timelimit': '00:20:00',
        'MaxRSS': '',
        'NNodes': '1',
        'NTasks': ''
    })
    jobs.append(job)
    job = job_module.Job('24371659', '24371659', None)
    job.update({
        'JobID': '24371659',
        'State': 'TIMEOUT',
        'AllocCPUS': '1',
        'REQMEM': '2Gn',
        'TotalCPU': '00:04:00',
        'Elapsed': '00:21:00',
        'Timelimit': '00:20:00',
        'MaxRSS': '',
        'NNodes': '1',
        'NTasks': ''
    })
    jobs.append(job)
    job = job_module.Job('24371660', '24371660', None)
    job.update({
        'JobID': '24371660',
        'State': 'OTHER',
        'AllocCPUS': '1',
        'REQMEM': '2Gn',
        'TotalCPU': '00:09:00',
        'Elapsed': '00:12:05',
        'Timelimit': '00:20:00',
        'MaxRSS': '',
        'NNodes': '1',
        'NTasks': ''
    })
    jobs.append(job)
    result = renderer.format_jobs(jobs)
    ansi_escape = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')
    # check removed codes
    codes = ansi_escape.findall(result)
    for i, c in enumerate(codes):
        print(f'{i} -> {repr(c)}')
    for code in codes[1::2]:  # normal
        assert code == '\x1b[0m'
    for code in codes[0:10:20]:
        assert code == '\x1b[1m'  # bold
    for i in (22, 24, 26, 28):
        assert codes[i] == '\x1b[31m'  # red
    for i in (12, 14, 30):
        assert codes[i] == '\x1b[32m'  # green
    for i in (20,):
        assert codes[i] == '\x1b[33m'  # yellow
    for i in (16,):
        assert codes[i] == '\x1b[34m'  # blue
    for i in (18,):
        assert codes[i] == '\x1b[36m'  # cyan
    # remove color codes
    result = ansi_escape.sub('', result)
    lines = result.split('\n')
    assert lines[0].split() == \
        'JobID State Elapsed CPUEff REQMEM TimeEff'.split()
    assert lines[1].split() == \
        '24371655 COMPLETED 00:10:00 90.0% 1Gn 50.0%'.split()
    assert lines[2].split() == \
        '24371656 PENDING --- --- --- ---'.split()
    assert lines[3].split() == \
        '24371657 RUNNING 00:10:00 --- 1Gn 50.0%'.split()
    assert lines[4].split() == \
        '24371658 CANCELLED 00:00:00 --- 1Gn 0.0%'.split()
    assert lines[5].split() == \
        '24371659 TIMEOUT 00:21:00 19.0% 2Gn 105.0%'.split()
    assert lines[6].split() == \
        '24371660 OTHER 00:12:05 74.5% 2Gn 60.4%'.split()


def test_formatter_init():
    # empty
    result = output_renderer.Column_Formatter('')
    assert result.title == ''
    assert result.alignment == '^'
    assert result.width is None

    # simple name
    result = output_renderer.Column_Formatter('test')
    assert result.title == 'test'
    assert result.alignment == '^'
    assert result.width is None

    # with alignment
    result = output_renderer.Column_Formatter('test%>')
    assert result.title == 'test'
    assert result.alignment == '>'
    assert result.width is None

    # with width
    result = output_renderer.Column_Formatter('test%10')
    assert result.title == 'test'
    assert result.alignment == '^'
    assert result.width == 10

    # with both
    result = output_renderer.Column_Formatter('test%<10')
    assert result.title == 'test'
    assert result.alignment == '<'
    assert result.width == 10

    # with invalid width
    with pytest.raises(ValueError) as e:
        result = output_renderer.Column_Formatter('test%1<0')
    assert "Unable to parse format token 'test%1<0'" in str(e)

    # with ignore other % things
    result = output_renderer.Column_Formatter('test%<10%>5')
    assert result.title == 'test'
    assert result.alignment == '<'
    assert result.width == 10


def test_formatter_eq():
    fmt = output_renderer.Column_Formatter('Name')
    fmt2 = output_renderer.Column_Formatter('Name')
    fmt3 = output_renderer.Column_Formatter('Name>')
    assert fmt == fmt2
    assert fmt != fmt3

    assert fmt != list()
    assert repr(fmt) == 'Name%^None'

    assert 'Name' == fmt
    assert fmt == 'Name'
    assert fmt != 'NaMe'

    formatters = [fmt, fmt2, fmt3]
    assert 'Name' in formatters
    assert 'NAME' not in formatters


def test_formatter_validate_title():
    fmt = output_renderer.Column_Formatter('NaMe')

    with pytest.raises(ValueError) as e:
        fmt.validate_title(['JobID', 'State'])
    assert "'NaMe' is not a valid title" in str(e)

    fmt.title = 'jOBid'
    assert fmt.validate_title(['other', 'JobID', 'State']) == 'JobID'
    assert fmt.title == 'JobID'


def test_formatter_compute_width():
    fmt = output_renderer.Column_Formatter('JobID')
    # matches title
    jobs = [
        job_module.Job(None, 'tes', None),
        job_module.Job(None, 'tin', None),
        job_module.Job(None, 'g', None),
    ]
    fmt.compute_width(jobs)
    assert fmt.width == 7

    # already set
    jobs = [
        job_module.Job(None, 'aLongEntry', None),
        job_module.Job(None, 'addAnother', None),
    ]
    fmt.compute_width(jobs)
    assert fmt.width == 7

    fmt = output_renderer.Column_Formatter('JobID')
    fmt.compute_width(jobs)
    assert fmt.width == 12


def test_formatter_format_entry():
    fmt = output_renderer.Column_Formatter('Name')
    with pytest.raises(ValueError) as e:
        fmt.format_title()
    assert 'Attempting to format Name with unset width!' in str(e)

    fmt.width = 8
    assert fmt.format_title() == click.style('  Name  ', bold=True)
    fmt.alignment = '<'
    assert fmt.format_title() == click.style('Name    ', bold=True)
    fmt.alignment = '>'
    assert fmt.format_title() == click.style('    Name', bold=True)

    assert fmt.format_entry('A Long Entry') == 'A Long E'
    assert fmt.format_entry('A Long Entry', 'green') == \
        click.style('A Long E', fg='green')
