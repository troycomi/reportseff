from reportseff import output_renderer
import pytest
import click


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
    assert renderer.formatters == [
        output_renderer.Column_Formatter('JobID%>'),
        output_renderer.Column_Formatter('State'),
    ]
    assert sorted(renderer.query_columns) == sorted((
        'JobID JobIDRaw State').split())

    renderer = output_renderer.Output_Renderer(min_required, 'TotalCPU%<5')
    assert renderer.formatters == [
        output_renderer.Column_Formatter('JobID%>'),
        output_renderer.Column_Formatter('State'),
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


def test_renderer_add_included(renderer):
    renderer.formatters = []
    renderer.add_included()
    assert renderer.formatters == [
        output_renderer.Column_Formatter('JobID%>'),
        output_renderer.Column_Formatter('State'),
    ]

    # won't overwrite formatting, state comes first
    renderer.formatters = renderer.build_formatters('JobID,Test%<10')
    renderer.add_included()
    assert renderer.formatters == [
        output_renderer.Column_Formatter('State'),
        output_renderer.Column_Formatter('JobID'),
        output_renderer.Column_Formatter('Test%<10'),
    ]


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

    # with ignor other % things
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
    fmt = output_renderer.Column_Formatter('Name')
    # matches title
    fmt.compute_width('tes tin g'.split())
    assert fmt.width == 4

    # already set
    fmt.compute_width('aLongEntry andAnother'.split())
    assert fmt.width == 4

    fmt = output_renderer.Column_Formatter('Name')
    fmt.compute_width('aLongEntry andAnother'.split())
    assert fmt.width == 10


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
