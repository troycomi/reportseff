from reportseff import job as job_module
import pytest
import click


@pytest.fixture
def job():
    return job_module.Job('job', 'jobid', 'filename')


def test_job_init(job):
    assert job.job == 'job'
    assert job.jobid == 'jobid'
    assert job.filename == 'filename'
    assert job.stepmem == 0
    assert job.totalmem is None
    assert job.time == '---'
    assert job.cpu == '---'
    assert job.mem == '---'
    assert job.state is None


def test_update_main_job():
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
    assert job.state == 'COMPLETED'
    assert job.time == '00:10:00'
    assert job.time_eff == 50.0
    assert job.cpu == 90.0
    assert job.totalmem == 1*1024**2

    job = job_module.Job('24371655', '24371655', None)
    job.update({
        'JobID': '24371655',
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
    assert job.state == 'PENDING'
    assert job.time == '---'
    assert job.time_eff == '---'
    assert job.cpu == '---'
    assert job.totalmem is None

    job = job_module.Job('24371655', '24371655', None)
    job.update({
        'JobID': '24371655',
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
    assert job.state == 'RUNNING'
    assert job.time == '00:10:00'
    assert job.time_eff == '---'
    assert job.cpu == '---'
    assert job.totalmem is None


def test_update_part_job():
    job = job_module.Job('24371655', '24371655', None)
    job.update({
        'JobID': '24371655.batch',
        'State': 'COMPLETED',
        'AllocCPUS': '1',
        'REQMEM': '1Gn',
        'TotalCPU': '00:09:00',
        'Elapsed': '00:10:00',
        'MaxRSS': '495644K',
        'NNodes': '1',
        'NTasks': ''
    })
    assert job.state is None
    assert job.time == '---'
    assert job.cpu == '---'
    assert job.totalmem is None
    assert job.stepmem == 495644


def test_name(job):
    assert job.name() == 'filename'
    job = job_module.Job('job', 'jobid', None)
    assert job.name() == 'jobid'


def test_render_eff():
    assert job_module.render_eff(19, 'high') == \
        click.style('   19%   ', fg='red')
    assert job_module.render_eff(20, 'high') == \
        click.style('   20%   ', fg=None)
    assert job_module.render_eff(80, 'high') == \
        click.style('   80%   ', fg=None)
    assert job_module.render_eff(81, 'high') == \
        click.style('   81%   ', fg='green')

    assert job_module.render_eff(19, 'mid') == \
        click.style('   19%   ', fg='red')
    assert job_module.render_eff(20, 'mid') == \
        click.style('   20%   ', fg=None)
    assert job_module.render_eff(60, 'mid') == \
        click.style('   60%   ', fg=None)
    assert job_module.render_eff(61, 'mid') == \
        click.style('   61%   ', fg='green')
    assert job_module.render_eff(90, 'mid') == \
        click.style('   90%   ', fg='green')
    assert job_module.render_eff(91, 'mid') == \
        click.style('   91%   ', fg='red')

    with pytest.raises(ValueError) as e:
        job_module.render_eff(99, 'test')

    assert 'Unsupported target type: test' in str(e)


def test_color_high():
    assert job_module.color_high(19) == 'red'
    assert job_module.color_high(20) is None
    assert job_module.color_high(80) is None
    assert job_module.color_high(81) == 'green'


def test_color_mid():
    assert job_module.color_mid(19) == 'red'
    assert job_module.color_mid(20) is None
    assert job_module.color_mid(60) is None
    assert job_module.color_mid(61) == 'green'
    assert job_module.color_mid(90) == 'green'
    assert job_module.color_mid(91) == 'red'


def test_parse_slurm_timedelta():
    timestamps = ['01-03:04:02', '03:04:02', '04:02.123']
    expected_seconds = [97442, 11042, 242]
    for i, t in enumerate(timestamps):
        assert job_module._parse_slurm_timedelta(t) == expected_seconds[i]


def test_parsemem_nodes():
    for mem in (1, 2, 4):
        for exp, multiple in enumerate('K M G T E'.split()):
            for alloc in (1, 2, 4):
                assert job_module.parsemem(f'{mem}{multiple}n', alloc, -1) == \
                    mem * 1024 ** exp * alloc, f'{mem}{multiple}n'


def test_parsemem_cpus():
    for mem in (1, 2, 4):
        for exp, multiple in enumerate('K M G T E'.split()):
            for alloc in (1, 2, 4):
                assert job_module.parsemem(f'{mem}{multiple}c', -1, alloc) == \
                    mem * 1024 ** exp * alloc, f'{mem}{multiple}c'


def test_parsememstep():
    for exp, multiple in enumerate('K M G T E'.split()):
        for mem in (2, 4, 6):
            assert job_module.parsememstep(f'{mem}{multiple}') == \
                mem * 1024 ** exp

    with pytest.raises(ValueError) as e:
        job_module.parsememstep('18GG')
    assert 'Unexpected memstep format: 18GG' in str(e)

    assert job_module.parsememstep('') == 0
