from reportseff import job as job_module
import pytest


@pytest.fixture
def job():
    return job_module.Job('job', 'jobid', 'filename')


def test_eq():
    job1 = job_module.Job('j1', 'j1', 'filename')
    job2 = job_module.Job('j1', 'j1', 'filename')
    assert job1 == job2

    job2 = job_module.Job('j2', 'j1', 'filename')
    assert job1 != job2
    job2 = dict()
    assert job1 != job2


def test_repr():
    job1 = job_module.Job('j1', 'jid1', 'filename')
    assert repr(job1) == 'Job(job=j1, jobid=jid1, filename=filename)'

    job2 = job_module.Job('j2', 'jid2', None)
    assert repr(job2) == 'Job(job=j2, jobid=jid2, filename=None)'


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
    assert job.time_eff == 50.0
    assert job.cpu == '---'
    assert job.totalmem is None

    job = job_module.Job('24371655', '24371655', None)
    job.update({
        'JobID': '24371655',
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
    assert job.state == 'CANCELLED'
    assert job.time == '00:00:00'
    assert job.time_eff == 0.0
    assert job.cpu is None
    assert job.totalmem == 1024**2


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


def test_parse_bug():
    job = job_module.Job('24371655', '24371655', None)
    job.update({
        'AllocCPUS': '1',
        'Elapsed': '00:00:19',
        'JobID': '34853801.extern',
        'JobIDRaw': '34853801.extern',
        'JobName': 'extern',
        'MaxRSS': '0',
        'NNodes': '1',
        'REQMEM': '4Gn',
        'State': 'COMPLETED',
        'Timelimit': '',
        'TotalCPU': '00:00:00',
    })


def test_name(job):
    assert job.name() == 'filename'
    job = job_module.Job('job', 'jobid', None)
    assert job.name() == 'jobid'


def test_get_entry(job):
    job.state = 'TEST'
    assert job.get_entry('JobID') == 'filename'
    assert job.get_entry('State') == 'TEST'
    assert job.get_entry('MemEff') == '---'
    assert job.get_entry('TimeEff') == '---'
    assert job.get_entry('CPUEff') == '---'
    assert job.get_entry('undefined') == '---'

    job = job_module.Job('24371655', '24371655', None)
    job.update({
        'JobID': '24371655',
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
    assert job.get_entry('JobID') == '24371655'
    assert job.get_entry('State') == 'CANCELLED'
    assert job.get_entry('MemEff') == 0.0
    assert job.get_entry('TimeEff') == 0.0
    assert job.get_entry('CPUEff') == '---'
    assert job.get_entry('undefined') == '---'
    assert job.get_entry('Elapsed') == '00:00:00'


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
