from reportseff import job_collection
from reportseff.job import Job
import pytest


@pytest.fixture
def jobs():
    return job_collection.Job_Collection()


def test_get_slurm_format(jobs):
    assert jobs.get_slurm_format() == (
        'JobIDRaw,JobID,State,AllocCPUS,REQMEM,TotalCPU,Elapsed,MaxRSS,'
        'NNodes,NTasks'
    )


def test_get_slurm_jobs(jobs):
    assert jobs.get_slurm_jobs() == ''

    jobs.jobs = {
        '1_1': Job('1', '1_1', None),
        '1_2': Job('1', '1_2', None),
        '2_2': Job('2', '2_2', None),
    }
    assert jobs.get_slurm_jobs() == '1,2'


def test_set_slurm_out_dir_dir_handling(jobs, mocker):
    # dir handling
    mock_cwd = mocker.patch('reportseff.job_collection.os.getcwd',
                            return_value='/dir/path/')
    mock_real = mocker.patch('reportseff.job_collection.os.path.realpath',
                             return_value='/dir/path2/')
    mock_exists = mocker.patch('reportseff.job_collection.os.path.exists',
                               return_value=False)

    with pytest.raises(ValueError) as e:
        jobs.set_slurm_out_dir('')
    assert '/dir/path/ does not exist!' in str(e)
    mock_cwd.assert_called_once()
    mock_real.assert_not_called()
    mock_exists.assert_called_once_with('/dir/path/')

    mock_cwd.reset_mock()
    mock_real.reset_mock()
    mock_exists.reset_mock()

    with pytest.raises(ValueError) as e:
        jobs.set_slurm_out_dir('pwd')
    assert '/dir/path2/ does not exist!' in str(e)
    mock_cwd.assert_not_called()
    mock_real.assert_called_once_with('pwd')
    mock_exists.assert_called_once_with('/dir/path2/')


def test_set_slurm_jobs(jobs):
    with pytest.raises(ValueError) as e:
        jobs.set_slurm_jobs(())
    assert 'No valid slurm jobs provided!' in str(e)

    with pytest.raises(ValueError) as e:
        jobs.set_slurm_jobs(('asdf', 'qwer', 'zxcv', 'asdf1.1.1'))
    assert 'No valid slurm jobs provided!' in str(e)

    jobs.set_slurm_jobs(('asdf', '1', '1_1', 'asdf_1_2', '1_asdf_2'))
    assert jobs.jobs == {
        '1': Job('1', '1', None),
        '1_1': Job('1', '1_1', None),
        '1_2': Job('1', '1_2', 'asdf_1_2'),
        '2': Job('2', '2', '1_asdf_2'),
    }


def test_process_line(jobs, mocker):
    jobs.jobs = {
        '24371655': Job('24371655', '24371655', 'test_24371655')
    }
    mock_update = mocker.patch.object(Job, 'update')
    jobs.process_line('24371655|24371655|COMPLETED|1|1Gn|'
                      '01:29:47|01:29:56||1|')
    jobs.process_line('24371655.batch|24371655.batch|COMPLETED|1|1Gn|'
                      '01:29:47|01:29:56|495644K|1|1')
    jobs.process_line('24371655.extern|24371655.extern|COMPLETED|1|1Gn|'
                      '00:00:00|01:29:56|1372K|1|1')

    assert mock_update.call_args_list == [
        mocker.call({
            'JobIDRaw': '24371655',
            'JobID': '24371655',
            'State': 'COMPLETED',
            'AllocCPUS': '1',
            'REQMEM': '1Gn',
            'TotalCPU': '01:29:47',
            'Elapsed': '01:29:56',
            'MaxRSS': '',
            'NNodes': '1',
            'NTasks': ''
        }),
        mocker.call({
            'JobIDRaw': '24371655.batch',
            'JobID': '24371655.batch',
            'State': 'COMPLETED',
            'AllocCPUS': '1',
            'REQMEM': '1Gn',
            'TotalCPU': '01:29:47',
            'Elapsed': '01:29:56',
            'MaxRSS': '495644K',
            'NNodes': '1',
            'NTasks': '1'
        }),
        mocker.call({
            'JobIDRaw': '24371655.extern',
            'JobID': '24371655.extern',
            'State': 'COMPLETED',
            'AllocCPUS': '1',
            'REQMEM': '1Gn',
            'TotalCPU': '00:00:00',
            'Elapsed': '01:29:56',
            'MaxRSS': '1372K',
            'NNodes': '1',
            'NTasks': '1'
        }),
    ]


def test_set_slurm_out_dir(jobs, mocker):
    mocker.patch('reportseff.job_collection.os.path.realpath',
                 side_effect=lambda x: f'/dir/path2/{x}')
    mocker.patch('reportseff.job_collection.os.path.exists',
                 return_value=True)
    mocker.patch('reportseff.job_collection.os.path.isfile',
                 return_value=True)

    mocker.patch('reportseff.job_collection.os.listdir',
                 return_value=[])
    with pytest.raises(ValueError) as e:
        jobs.set_slurm_out_dir('test')
    assert '/dir/path2/test contains no files!' in str(e)

    mocker.patch('reportseff.job_collection.os.listdir',
                 return_value=['asdf'])
    with pytest.raises(ValueError) as e:
        jobs.set_slurm_out_dir('test')
    assert '/dir/path2/test contains no valid slurm outputs!' in str(e)

    mocker.patch('reportseff.job_collection.os.listdir',
                 return_value=['asdf',
                               'base_1',
                               'base_1_1.out',
                               'base_2_1',  # overwritten
                               'base_2_1.out'])
    jobs.set_slurm_out_dir('test')

    assert jobs.jobs == {
        '1': Job('1', '1', 'base_1'),
        '1_1': Job('1', '1_1', 'base_1_1.out'),
        '2_1': Job('2', '2_1', 'base_2_1.out'),
    }


def test_process_seff_file(jobs):
    # no matches
    jobs.process_seff_file('')
    assert jobs.jobs == {}

    jobs.process_seff_file('base_name')
    assert jobs.jobs == {}

    # simple job file
    jobs.process_seff_file('base_name_1')
    assert jobs.jobs == {
        '1': Job('1', '1', 'base_name_1')
    }

    # with .out
    jobs.process_seff_file('base_name_2.out')
    assert jobs.jobs == {
        '1': Job('1', '1', 'base_name_1'),
        '2': Job('2', '2', 'base_name_2.out')
    }

    # with array
    jobs.process_seff_file('base_name_3_1')
    assert jobs.jobs == {
        '1': Job('1', '1', 'base_name_1'),
        '2': Job('2', '2', 'base_name_2.out'),
        '3_1': Job('3', '3_1', 'base_name_3_1')
    }

    # array and .out
    jobs.process_seff_file('base_name_4_1.out')
    assert jobs.jobs == {
        '1': Job('1', '1', 'base_name_1'),
        '2': Job('2', '2', 'base_name_2.out'),
        '3_1': Job('3', '3_1', 'base_name_3_1'),
        '4_1': Job('4', '4_1', 'base_name_4_1.out')
    }

    jobs.jobs = {}
    # slight mistakes
    jobs.process_seff_file('base_name_4_1out')
    assert jobs.jobs == {}

    jobs.process_seff_file('base_name4_1.out')
    assert jobs.jobs == {
        '1': Job('1', '1', 'base_name4_1.out')
    }

    jobs.process_seff_file('base_name_0_4_1.out')
    assert jobs.jobs == {
        '1': Job('1', '1', 'base_name4_1.out'),
        '4_1': Job('4', '4_1', 'base_name_0_4_1.out'),
    }

    jobs.jobs = {}
    # default slurm out files
    jobs.process_seff_file('slurm-10.out')
    assert jobs.jobs == {
        '10': Job('10', '10', 'slurm-10.out')
    }
    jobs.process_seff_file('slurm-10_2.out')
    assert jobs.jobs == {
        '10': Job('10', '10', 'slurm-10.out'),
        '10_2': Job('10', '10_2', 'slurm-10_2.out')
    }


def test_add_job(jobs):
    jobs.add_job('j1', 'jid1')
    assert jobs.jobs == {
        'jid1': Job('j1', 'jid1', None)
    }

    # overwrite based on jobid
    jobs.add_job('j2', 'jid1', 'file')
    assert jobs.jobs == {
        'jid1': Job('j2', 'jid1', 'file')
    }

    # another job, different jid
    jobs.add_job('j2', 'jid2', 'file')
    assert jobs.jobs == {
        'jid1': Job('j2', 'jid1', 'file'),
        'jid2': Job('j2', 'jid2', 'file')
    }
