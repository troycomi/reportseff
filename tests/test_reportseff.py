from reportseff import reportseff
from reportseff.job_collection import Job_Collection
from click.testing import CliRunner


def test_directory_input(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24418435|24418435|COMPLETED|1|'
        '01:27:29|01:27:42|03:00:00|1Gn||1|\n'
        '24418435.batch|24418435.batch|COMPLETED|1|'
        '01:27:29|01:27:42||1Gn|499092K|1|1\n'
        '24418435.extern|24418435.extern|COMPLETED|1|'
        '00:00:00|01:27:42||1Gn|1376K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)

    def set_jobs(self, directory):
        self.set_slurm_jobs(('24418435',))

    mocker.patch.object(Job_Collection, 'set_slurm_out_dir', new=set_jobs)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color --directory dir')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:]
    assert output[0].split() == [
        '24418435', 'COMPLETED', '01:27:42', '48.7%', '99.8%', '47.7%'
    ]


def test_directory_input_exception(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24418435|24418435|COMPLETED|1|'
        '01:27:29|01:27:42|03:00:00|1Gn||1|\n'
        '24418435.batch|24418435.batch|COMPLETED|1|'
        '01:27:29|01:27:42||1Gn|499092K|1|1\n'
        '24418435.extern|24418435.extern|COMPLETED|1|'
        '00:00:00|01:27:42||1Gn|1376K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)

    def set_jobs(self, directory):
        raise ValueError('Testing EXCEPTION')

    mocker.patch.object(Job_Collection, 'set_slurm_out_dir', new=set_jobs)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color --directory dir')

    assert result.exit_code == 1
    assert 'Testing EXCEPTION' in result.output


def test_debug_option(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '23000233|23000233|CANCELLED by 129319|16|'
        '00:00:00|00:00:00|6-00:00:00|4000Mc||1|\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color --debug 23000233')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')
    assert output[0] == (
        '23000233|23000233|CANCELLED by 129319|16|'
        '00:00:00|00:00:00|6-00:00:00|4000Mc||1|'
    )
    assert output[3].split() == [
        '23000233', 'CANCELLED', '00:00:00', '0.0%', '---', '0.0%'
    ]


def test_process_failure(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '23000233|23000233|CANCELLED by 129319|16|'
        '00:00:00|00:00:00|6-00:00:00|4000Mc||1|\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    mocker.patch.object(Job_Collection,
                        'process_line',
                        side_effect=Exception('TESTING'))
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 23000233')

    assert result.exit_code != 0
    # remove header
    output = result.output.split('\n')
    assert output[0] == 'SACCT:'
    assert output[1] == (
        '23000233|23000233|CANCELLED by 129319|16|'
        '00:00:00|00:00:00|6-00:00:00|4000Mc||1|'
    )


def test_short_output(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '23000233|23000233|CANCELLED by 129319|16|'
        '00:00:00|00:00:00|6-00:00:00|4000Mc||1|\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    mocker.patch.object(Job_Collection,
                        'get_output',
                        return_value=('output', 20))
    mock_click = mocker.patch('reportseff.reportseff.click.echo')
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 23000233')

    assert result.exit_code == 0
    mock_click.assert_called_once_with('output', color=False)


def test_long_output(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '23000233|23000233|CANCELLED by 129319|16|'
        '00:00:00|00:00:00|6-00:00:00|4000Mc||1|\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    mocker.patch.object(Job_Collection,
                        'get_output',
                        return_value=('output', 21))
    mock_click = mocker.patch('reportseff.reportseff.click.echo_via_pager')
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 23000233')

    assert result.exit_code == 0
    mock_click.assert_called_once_with('output', color=False)


def test_simple_job(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24418435|24418435|COMPLETED|1|'
        '01:27:29|01:27:42|03:00:00|1Gn||1|\n'
        '24418435.batch|24418435.batch|COMPLETED|1|'
        '01:27:29|01:27:42||1Gn|499092K|1|1\n'
        '24418435.extern|24418435.extern|COMPLETED|1|'
        '00:00:00|01:27:42||1Gn|1376K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 24418435')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:]
    assert output[0].split() == [
        '24418435', 'COMPLETED', '01:27:42', '48.7%', '99.8%', '47.7%'
    ]


def test_array_job_raw_id(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24221219|24220929_421|COMPLETED|1|'
        '09:28.052|00:09:34|06:02:00|16000Mn||1|\n'
        '24221219.batch|24220929_421.batch|COMPLETED|1|'
        '09:28.051|00:09:34||16000Mn|5664932K|1|1\n'
        '24221219.extern|24220929_421.extern|COMPLETED|1|'
        '00:00:00|00:09:34||16000Mn|1404K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 24221219')

    assert result.exit_code == 0
    # remove header
    print(result.output)
    output = result.output.split('\n')[1:-2]
    assert output[0].split() == [
        '24220929_421', 'COMPLETED', '00:09:34', '2.6%', '99.0%', '34.6%'
    ]
    assert len(output) == 1


def test_array_job_single(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24221219|24220929_421|COMPLETED|1|'
        '09:28.052|00:09:34|06:02:00|16000Mn||1|\n'
        '24221219.batch|24220929_421.batch|COMPLETED|1|'
        '09:28.051|00:09:34||16000Mn|5664932K|1|1\n'
        '24221219.extern|24220929_421.extern|COMPLETED|1|'
        '00:00:00|00:09:34||16000Mn|1404K|1|1\n'
        '24221220|24220929_431|PENDING|1|'
        '09:27.460|00:09:33|06:02:00|16000Mn||1|\n'
        '24221220.batch|24220929_431.batch|PENDING|1|'
        '09:27.459|00:09:33||16000Mn|5518572K|1|1\n'
        '24221220.extern|24220929_431.extern|PENDING|1|'
        '00:00:00|00:09:33||16000Mn|1400K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 24220929_421')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-2]
    assert output[0].split() == [
        '24220929_421', 'COMPLETED', '00:09:34', '2.6%', '99.0%', '34.6%'
    ]
    assert len(output) == 1


def test_array_job_base(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24221219|24220929_421|COMPLETED|1|'
        '09:28.052|00:09:34|06:02:00|16000Mn||1|\n'
        '24221219.batch|24220929_421.batch|COMPLETED|1|'
        '09:28.051|00:09:34||16000Mn|5664932K|1|1\n'
        '24221219.extern|24220929_421.extern|COMPLETED|1|'
        '00:00:00|00:09:34||16000Mn|1404K|1|1\n'
        '24221220|24220929_431|PENDING|1|'
        '09:27.460|00:09:33|06:02:00|16000Mn||1|\n'
        '24221220.batch|24220929_431.batch|PENDING|1|'
        '09:27.459|00:09:33||16000Mn|5518572K|1|1\n'
        '24221220.extern|24220929_431.extern|PENDING|1|'
        '00:00:00|00:09:33||16000Mn|1400K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 24220929')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-2]
    assert output[0].split() == [
        '24220929_421', 'COMPLETED', '00:09:34', '2.6%', '99.0%', '34.6%'
    ]
    assert output[1].split() == [
        '24220929_431', 'PENDING', '---', '---', '---', '---'
    ]
    assert len(output) == 2
    # TODO have it handle array sub ids (like 242_421) as single entries
    # though sacct gives all, but have it handle the base name (242) like all


def test_sacct_error(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 1
    sub_result.stdout = (
        ''
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 9999999')

    assert result.exit_code == 1
    assert 'Error running sacct!' in result.output


def test_empty_sacct(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        ''
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 9999999')

    assert result.exit_code == 0
    output = result.output.split('\n')[:-2]
    assert output[0].split() == [
        'Name', 'State', 'Time', 'Used', 'CPU', 'Memory'
    ]
    assert len(output) == 1


def test_failed_no_mem(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '23000381|23000381|FAILED|8|00:00:00|'
        '00:00:12|2-00:00:00|4000Mc||1|\n'
        '23000381.batch|23000381.batch|FAILED|8|'
        '00:00:00|00:00:12||4000Mc||1|1\n'
        '23000381.extern|23000381.extern|COMPLETED|8|'
        '00:00:00|00:00:12||4000Mc|1592K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 23000381')

    print(result.output)
    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-2]
    assert output[0].split() == [
        '23000381', 'FAILED', '00:00:12', '0.0%', '0.0%', '0.0%'
    ]
    assert len(output) == 1


def test_canceled_by_other(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '23000233|23000233|CANCELLED by 129319|16|'
        '00:00:00|00:00:00|6-00:00:00|4000Mc||1|\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 23000233')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-2]
    assert output[0].split() == [
        '23000233', 'CANCELLED', '00:00:00', '0.0%', '---', '0.0%'
    ]
    assert len(output) == 1


def test_zero_runtime(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '23000210|23000210|FAILED|8|'
        '00:00.007|00:00:00|10:00:00|20000Mn||1|\n'
        '23000210.batch|23000210.batch|FAILED|8|'
        '00:00.006|00:00:00||20000Mn|1988K|1|1\n'
        '23000210.extern|23000210.extern|COMPLETED|8|'
        '00:00:00|00:00:00||20000Mn|1556K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 23000210')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-2]
    assert output[0].split() == [
        '23000210', 'FAILED', '00:00:00', '0.0%', '---', '0.0%'
    ]
    assert len(output) == 1
