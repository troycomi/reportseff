import pytest
from reportseff import console
from reportseff.job_collection import Job_Collection
from reportseff.output_renderer import Output_Renderer
from reportseff.db_inquirer import Sacct_Inquirer
from click.testing import CliRunner


@pytest.fixture
def mock_inquirer(mocker):
    def mock_valid(self):
        return ('JobID,State,Elapsed,JobIDRaw,State,TotalCPU,AllocCPUS,'
                'REQMEM,NNodes,MaxRSS,Timelimit').split(',')
    mocker.patch.object(Sacct_Inquirer, 'get_valid_formats', new=mock_valid)


def test_directory_input(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '1|01:27:42|24418435|24418435||1|1Gn|'
        'COMPLETED|03:00:00|01:27:29\n'
        '1|01:27:42|24418435.batch|24418435.batch|499092K|1|1Gn|'
        'COMPLETED||01:27:29\n'
        '1|01:27:42|24418435.extern|24418435.extern|1376K|1|1Gn|'
        'COMPLETED||00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)

    def set_jobs(self, directory):
        self.set_jobs(('24418435',))

    mocker.patch.object(Job_Collection, 'set_out_dir', new=set_jobs)
    result = runner.invoke(console.main,
                           '--no-color --format '
                           '"JobID,State,Elapsed,TimeEff,CPUEff,MemEff"')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:]
    assert output[0].split() == [
        '24418435', 'COMPLETED', '01:27:42', '48.7%', '99.8%', '47.7%'
    ]


def test_directory_input_exception(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
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
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)

    def set_jobs(self, directory):
        raise ValueError('Testing EXCEPTION')

    mocker.patch.object(Job_Collection, 'set_out_dir', new=set_jobs)
    result = runner.invoke(console.main,
                           '--no-color')

    assert result.exit_code == 1
    assert 'Testing EXCEPTION' in result.output


def test_debug_option(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '16|00:00:00|23000233|23000233||1|4000Mc|'
        'CANCELLED by 129319|6-00:00:00|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--format '
                           '"JobID,State,Elapsed,TimeEff,CPUEff,MemEff" '
                           '--no-color --debug 23000233')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')
    assert output[0] == (
        '16|00:00:00|23000233|23000233||1|4000Mc|'
        'CANCELLED by 129319|6-00:00:00|00:00:00'
    )
    assert output[3].split() == [
        '23000233', 'CANCELLED', '00:00:00', '0.0%', '---', '0.0%'
    ]


def test_process_failure(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '16|00:00:00|23000233|23000233||1|4000Mc|'
        'CANCELLED by 129319|6-00:00:00|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    mocker.patch.object(Job_Collection,
                        'process_entry',
                        side_effect=Exception('TESTING'))
    result = runner.invoke(console.main,
                           '--no-color 23000233')

    assert result.exit_code != 0
    # remove header
    output = result.output.split('\n')
    assert output[0] == 'Error processing entry: ' + (
        "{'AllocCPUS': '16', 'Elapsed': '00:00:00', 'JobID': '23000233', "
        "'JobIDRaw': '23000233', 'MaxRSS': '', 'NNodes': '1', "
        "'REQMEM': '4000Mc', 'State': 'CANCELLED by 129319', "
        "'TotalCPU': '6-00:00:00'}"
    )


def test_short_output(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '23000233|23000233|CANCELLED by 129319|16|'
        '00:00:00|00:00:00|6-00:00:00|4000Mc||1|\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    mocker.patch('reportseff.console.len', return_value=20)
    mocker.patch.object(Output_Renderer,
                        'format_jobs',
                        return_value='output')

    mock_click = mocker.patch('reportseff.console.click.echo')
    result = runner.invoke(console.main,
                           '--no-color 23000233')

    assert result.exit_code == 0
    mock_click.assert_called_once_with('output', color=False)


def test_long_output(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '16|00:00:00|23000233|23000233||1|4000Mc|'
        'CANCELLED by 129319|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    mocker.patch('reportseff.console.len', return_value=21)
    mocker.patch.object(Output_Renderer,
                        'format_jobs',
                        return_value='output')
    mock_click = mocker.patch('reportseff.console.click.echo_via_pager')
    result = runner.invoke(console.main,
                           '--no-color 23000233')

    assert result.exit_code == 0
    mock_click.assert_called_once_with('output', color=False)


def test_simple_job(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '1|01:27:42|24418435|24418435||1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.batch|24418435.batch|499092K|1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.extern|24418435.extern|1376K|1|1Gn|'
        'COMPLETED|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color 24418435')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:]
    assert output[0].split() == [
        '24418435', 'COMPLETED', '01:27:42', '99.8%', '47.7%'
    ]


def test_simple_user(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '1|01:27:42|24418435|24418435||1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.batch|24418435.batch|499092K|1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.extern|24418435.extern|1376K|1|1Gn|'
        'COMPLETED|00:00:00\n'
        '1|21:14:48|25569410|25569410||1|4000Mc|COMPLETED|19:28:36\n'
        '1|21:14:49|25569410.extern|25569410.extern|1548K|1|4000Mc|'
        'COMPLETED|00:00:00\n'
        '1|21:14:43|25569410.0|25569410.0|62328K|1|4000Mc|COMPLETED|19:28:36\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color --user test')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:]
    assert output[0].split() == [
        '24418435', 'COMPLETED', '01:27:42', '99.8%', '47.7%'
    ]
    assert output[1].split() == [
        '25569410', 'COMPLETED', '21:14:48', '91.7%', '1.6%'
    ]


def test_format_add(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    mock_jobs = mocker.patch('reportseff.console.get_jobs',
                             return_value=('Testing', 1))
    result = runner.invoke(console.main,
                           '--no-color --format=test')

    assert result.exit_code == 0
    mock_jobs.call_args[1] == 'test'

    # test adding onto end
    result = runner.invoke(console.main,
                           '--no-color --format=+test')

    assert result.exit_code == 0
    mock_jobs.call_args[1] == 'JobID%>,State,Elapsed%>,CPUEff,MemEff,test'


def test_since(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '1|01:27:42|24418435|24418435||1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.batch|24418435.batch|499092K|1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.extern|24418435.extern|1376K|1|1Gn|'
        'COMPLETED|00:00:00\n'
        '1|21:14:48|25569410|25569410||1|4000Mc|COMPLETED|19:28:36\n'
        '1|21:14:49|25569410.extern|25569410.extern|1548K|1|4000Mc|'
        'COMPLETED|00:00:00\n'
        '1|21:14:43|25569410.0|25569410.0|62328K|1|4000Mc|COMPLETED|19:28:36\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color --since 200406 24418435 25569410')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:]
    assert output[0].split() == [
        '24418435', 'COMPLETED', '01:27:42', '99.8%', '47.7%'
    ]
    assert output[1].split() == [
        '25569410', 'COMPLETED', '21:14:48', '91.7%', '1.6%'
    ]


def test_simple_state(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '1|01:27:42|24418435|24418435||1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.batch|24418435.batch|499092K|1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.extern|24418435.extern|1376K|1|1Gn|'
        'COMPLETED|00:00:00\n'
        '1|21:14:48|25569410|25569410||1|4000Mc|RUNNING|19:28:36\n'
        '1|21:14:49|25569410.extern|25569410.extern|1548K|1|4000Mc|'
        'RUNNING|00:00:00\n'
        '1|21:14:43|25569410.0|25569410.0|62328K|1|4000Mc|RUNNING|19:28:36\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color --state completed '
                           '25569410 24418435'
                           )

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:]
    assert output[0].split() == [
        '24418435', 'COMPLETED', '01:27:42', '99.8%', '47.7%'
    ]
    # other is suppressed by state filter
    assert output[1].split() == []


def test_no_state(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '1|01:27:42|24418435|24418435||1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.batch|24418435.batch|499092K|1|1Gn|'
        'COMPLETED|01:27:29\n'
        '1|01:27:42|24418435.extern|24418435.extern|1376K|1|1Gn|'
        'COMPLETED|00:00:00\n'
        '1|21:14:48|25569410|25569410||1|4000Mc|RUNNING|19:28:36\n'
        '1|21:14:49|25569410.extern|25569410.extern|1548K|1|4000Mc|'
        'RUNNING|00:00:00\n'
        '1|21:14:43|25569410.0|25569410.0|62328K|1|4000Mc|RUNNING|19:28:36\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color --state ZZ '
                           '25569410 24418435'
                           )

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')
    assert output[0] == 'Unknown state ZZ'
    assert output[1] == 'No valid states provided'
    assert output[2].split() == [
        'JobID', 'State', 'Elapsed', 'CPUEff', 'MemEff'
    ]
    assert output[3] == ''


def test_array_job_raw_id(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '1|00:09:34|24220929_421|24221219||1|16000Mn|'
        'COMPLETED|09:28.052\n'
        '1|00:09:34|24220929_421.batch|24221219.batch|5664932K|1|16000Mn|'
        'COMPLETED|09:28.051\n'
        '1|00:09:34|24220929_421.extern|24221219.extern|1404K|1|16000Mn|'
        'COMPLETED|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color 24221219')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-1]
    assert output[0].split() == [
        '24220929_421', 'COMPLETED', '00:09:34', '99.0%', '34.6%'
    ]
    assert len(output) == 1


def test_array_job_single(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '1|00:09:34|24220929_421|24221219||1|16000Mn|'
        'COMPLETED|09:28.052\n'
        '1|00:09:34|24220929_421.batch|24221219.batch|5664932K|1|16000Mn|'
        'COMPLETED|09:28.051\n'
        '1|00:09:34|24220929_421.extern|24221219.extern|1404K|1|16000Mn|'
        'COMPLETED|00:00:00\n'
        '1|00:09:33|24220929_431|24221220||1|16000Mn|'
        'PENDING|09:27.460\n'
        '1|00:09:33|24220929_431.batch|24221220.batch|5518572K|1|16000Mn|'
        'PENDING|09:27.459\n'
        '1|00:09:33|24220929_431.extern|24221220.extern|1400K|1|16000Mn|'
        'PENDING|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color 24220929_421')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-1]
    assert output[0].split() == [
        '24220929_421', 'COMPLETED', '00:09:34', '99.0%', '34.6%'
    ]
    assert len(output) == 1


def test_array_job_base(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '1|00:09:34|24220929_421|24221219||1|16000Mn|'
        'COMPLETED|09:28.052\n'
        '1|00:09:34|24220929_421.batch|24221219.batch|5664932K|1|16000Mn|'
        'COMPLETED|09:28.051\n'
        '1|00:09:34|24220929_421.extern|24221219.extern|1404K|1|16000Mn|'
        'COMPLETED|00:00:00\n'
        '1|00:09:33|24220929_431|24221220||1|16000Mn|'
        'PENDING|09:27.460\n'
        '1|00:09:33|24220929_431.batch|24221220.batch|5518572K|1|16000Mn|'
        'PENDING|09:27.459\n'
        '1|00:09:33|24220929_431.extern|24221220.extern|1400K|1|16000Mn|'
        'PENDING|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color 24220929')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-1]
    assert output[0].split() == [
        '24220929_421', 'COMPLETED', '00:09:34', '99.0%', '34.6%'
    ]
    assert output[1].split() == [
        '24220929_431', 'PENDING', '---', '---', '---'
    ]
    assert len(output) == 2


def test_sacct_error(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 1
    sub_result.stdout = (
        ''
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color 9999999')

    assert result.exit_code == 1
    assert 'Error running sacct!' in result.output


def test_empty_sacct(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        ''
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color 9999999')

    assert result.exit_code == 0
    output = result.output.split('\n')[:-1]
    assert output[0].split() == [
        'JobID', 'State', 'Elapsed', 'CPUEff', 'MemEff'
    ]
    assert len(output) == 1


def test_failed_no_mem(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '8|00:00:12|23000381|23000381||1|4000Mc|FAILED|00:00:00\n'
        '8|00:00:12|23000381.batch|23000381.batch||1|4000Mc|'
        'FAILED|00:00:00\n'
        '8|00:00:12|23000381.extern|23000381.extern|1592K|1|4000Mc|'
        'COMPLETED|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color 23000381')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-1]
    assert output[0].split() == [
        '23000381', 'FAILED', '00:00:12', '---', '0.0%'
    ]
    assert len(output) == 1


def test_canceled_by_other(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '16|00:00:00|23000233|23000233||1|4000Mc|'
        'CANCELLED by 129319|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color 23000233')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-1]
    assert output[0].split() == [
        '23000233', 'CANCELLED', '00:00:00', '---', '0.0%'
    ]
    assert len(output) == 1


def test_zero_runtime(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=True)
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '8|00:00:00|23000210|23000210||1|20000Mn|'
        'FAILED|00:00.007\n'
        '8|00:00:00|23000210.batch|23000210.batch|1988K|1|20000Mn|'
        'FAILED|00:00.006\n'
        '8|00:00:00|23000210.extern|23000210.extern|1556K|1|20000Mn|'
        'COMPLETED|00:00:00\n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(console.main,
                           '--no-color 23000210')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-1]
    assert output[0].split() == [
        '23000210', 'FAILED', '00:00:00', '---', '0.0%'
    ]
    assert len(output) == 1


def test_no_systems(mocker, mock_inquirer):
    mocker.patch('reportseff.console.which', return_value=None)
    runner = CliRunner()
    result = runner.invoke(console.main,
                           '--no-color 23000210')

    assert result.exit_code == 1
    # remove header
    output = result.output.split('\n')
    assert output[0] == 'No supported scheduling systems found!'
