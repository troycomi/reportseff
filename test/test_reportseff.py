from reportseff import reportseff
from click.testing import CliRunner


def test_simple_job(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24418435|24418435|COMPLETED|1|1Gn|01:27:29|01:27:42||1|\n'
        '24418435.batch|24418435.batch|COMPLETED|1|1Gn|'
        '01:27:29|01:27:42|499092K|1|1\n'
        '24418435.extern|24418435.extern|COMPLETED|1|1Gn|'
        '00:00:00|01:27:42|1376K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 24418435')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:]
    assert output[0].split() == [
        '24418435', 'COMPLETED', '01:27:42', '99.8%', '47.7%'
    ]


def test_array_job_raw_id(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24221219|24220929_421|COMPLETED|1|16000Mn|09:28.052|00:09:34||1|\n'
        '24221219.batch|24220929_421.batch|COMPLETED|1|16000Mn|'
        '09:28.051|00:09:34|5664932K|1|1\n'
        '24221219.extern|24220929_421.extern|COMPLETED|1|16000Mn|'
        '00:00:00|00:09:34|1404K|1|1\n'
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
        '24220929_421', 'COMPLETED', '00:09:34', '99.0%', '34.6%'
    ]
    assert len(output) == 1


def test_array_job_single(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24221219|24220929_421|COMPLETED|1|16000Mn|09:28.052|00:09:34||1|\n'
        '24221219.batch|24220929_421.batch|COMPLETED|1|16000Mn|'
        '09:28.051|00:09:34|5664932K|1|1\n'
        '24221219.extern|24220929_421.extern|COMPLETED|1|16000Mn|'
        '00:00:00|00:09:34|1404K|1|1\n'
        '24221220|24220929_422|COMPLETED|1|16000Mn|09:28.052|00:09:34||1|\n'
        '24221220.batch|24220929_422.batch|COMPLETED|1|16000Mn|'
        '09:28.051|00:09:34|5664932K|1|1\n'
        '24221220.extern|24220929_422.extern|COMPLETED|1|16000Mn|'
        '00:00:00|00:09:34|1404K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 24220929_421')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-2]
    assert output[0].split() == [
        '24220929_421', 'COMPLETED', '00:09:34', '99.0%', '34.6%'
    ]
    assert len(output) == 1


def test_array_job_base(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '24221219|24220929_421|COMPLETED|1|16000Mn|09:28.052|00:09:34||1|\n'
        '24221219.batch|24220929_421.batch|COMPLETED|1|16000Mn|'
        '09:28.051|00:09:34|5664932K|1|1\n'
        '24221219.extern|24220929_421.extern|COMPLETED|1|16000Mn|'
        '00:00:00|00:09:34|1404K|1|1\n'
        '24221220|24220929_422|PENDING|1|16000Mn|09:28.052|00:09:34||1|\n'
        '24221220.batch|24220929_422.batch|PENDING|1|16000Mn|'
        '09:28.051|00:09:34|5664932K|1|1\n'
        '24221220.extern|24220929_422.extern|PENDING|1|16000Mn|'
        '00:00:00|00:09:34|1404K|1|1\n'
    )
    mocker.patch('reportseff.reportseff.subprocess.run',
                 return_value=sub_result)
    result = runner.invoke(reportseff.reportseff,
                           '--no-color 24220929')

    assert result.exit_code == 0
    # remove header
    output = result.output.split('\n')[1:-2]
    assert output[0].split() == [
        '24220929_421', 'COMPLETED', '00:09:34', '99.0%', '34.6%'
    ]
    assert output[1].split() == [
        '24220929_422', 'PENDING', '---', '---', '---'
    ]
    assert len(output) == 2
    # TODO have it handle array sub ids (like 242_421) as single entries
    # though sacct gives all, but have it handle the base name (242) like all


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
        'Name', 'State', 'Time', 'CPU', 'Memory'
    ]
    assert len(output) == 1


def test_failed_no_mem(mocker):
    runner = CliRunner()
    sub_result = mocker.MagicMock()
    sub_result.returncode = 0
    sub_result.stdout = (
        '23000381|23000381|FAILED|8|4000Mc|00:00:00|00:00:12||1|\n'
        '23000381.batch|23000381.batch|FAILED|8|4000Mc|'
        '00:00:00|00:00:12||1|1\n'
        '23000381.extern|23000381.extern|COMPLETED|8|4000Mc|'
        '00:00:00|00:00:12|1592K|1|1\n'
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
        '23000381', 'FAILED', '00:00:12', '0.0%', '0.0%'
    ]
    assert len(output) == 1


'''
OTHERS TO TEST
23000233: cancelled by other
23000210: 0 run time
23000381: string index out of range
'''
