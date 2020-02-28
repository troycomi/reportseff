from reportseff import db_inquirer
import pytest
import datetime


@pytest.fixture
def sacct():
    return db_inquirer.Sacct_Inquirer()


def test_sacct_init(sacct):
    assert sacct.default_args == ['sacct', '-P', '-n']
    assert sacct.user is None


def test_sacct_get_valid_formats(sacct, mocker):
    mock_sacct = mocker.MagicMock
    mock_sacct.returncode = 1
    # these are the values for 18.08.7
    mock_sacct.stdout = (
        'Account             AdminComment        AllocCPUS           AllocGRES'
        '\nAllocNodes          AllocTRES           AssocID             AveCPU'
        '\nAveCPUFreq          AveDiskRead         AveDiskWrite        '
        'AvePages           \nAveRSS              AveVMSize           BlockID '
        'Cluster            \nComment             ConsumedEnergy      '
        'ConsumedEnergyRaw   CPUTime            \nCPUTimeRAW '
        'DerivedExitCode     Elapsed             ElapsedRaw         \nEligible'
        ' End                 ExitCode            GID                \nGroup'
        ' JobID               JobIDRaw            JobName            \nLayout'
        ' MaxDiskRead         MaxDiskReadNode     MaxDiskReadTask    '
        '\nMaxDiskWrite        MaxDiskWriteNode    MaxDiskWriteTask    '
        'MaxPages           \nMaxPagesNode        MaxPagesTask        MaxRSS'
        ' MaxRSSNode         \nMaxRSSTask          MaxVMSize           '
        'MaxVMSizeNode       MaxVMSizeTask      \nMcsLabel            '
        'MinCPU              MinCPUNode          MinCPUTask         \nNCPUS'
        ' NNodes              NodeList            NTasks             '
        '\nPriority Partition           QOS                 QOSRAW           '
        '\nReqCPUFreq          ReqCPUFreqMin       ReqCPUFreqMax       '
        'ReqCPUFreqGov      \nReqCPUS             ReqGRES             ReqMem '
        'ReqNodes           \nReqTRES             Reservation         '
        'ReservationId       Reserved           \nResvCPU             '
        'ResvCPURAW          Start               State              \nSubmit '
        'Suspended           SystemCPU           SystemComment      '
        '\nTimelimit           TimelimitRaw        TotalCPU            '
        'TRESUsageInAve     \nTRESUsageInMax      TRESUsageInMaxNode '
        'TRESUsageInMaxTask  TRESUsageInMin     \nTRESUsageInMinNode '
        'TRESUsageInMinTask  TRESUsageInTot      TRESUsageOutAve '
        '\nTRESUsageOutMax     TRESUsageOutMaxNode TRESUsageOutMaxTask '
        'TRESUsageOutMin    \nTRESUsageOutMinNode TRESUsageOutMinTask '
        'TRESUsageOutTot     UID                \nUser                '
        'UserCPU             WCKey               WCKeyID            '
        '\nWorkDir            \n'
    )
    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=mock_sacct)
    with pytest.raises(Exception) as e:
        sacct.get_valid_formats()
    assert 'Error retrieving sacct options with --helpformat' in str(e)

    mock_sacct.returncode = 0
    result = ['Account', 'AdminComment', 'AllocCPUS', 'AllocGRES',
              'AllocNodes', 'AllocTRES', 'AssocID', 'AveCPU', 'AveCPUFreq',
              'AveDiskRead', 'AveDiskWrite', 'AvePages', 'AveRSS', 'AveVMSize',
              'BlockID', 'Cluster', 'Comment', 'ConsumedEnergy',
              'ConsumedEnergyRaw', 'CPUTime', 'CPUTimeRAW', 'DerivedExitCode',
              'Elapsed', 'ElapsedRaw', 'Eligible', 'End', 'ExitCode', 'GID',
              'Group', 'JobID', 'JobIDRaw', 'JobName', 'Layout', 'MaxDiskRead',
              'MaxDiskReadNode', 'MaxDiskReadTask', 'MaxDiskWrite',
              'MaxDiskWriteNode', 'MaxDiskWriteTask', 'MaxPages',
              'MaxPagesNode', 'MaxPagesTask', 'MaxRSS', 'MaxRSSNode',
              'MaxRSSTask', 'MaxVMSize', 'MaxVMSizeNode', 'MaxVMSizeTask',
              'McsLabel', 'MinCPU', 'MinCPUNode', 'MinCPUTask', 'NCPUS',
              'NNodes', 'NodeList', 'NTasks', 'Priority', 'Partition', 'QOS',
              'QOSRAW', 'ReqCPUFreq', 'ReqCPUFreqMin', 'ReqCPUFreqMax',
              'ReqCPUFreqGov', 'ReqCPUS', 'ReqGRES', 'ReqMem', 'ReqNodes',
              'ReqTRES', 'Reservation', 'ReservationId', 'Reserved', 'ResvCPU',
              'ResvCPURAW', 'Start', 'State', 'Submit', 'Suspended',
              'SystemCPU', 'SystemComment', 'Timelimit', 'TimelimitRaw',
              'TotalCPU', 'TRESUsageInAve', 'TRESUsageInMax',
              'TRESUsageInMaxNode', 'TRESUsageInMaxTask', 'TRESUsageInMin',
              'TRESUsageInMinNode', 'TRESUsageInMinTask', 'TRESUsageInTot',
              'TRESUsageOutAve', 'TRESUsageOutMax', 'TRESUsageOutMaxNode',
              'TRESUsageOutMaxTask', 'TRESUsageOutMin', 'TRESUsageOutMinNode',
              'TRESUsageOutMinTask', 'TRESUsageOutTot', 'UID', 'User',
              'UserCPU', 'WCKey', 'WCKeyID', 'WorkDir']
    assert sacct.get_valid_formats() == result


def test_sacct_get_db_output(sacct, mocker):
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 1

    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=mock_sacct)
    with pytest.raises(Exception) as e:
        sacct.get_db_output('c1 c2'.split(), 'j1 j2 j3'.split())
    assert 'Error running sacct!' in str(e)

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        'c1j1|c2j1\n'
        'c1j2|c2j2\n'
        'c1j3|c2j3\n'
    )
    mock_sub = mocker.patch('reportseff.db_inquirer.subprocess.run',
                            return_value=mock_sacct)
    result = sacct.get_db_output('c1 c2'.split(), 'j1 j2 j3'.split())
    assert result == [
        {'c1': 'c1j1', 'c2': 'c2j1'},
        {'c1': 'c1j2', 'c2': 'c2j2'},
        {'c1': 'c1j3', 'c2': 'c2j3'},
    ]
    mock_sub.assert_called_once_with(
        args='sacct -P -n --format=c1,c2 --jobs=j1,j2,j3'.split(),
        stdout=mocker.ANY, encoding=mocker.ANY,
        check=mocker.ANY, universal_newlines=mocker.ANY)

    _, debug = sacct.get_db_output('c1 c2'.split(), 'j1 j2 j3'.split(), True)
    assert debug == (
        'c1j1|c2j1\n'
        'c1j2|c2j2\n'
        'c1j3|c2j3\n'
    )


def test_sacct_get_db_output_no_newline(sacct, mocker):
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        '16|00:00:00|23000233|23000233||1|4000Mc|CANCELLED by 129319|'
        '6-00:00:00|00:00:00'
    )
    mock_sub = mocker.patch('reportseff.db_inquirer.subprocess.run',
                            return_value=mock_sacct)
    result, debug = sacct.get_db_output(
        ['AllocCPUS', 'Elapsed', 'JobID', 'JobIDRaw', 'MaxRSS', 'NNodes',
         'REQMEM', 'State', 'Timelimit', 'TotalCPU'], ['23000233'], True)
    assert result == [
        {'AllocCPUS': '16', 'Elapsed': '00:00:00', 'JobID': '23000233',
         'JobIDRaw': '23000233', 'MaxRSS': '', 'NNodes': '1',
         'REQMEM': '4000Mc', 'State': 'CANCELLED by 129319',
         'Timelimit': '6-00:00:00', 'TotalCPU': '00:00:00'}
    ]
    mock_sub.assert_called_once()

    assert debug == (
        '16|00:00:00|23000233|23000233||1|4000Mc|CANCELLED by 129319|'
        '6-00:00:00|00:00:00'
    )


def test_sacct_set_user(sacct):
    sacct.set_user('user')
    assert sacct.user == 'user'


def test_sacct_get_db_output_user(sacct, mocker):
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 1

    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=mock_sacct)
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.date(2018, 1, 20)
    mock_date.side_effect = lambda *args, **kw: datetime.date(*args, **kw)
    mocker.patch('reportseff.db_inquirer.datetime.date', mock_date)
    with pytest.raises(Exception) as e:
        sacct.get_db_output('c1 c2'.split(), 'j1 j2 j3'.split())
    assert 'Error running sacct!' in str(e)

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        'c1j1|c2j1\n'
        'c1j2|c2j2\n'
        'c1j3|c2j3\n'
    )
    mock_sub = mocker.patch('reportseff.db_inquirer.subprocess.run',
                            return_value=mock_sacct)
    sacct.set_user('user')
    result = sacct.get_db_output('c1 c2'.split(), {})
    assert result == [
        {'c1': 'c1j1', 'c2': 'c2j1'},
        {'c1': 'c1j2', 'c2': 'c2j2'},
        {'c1': 'c1j3', 'c2': 'c2j3'},
    ]
    mock_sub.assert_called_once_with(
        args=('sacct -P -n --format=c1,c2 --user=user '
              '--starttime=011318').split(),
        stdout=mocker.ANY, encoding=mocker.ANY,
        check=mocker.ANY, universal_newlines=mocker.ANY)

    _, debug = sacct.get_db_output('c1 c2'.split(), 'j1 j2 j3'.split(), True)
    assert debug == (
        'c1j1|c2j1\n'
        'c1j2|c2j2\n'
        'c1j3|c2j3\n'
    )


def test_sacct_set_state(sacct, capsys):
    # decodes properly, to upper
    sacct.set_state('BF,ca,cD,Dl,F,NF,OOM,PD,PR,R,RQ,RS,RV,S,TO')
    assert sacct.state == {
        'BOOT_FAIL',
        'CANCELLED',
        'COMPLETED',
        'DEADLINE',
        'FAILED',
        'NODE_FAIL',
        'OUT_OF_MEMORY',
        'PENDING',
        'PREEMPTED',
        'RUNNING',
        'REQUEUED',
        'RESIZING',
        'REVOKED',
        'SUSPENDED',
        'TIMEOUT',
    }

    # sets to upper and removes duplicates
    sacct.set_state('TiMeOuT,running,FAILED,failed')
    assert sacct.state == {'TIMEOUT', 'RUNNING', 'FAILED'}

    # sets while warning of missing
    sacct.set_state('unknown,r,F')
    assert sacct.state == {'RUNNING', 'FAILED'}
    assert capsys.readouterr().err == 'Unknown state UNKNOWN\n'


def test_sacct_get_db_output_user_state(sacct, mocker):
    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 1

    mocker.patch('reportseff.db_inquirer.subprocess.run',
                 return_value=mock_sacct)
    mock_date = mocker.MagicMock()
    mock_date.today.return_value = datetime.date(2018, 1, 20)
    mock_date.side_effect = lambda *args, **kw: datetime.date(*args, **kw)
    mocker.patch('reportseff.db_inquirer.datetime.date', mock_date)
    with pytest.raises(Exception) as e:
        sacct.get_db_output('c1 c2'.split(), 'j1 j2 j3'.split())
    assert 'Error running sacct!' in str(e)

    mock_sacct = mocker.MagicMock()
    mock_sacct.returncode = 0
    mock_sacct.stdout = (
        'c1j1|c2j1|RUNNING\n'
        'c1j2|c2j2|RUNNING\n'
        'c1j3|c2j3|COMPLETED\n'
    )
    mock_sub = mocker.patch('reportseff.db_inquirer.subprocess.run',
                            return_value=mock_sacct)
    sacct.set_user('user')
    sacct.set_state('R')
    result = sacct.get_db_output('c1 c2 State'.split(), {})
    assert result == [
        {'c1': 'c1j1', 'c2': 'c2j1', 'State': 'RUNNING'},
        {'c1': 'c1j2', 'c2': 'c2j2', 'State': 'RUNNING'},
    ]
    mock_sub.assert_called_once_with(
        args=('sacct -P -n --format=c1,c2,State --user=user '
              '--starttime=011318').split(),
        stdout=mocker.ANY, encoding=mocker.ANY,
        check=mocker.ANY, universal_newlines=mocker.ANY)

    # debug is not affected by state
    _, debug = sacct.get_db_output('c1 c2 State'.split(),
                                   'j1 j2 j3'.split(), True)
    assert debug == (
        'c1j1|c2j1|RUNNING\n'
        'c1j2|c2j2|RUNNING\n'
        'c1j3|c2j3|COMPLETED\n'
    )
