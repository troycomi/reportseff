"""Test job collection functions."""
import pytest

from reportseff import job_collection
from reportseff.job import Job


@pytest.fixture
def jobs():
    """New default job collection."""
    return job_collection.JobCollection()


def test_get_columns(jobs):
    """Default get columns are reasonable."""
    assert jobs.get_columns() == (
        "JobIDRaw,JobID,State,AllocCPUS,TotalCPU,Elapsed,Timelimit,"
        "REQMEM,MaxRSS,NNodes,NTasks,Partition"
    ).split(",")


def test_get_jobs(jobs):
    """Can hold a set of jobs and access them."""
    assert jobs.get_jobs() == []

    jobs.jobs = {
        "1_1": Job("1", "1_1", None),
        "1_2": Job("1", "1_2", None),
        "2_2": Job("2", "2_2", None),
    }
    assert jobs.get_jobs() == "1,2".split(",")


def test_set_out_dir_dir_handling(jobs, mocker):
    """Can handle setting path from cwd or provided value."""
    # dir handling
    mock_cwd = mocker.patch(
        "reportseff.job_collection.os.getcwd", return_value="/dir/path/"
    )
    mock_real = mocker.patch(
        "reportseff.job_collection.os.path.realpath", return_value="/dir/path2/"
    )
    mock_exists = mocker.patch(
        "reportseff.job_collection.os.path.exists", return_value=False
    )

    with pytest.raises(ValueError) as exception:
        jobs.set_out_dir("")
    assert "/dir/path/ does not exist!" in str(exception)
    mock_cwd.assert_called_once()
    mock_real.assert_not_called()
    mock_exists.assert_called_once_with("/dir/path/")

    mock_cwd.reset_mock()
    mock_real.reset_mock()
    mock_exists.reset_mock()

    with pytest.raises(ValueError) as exception:
        jobs.set_out_dir("pwd")
    assert "/dir/path2/ does not exist!" in str(exception)
    mock_cwd.assert_not_called()
    mock_real.assert_called_once_with("pwd")
    mock_exists.assert_called_once_with("/dir/path2/")


def test_set_jobs_none_valid(jobs, mocker):
    """Set jobs raises exceptions when no valid name is provided."""
    with pytest.raises(ValueError) as exception:
        jobs.set_jobs(("asdf", "qwer", "zxcv", "asdf111"))
    assert "No valid jobs provided!" in str(exception)


def test_set_jobs_filter(jobs, mocker):
    """Set jobs take only valid names from list."""
    jobs.set_jobs(("asdf", "1", "1_1", "asdf_1_2", "1_asdf_2"))
    assert jobs.jobs == {
        "1": Job("1", "1", None),
        "1_1": Job("1", "1_1", None),
        "1_2": Job("1", "1_2", "asdf_1_2"),
        "2": Job("2", "2", "1_asdf_2"),
    }


def test_set_jobs_dir(jobs, mocker):
    """Can provide a directory to set jobs."""
    jobs.jobs = {}
    mocker.patch("reportseff.job_collection.os.path.isdir", return_value=False)
    with pytest.raises(ValueError) as exception:
        jobs.set_jobs(("dir",))
    assert "No valid jobs provided!" in str(exception)

    jobs.set_jobs(("1",))
    assert jobs.jobs == {"1": Job("1", "1", None)}

    mocker.patch("reportseff.job_collection.os.path.isdir", return_value=True)
    mock_set_out = mocker.patch.object(job_collection.JobCollection, "set_out_dir")

    jobs.set_jobs(())
    mock_set_out.assert_called_once_with("")

    mock_set_out.reset_mock()
    jobs.set_jobs(("dir",))
    mock_set_out.assert_called_once_with("dir")


def test_process_line(jobs, mocker):
    """Can process entries from sacct and send to update."""
    jobs.jobs = {"24371655": Job("24371655", "24371655", "test_24371655")}
    mock_update = mocker.patch.object(Job, "update")
    jobs.process_entry(
        dict(
            zip(
                jobs.get_columns(),
                "24371655|24371655|COMPLETED|1|"
                "01:29:47|01:29:56|03:00:00|1Gn||1|".split("|"),
            )
        )
    )
    jobs.process_entry(
        dict(
            zip(
                jobs.get_columns(),
                "24371655.batch|24371655.batch|COMPLETED|1|"
                "01:29:47|01:29:56||1Gn|495644K|1|1".split("|"),
            )
        )
    )
    jobs.process_entry(
        dict(
            zip(
                jobs.get_columns(),
                "24371655.extern|24371655.extern|COMPLETED|1|"
                "00:00:00|01:29:56||1Gn|1372K|1|1".split("|"),
            )
        )
    )

    assert mock_update.call_args_list == [
        mocker.call(
            {
                "JobIDRaw": "24371655",
                "JobID": "24371655",
                "State": "COMPLETED",
                "AllocCPUS": "1",
                "REQMEM": "1Gn",
                "TotalCPU": "01:29:47",
                "Elapsed": "01:29:56",
                "Timelimit": "03:00:00",
                "MaxRSS": "",
                "NNodes": "1",
                "NTasks": "",
            }
        ),
        mocker.call(
            {
                "JobIDRaw": "24371655.batch",
                "JobID": "24371655.batch",
                "State": "COMPLETED",
                "AllocCPUS": "1",
                "REQMEM": "1Gn",
                "TotalCPU": "01:29:47",
                "Elapsed": "01:29:56",
                "Timelimit": "",
                "MaxRSS": "495644K",
                "NNodes": "1",
                "NTasks": "1",
            }
        ),
        mocker.call(
            {
                "JobIDRaw": "24371655.extern",
                "JobID": "24371655.extern",
                "State": "COMPLETED",
                "AllocCPUS": "1",
                "REQMEM": "1Gn",
                "TotalCPU": "00:00:00",
                "Elapsed": "01:29:56",
                "Timelimit": "",
                "MaxRSS": "1372K",
                "NNodes": "1",
                "NTasks": "1",
            }
        ),
    ]


def test_process_line_partition_timelimit_not_set(jobs, mocker):
    """When partition limits is not set, forward to job."""
    jobs.jobs = {"24371655": Job("24371655", "24371655", "test_24371655")}
    mock_update = mocker.patch.object(Job, "update")
    jobs.process_entry(
        dict(
            zip(
                jobs.get_columns(),
                "24371655|24371655|COMPLETED|1|"
                "01:29:47|01:29:56|Partition_Limit|1Gn||1||mainqueue".split("|"),
            )
        )
    )

    assert mock_update.call_args_list == [
        mocker.call(
            {
                "JobIDRaw": "24371655",
                "JobID": "24371655",
                "State": "COMPLETED",
                "AllocCPUS": "1",
                "REQMEM": "1Gn",
                "TotalCPU": "01:29:47",
                "Elapsed": "01:29:56",
                "Timelimit": "Partition_Limit",
                "MaxRSS": "",
                "NNodes": "1",
                "NTasks": "",
                "Partition": "mainqueue",
            }
        ),
    ]


def test_process_line_partition_timelimit(jobs, mocker):
    """When partition limits is set, replace with limit."""
    jobs.jobs = {"24371655": Job("24371655", "24371655", "test_24371655")}
    mock_update = mocker.patch.object(Job, "update")
    jobs.set_partition_limits({"mainqueue": "01:02:03"})
    jobs.process_entry(
        dict(
            zip(
                jobs.get_columns(),
                "24371655|24371655|COMPLETED|1|"
                "01:29:47|01:29:56|Partition_Limit|1Gn||1||mainqueue".split("|"),
            )
        )
    )

    assert mock_update.call_args_list == [
        mocker.call(
            {
                "JobIDRaw": "24371655",
                "JobID": "24371655",
                "State": "COMPLETED",
                "AllocCPUS": "1",
                "REQMEM": "1Gn",
                "TotalCPU": "01:29:47",
                "Elapsed": "01:29:56",
                "Timelimit": "01:02:03",
                "MaxRSS": "",
                "NNodes": "1",
                "NTasks": "",
                "Partition": "mainqueue",
            }
        ),
    ]


def test_process_line_partition_timelimit_no_match(jobs, mocker):
    """When partition limits is set, but not matching, forward limit."""
    jobs.jobs = {"24371655": Job("24371655", "24371655", "test_24371655")}
    mock_update = mocker.patch.object(Job, "update")
    jobs.set_partition_limits({"mainqueue2": "01:02:03"})
    jobs.process_entry(
        dict(
            zip(
                jobs.get_columns(),
                "24371655|24371655|COMPLETED|1|"
                "01:29:47|01:29:56|Partition_Limit|1Gn||1||mainqueue".split("|"),
            )
        )
    )

    assert mock_update.call_args_list == [
        mocker.call(
            {
                "JobIDRaw": "24371655",
                "JobID": "24371655",
                "State": "COMPLETED",
                "AllocCPUS": "1",
                "REQMEM": "1Gn",
                "TotalCPU": "01:29:47",
                "Elapsed": "01:29:56",
                "Timelimit": "Partition_Limit",
                "MaxRSS": "",
                "NNodes": "1",
                "NTasks": "",
                "Partition": "mainqueue",
            }
        ),
    ]


def test_set_out_dir(jobs, mocker):
    """Can set directory with slurm out files."""
    mocker.patch(
        "reportseff.job_collection.os.path.realpath",
        side_effect=lambda x: f"/dir/path2/{x}",
    )
    mocker.patch("reportseff.job_collection.os.path.exists", return_value=True)
    mocker.patch("reportseff.job_collection.os.path.isfile", return_value=True)

    mocker.patch("reportseff.job_collection.os.listdir", return_value=[])
    with pytest.raises(ValueError) as exception:
        jobs.set_out_dir("test")
    assert "/dir/path2/test contains no files!" in str(exception)

    mocker.patch("reportseff.job_collection.os.listdir", return_value=["asdf"])
    with pytest.raises(ValueError) as exception:
        jobs.set_out_dir("test")
    assert "/dir/path2/test contains no valid output files!" in str(exception)

    mocker.patch(
        "reportseff.job_collection.os.listdir",
        return_value=[
            "asdf",
            "base_1",
            "base_1_1.out",
            "base_2_1",  # overwritten
            "base_2_1.out",
        ],
    )
    jobs.set_out_dir("test")

    assert jobs.jobs == {
        "1": Job("1", "1", "base_1"),
        "1_1": Job("1", "1_1", "base_1_1.out"),
        "2_1": Job("2", "2_1", "base_2_1.out"),
    }


def test_process_seff_file(jobs):
    """Can parse job names from slurm output file names."""
    # no matches
    jobs.process_seff_file("")
    assert jobs.jobs == {}

    jobs.process_seff_file("base_name")
    assert jobs.jobs == {}

    # simple job file
    jobs.process_seff_file("base_name_1")
    assert jobs.jobs == {"1": Job("1", "1", "base_name_1")}

    # with .out
    jobs.process_seff_file("base_name_2.out")
    assert jobs.jobs == {
        "1": Job("1", "1", "base_name_1"),
        "2": Job("2", "2", "base_name_2.out"),
    }

    # with array
    jobs.process_seff_file("base_name_3_1")
    assert jobs.jobs == {
        "1": Job("1", "1", "base_name_1"),
        "2": Job("2", "2", "base_name_2.out"),
        "3_1": Job("3", "3_1", "base_name_3_1"),
    }

    # array and .out
    jobs.process_seff_file("base_name_4_1.out")
    assert jobs.jobs == {
        "1": Job("1", "1", "base_name_1"),
        "2": Job("2", "2", "base_name_2.out"),
        "3_1": Job("3", "3_1", "base_name_3_1"),
        "4_1": Job("4", "4_1", "base_name_4_1.out"),
    }

    jobs.jobs = {}
    # slight mistakes
    jobs.process_seff_file("base_name_4_1out")
    assert jobs.jobs == {}

    jobs.process_seff_file("base_name4_1.out")
    assert jobs.jobs == {"1": Job("1", "1", "base_name4_1.out")}

    jobs.process_seff_file("base_name_0_4_1.out")
    assert jobs.jobs == {
        "1": Job("1", "1", "base_name4_1.out"),
        "4_1": Job("4", "4_1", "base_name_0_4_1.out"),
    }

    jobs.process_seff_file("slurm-24825624.out")
    assert jobs.jobs == {
        "1": Job("1", "1", "base_name4_1.out"),
        "4_1": Job("4", "4_1", "base_name_0_4_1.out"),
        "24825624": Job("24825624", "24825624", "slurm-24825624.out"),
    }


def test_add_job(jobs):
    """Can add jobs to collection."""
    jobs.add_job("j1", "jid1")
    assert jobs.jobs == {"jid1": Job("j1", "jid1", None)}

    # overwrite based on jobid
    jobs.add_job("j2", "jid1", "file")
    assert jobs.jobs == {"jid1": Job("j2", "jid1", "file")}

    # another job, different jid
    jobs.add_job("j2", "jid2", "file")
    assert jobs.jobs == {
        "jid1": Job("j2", "jid1", "file"),
        "jid2": Job("j2", "jid2", "file"),
    }


def test_get_sorted_jobs(jobs, mocker):
    """Can get jobs in sorted order by name or time."""
    jobs.add_job("j3", "jid3")
    jobs.add_job("j1", "jid1")
    jobs.add_job("j2", "jid2")
    jobs.add_job("j13", "jid13")

    assert jobs.get_sorted_jobs(False) == [
        Job("j1", "jid1", None),
        Job("j2", "jid2", None),
        Job("j3", "jid3", None),
        Job("j13", "jid13", None),
    ]

    jobs.jobs = {}
    jobs.add_job("j3", "jid3", "file3")
    jobs.add_job("j1", "jid1", "file12")
    jobs.add_job("j2", "jid2", "file234")
    jobs.add_job("j13", "jid13")
    jobs.add_job("j14", "jid14", "nothing")

    # make all non-none files exist
    mocker.patch(
        "reportseff.job_collection.os.path.exists",
        side_effect=lambda x: x is not None and x != "dir/nothing",
    )
    # replace mtime with the length of the filename
    mocker.patch("reportseff.job_collection.os.path.getmtime", side_effect=len)

    # still uses other sorting, no dir_name set
    assert jobs.get_sorted_jobs(True) == [
        Job("j13", "jid13", None),
        Job("j2", "jid2", "file234"),
        Job("j14", "jid14", "nothing"),
        Job("j1", "jid1", "file12"),
        Job("j3", "jid3", "file3"),
    ]

    jobs.dir_name = "dir"  # now dir/nothing doesn't exist
    assert jobs.get_sorted_jobs(True) == [
        Job("j14", "jid14", "nothing"),
        Job("j13", "jid13", None),
        Job("j2", "jid2", "file234"),
        Job("j1", "jid1", "file12"),
        Job("j3", "jid3", "file3"),
    ]


def test_process_entry_array_user(jobs):
    """Providing a user shorts the checks for existing jobs."""
    jobs.process_entry(
        {
            "AllocCPUS": "1",
            "Elapsed": "00:00:00",
            "JobID": "14729857_[737-999]",
            "JobIDRaw": "14729857",
            "MaxRSS": "",
            "NNodes": "1",
            "REQMEM": "50Gn",
            "State": "PENDING",
            "TotalCPU": "00:00:00",
        },
        add_job=True,
    )
    expected_job = Job("14729857", "14729857_[737-999]", None)
    expected_job.state = "PENDING"
    expected_job._cache_entries()
    assert jobs.jobs == {"14729857_[737-999]": expected_job}
