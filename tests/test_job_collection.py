"""Test job collection functions."""

from pathlib import Path

import pytest
from reportseff import job_collection
from reportseff.job import Job


@pytest.fixture()
def jobs():
    """New default job collection."""
    return job_collection.JobCollection()


def test_get_columns(jobs):
    """Default get columns are reasonable."""
    assert jobs.get_columns() == (
        "JobIDRaw,JobID,State,AllocCPUS,TotalCPU,Elapsed,Timelimit,"
        "ReqMem,MaxRSS,NNodes,NTasks,Partition"
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
        "reportseff.job_collection.Path.cwd", return_value=Path("/dir/path")
    )
    mock_real = mocker.patch(
        "reportseff.job_collection.Path.resolve", return_value=Path("/dir/path2")
    )
    mock_exists = mocker.patch(
        "reportseff.job_collection.Path.exists", return_value=False
    )

    with pytest.raises(ValueError, match="/dir/path does not exist!"):
        jobs.set_out_dir("")
    mock_cwd.assert_called_once()
    mock_real.assert_not_called()
    mock_exists.assert_called_once()

    mock_cwd.reset_mock()
    mock_real.reset_mock()
    mock_exists.reset_mock()

    with pytest.raises(ValueError, match="/dir/path2 does not exist!"):
        jobs.set_out_dir("pwd")
    mock_cwd.assert_not_called()
    mock_real.assert_called_once()
    mock_exists.assert_called_once()


def test_set_jobs_none_valid(jobs):
    """Set jobs raises exceptions when no valid name is provided."""
    with pytest.raises(ValueError, match="No valid jobs provided!"):
        jobs.set_jobs(("asdf", "qwer", "zxcv", "asdf111"))


def test_set_jobs_filter(jobs):
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
    mocker.patch("reportseff.job_collection.Path.is_dir", return_value=False)
    with pytest.raises(ValueError, match="No valid jobs provided!"):
        jobs.set_jobs(("dir",))

    jobs.set_jobs(("1",))
    assert jobs.jobs == {"1": Job("1", "1", None)}

    mocker.patch("reportseff.job_collection.Path.is_dir", return_value=True)
    mock_set_out = mocker.patch.object(job_collection.JobCollection, "set_out_dir")

    jobs.set_jobs(())
    mock_set_out.assert_called_once()

    mock_set_out.reset_mock()
    jobs.set_jobs(("dir",))
    mock_set_out.assert_called_once()


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
                "ReqMem": "1Gn",
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
                "ReqMem": "1Gn",
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
                "ReqMem": "1Gn",
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
                "ReqMem": "1Gn",
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
                "ReqMem": "1Gn",
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
                "ReqMem": "1Gn",
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
        "reportseff.job_collection.Path.resolve",
        return_value=Path("/dir/path2/test"),
    )
    mocker.patch("reportseff.job_collection.Path.exists", return_value=True)
    mocker.patch("reportseff.job_collection.Path.is_file", return_value=True)

    mocker.patch("reportseff.job_collection.Path.iterdir", return_value=[])
    with pytest.raises(
        ValueError,
        match="/dir/path2/test contains no files!",
    ):
        jobs.set_out_dir("test")

    mocker.patch("reportseff.job_collection.Path.iterdir", return_value=[Path("asdf")])
    with pytest.raises(
        ValueError, match="/dir/path2/test contains no valid output files!"
    ):
        jobs.set_out_dir("test")

    mocker.patch(
        "reportseff.job_collection.Path.iterdir",
        return_value=[
            Path("asdf"),
            Path("base_1"),
            Path("base_1_1.out"),
            Path("base_2_1"),  # overwritten
            Path("base_2_1.out"),
        ],
    )
    jobs.set_out_dir("test")

    assert jobs.jobs == {
        "1": Job("1", "1", "base_1"),
        "1_1": Job("1", "1_1", "base_1_1.out"),
        "2_1": Job("2", "2_1", "base_2_1.out"),
    }


def test_set_custom_seff_format(jobs, mocker):
    """Can change the slurm output file matching."""
    mocker.patch("reportseff.job_collection.Path.exists", return_value=True)
    mocker.patch("reportseff.job_collection.Path.is_file", return_value=True)
    mocker.patch(
        "reportseff.job_collection.Path.iterdir",
        return_value=[
            Path("asdf"),
            Path("base_1"),
            Path("base_1_1.out"),
            Path("base_2_1"),
            Path("base_2_1.out"),
            Path("3.out"),
            Path("4_1.out"),
        ],
    )

    with pytest.raises(ValueError, match="Unable to determine jobid from %n.out."):
        jobs.set_custom_seff_format("%n.out")

    jobs.set_custom_seff_format("%j.out")
    assert jobs.job_file_regex.pattern == r"^(?P<jobid>(?P<job>[0-9]+))\.out$"

    jobs.set_out_dir("test")
    assert jobs.jobs == {
        "3": Job("3", "3", "3.out"),
    }
    jobs.jobs = {}

    jobs.set_custom_seff_format("%x%n_%A_%a")
    assert jobs.job_file_regex.pattern == r"^.*_(?P<jobid>(?P<job>[0-9]+)_[0-9]+)$"

    jobs.set_out_dir("test")
    assert jobs.jobs == {
        "2_1": Job("2", "2_1", "base_2_1"),
    }
    jobs.jobs = {}

    jobs.set_custom_seff_format("%x_%A.out")
    assert jobs.job_file_regex.pattern == r"^.*_(?P<jobid>(?P<job>[0-9]+))\.out$"

    jobs.set_out_dir("test")
    assert jobs.jobs == {
        "1": Job("1", "1", "4_1.out"),
    }
    jobs.jobs = {}


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

    assert jobs.get_sorted_jobs(sorting="filename") == [
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
        "reportseff.job_collection.Path.exists",
        lambda x: str(x) != "dir/nothing",
    )

    # replace mtime with the length of the filename
    def my_stat(file):
        mock = mocker.MagicMock()
        mock.st_mtime = len(file.name)
        return mock

    mocker.patch(
        "reportseff.job_collection.Path.stat",
        my_stat,
    )

    # still uses other sorting, no dir_name set
    assert jobs.get_sorted_jobs(sorting="mtime") == [
        Job("j13", "jid13", None),
        Job("j2", "jid2", "file234"),
        Job("j14", "jid14", "nothing"),
        Job("j1", "jid1", "file12"),
        Job("j3", "jid3", "file3"),
    ]

    jobs.dir_name = Path("dir")  # now dir/nothing doesn't exist
    assert jobs.get_sorted_jobs(sorting="mtime") == [
        Job("j14", "jid14", "nothing"),
        Job("j13", "jid13", None),
        Job("j2", "jid2", "file234"),
        Job("j1", "jid1", "file12"),
        Job("j3", "jid3", "file3"),
    ]


def test_get_sorted_jobs_jobid(jobs):
    """Can get jobs in sorted order by numeric job id."""
    jobs.add_job("3", "1_2")
    jobs.add_job("2", "1_1")
    jobs.add_job("1", "1")
    jobs.add_job("4", "2")
    jobs.add_job("5", "2_100")
    jobs.add_job("7", "a_2_100")
    jobs.add_job("6", "a2_100")

    assert jobs.get_sorted_jobs(sorting="jobid") == [
        Job("7", "a_2_100", None),
        Job("6", "a2_100", None),
        Job("1", "1", None),
        Job("2", "1_1", None),
        Job("3", "1_2", None),
        Job("4", "2", None),
        Job("5", "2_100", None),
    ]


def test_get_sorted_jobs_issue_75(jobs):
    """Test specific example from issue 75."""
    jobs.add_job("1", "5163879_8")
    jobs.add_job("2", "5163879_6")
    jobs.add_job("3", "5163879_4")
    jobs.add_job("4", "5163879_2")
    jobs.add_job("5", "5163879_15")
    jobs.add_job("6", "5163879_14")
    jobs.add_job("7", "5163879_13")
    jobs.add_job("8", "5163879_12")
    jobs.add_job("9", "5163879_11")
    jobs.add_job("10", "5163879_10")
    jobs.add_job("11", "5163543_20")
    jobs.add_job("12", "5163543_19")
    jobs.add_job("13", "5163543_18")
    jobs.add_job("14", "5163543_17")
    jobs.add_job("15", "5163543_16")
    jobs.add_job("16", "5163543_15")
    jobs.add_job("17", "5163543_14")
    jobs.add_job("18", "5163543_13")
    jobs.add_job("19", "5163543_12")
    jobs.add_job("20", "5163543_11")
    jobs.add_job("21", "5163543_10")
    jobs.add_job("22", "5159883_8")
    jobs.add_job("23", "5159883_6")
    jobs.add_job("24", "5159883_4")
    jobs.add_job("25", "5159883_2")
    jobs.add_job("26", "5159883_20")
    jobs.add_job("27", "5159883_18")
    jobs.add_job("28", "5159883_16")
    jobs.add_job("29", "5159883_14")
    jobs.add_job("30", "5159883_12")
    jobs.add_job("31", "5159883_10")
    jobs.add_job("32", "5159883_0")

    assert jobs.get_sorted_jobs(sorting="jobid") == [
        Job("32", "5159883_0", None),
        Job("25", "5159883_2", None),
        Job("24", "5159883_4", None),
        Job("23", "5159883_6", None),
        Job("22", "5159883_8", None),
        Job("31", "5159883_10", None),
        Job("30", "5159883_12", None),
        Job("29", "5159883_14", None),
        Job("28", "5159883_16", None),
        Job("27", "5159883_18", None),
        Job("26", "5159883_20", None),
        Job("21", "5163543_10", None),
        Job("20", "5163543_11", None),
        Job("19", "5163543_12", None),
        Job("18", "5163543_13", None),
        Job("17", "5163543_14", None),
        Job("16", "5163543_15", None),
        Job("15", "5163543_16", None),
        Job("14", "5163543_17", None),
        Job("13", "5163543_18", None),
        Job("12", "5163543_19", None),
        Job("11", "5163543_20", None),
        Job("4", "5163879_2", None),
        Job("3", "5163879_4", None),
        Job("2", "5163879_6", None),
        Job("1", "5163879_8", None),
        Job("10", "5163879_10", None),
        Job("9", "5163879_11", None),
        Job("8", "5163879_12", None),
        Job("7", "5163879_13", None),
        Job("6", "5163879_14", None),
        Job("5", "5163879_15", None),
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
            "ReqMem": "50Gn",
            "State": "PENDING",
            "TotalCPU": "00:00:00",
        },
        add_job=True,
    )
    expected_job = Job("14729857", "14729857_[737-999]", None)
    expected_job.state = "PENDING"
    expected_job._cache_entries()
    assert jobs.jobs == {"14729857_[737-999]": expected_job}


def test_filter_by_array_size(jobs):
    """Can filter array jobs when requested, singletons always accepted."""
    jobs.jobs = {
        "1": Job("1", "1", None),  # singleton job
        "2_1": Job("2", "2_1", None),
        "2_2": Job("2", "2_2", None),  # two jobs
        "3_1": Job("3", "3_1", None),
        "3_2": Job("3", "3_2", None),
        "3_3": Job("3", "3_3", None),  # three jobs
    }

    assert jobs.get_jobs() == "1,2,3".split(",")

    jobs.filter_by_array_size(0)
    assert jobs.get_jobs() == "1,2,3".split(",")

    jobs.filter_by_array_size(1)
    assert jobs.get_jobs() == "1,2,3".split(",")

    jobs.filter_by_array_size(3)
    assert jobs.get_jobs() == "1,3".split(",")
