# Monitoring slurm efficiency with reportseff

> Troy Comi

## Motivation

As I started using Snakemake, I had hundreds of jobs which I wanted to get
performance information about. seff gave the efficiency information I wanted,
but for only a single job at a time. `sacct` handles multiple jobs, but couldn't
give the efficiency. With the current python implementation,
all job information is obtained from a single
`sacct` call and with click the output is colored to quickly see how things are
running. (But color isn't displayed below due to markdown limitations).

## Be good to your scheduler

### An introduction to scheduling efficiency

Have you ever hosted an event that had to provide food? Perhaps you sent out
RSVP's to estimate how many people would attend, guessed a handful of people
would show up but not respond, and ordered some pizza. If you ordered enough
food for 20 people and 18 showed, that would be a pizza efficiency of 90%.
But what if only 2 people showed up? Or 30? As extreme as these numbers seem,
memory and cpu usage efficiencies around 10% are not uncommon.

The goal of a scheduler is to take the user-provided resource
estimates for many jobs and decide who runs when. Let's say I have a small
cluster with 64 cores, 128 GB of memory and want to run an array job of
single-core processes with an estimated memory usage of 4 GB. The scheduler
will allow only 32 jobs to run at once (128 GB / 4 GB) leaving half of the
cores idling. If I actually only use 1 GB of memory, 64 jobs could be running
instead.

**Good jobs use the resources they promise to.**

In practice, many more details of the system and user are incorporated into
the decision to schedule a job. Once the scheduler decides a job will run,
the scheduler has to dispatch the job. The overhead associated with scheduling
only makes sense if the job will run for longer than a few minutes. Instead of
submitting 1000 jobs that perform 1 minute of work, group 100 subprocesses
together as 10 jobs with 100 minutes of work.

**Good jobs run long enough to matter.**

If every job on a cluster is efficient and long-running, the scheduler can
make accurate decisions on execution order and keep usage high.

### Why it matters as a user?

"But my qos only allows 2 jobs to run at once if the time
is less than 2 hours! Can't I say my 10 minute job will take 2 hours?" Yes,
but it is *rude* to the scheduler. If that doesn't sway you, improperly
estimating resource usage can:

- Decrease your priority for subsequent jobs.
- Cause your account to be charged for the full, estimated usage.
- Have fewer of your jobs running simultaneously.
- Make it harder to fit your job into the available cluster resources,
increasing the queue time.

### Monitoring efficiency

Before releasing a swarm of jobs, check the estimated vs predicted usage.
Tune your parameters to improve efficiency.

[Seff](https://github.com/SchedMD/slurm/tree/master/contribs/seff) provides
efficiency estimates for a single job. But to look at your usage
for many jobs or monitor usage, I wrote
[reportseff](https://github.com/troycomi/reportseff). It polls `sacct`
and calculates the same efficiency information as seff, but outputs
a tabular report.

During testing, I looked at random ranges of jobids on a Princeton cluster.
Here is some typical output, with jobids modified to protect the innocent:

```txt
  Name      State        Time     CPU   Memory
XXXXX000  COMPLETED    00:01:53  97.3%  14.0%
XXXXX001  COMPLETED    00:02:19  84.2%  14.0%
XXXXX002  COMPLETED    00:06:33  28.2%  14.0%
XXXXX003  COMPLETED    00:04:59  39.1%  14.0%
XXXXX004  COMPLETED    00:02:31  97.4%  9.2%
XXXXX005  COMPLETED    00:02:38  98.1%  9.1%
XXXXX006  COMPLETED    00:02:24  97.2%  9.1%
XXXXX007  COMPLETED    00:02:40  98.1%  9.0%
XXXXX008  COMPLETED    00:02:39  96.2%  9.1%
XXXXX009  COMPLETED    00:02:45  96.4%  9.0%
XXXXX012  COMPLETED    00:00:53  58.5%  10.6%
XXXXX013  COMPLETED    00:02:13  38.3%  10.6%
XXXXX014  COMPLETED    00:37:02  44.9%  10.6%
XXXXX015  COMPLETED    00:44:33  34.0%  10.6%
XXXXX016  COMPLETED    00:38:29  29.6%  10.7%
XXXXX017  COMPLETED    00:19:57  74.5%  10.8%
XXXXX018  COMPLETED    00:14:25  95.0%  10.8%
XXXXX019  COMPLETED    00:35:38  2.6%   10.6%
XXXXX020  COMPLETED    00:02:16  38.2%  10.6%
XXXXX021  COMPLETED    00:02:34  46.1%  10.9%
XXXXX022  COMPLETED    00:20:53  7.1%   10.6%
XXXXX023  COMPLETED    00:01:00  95.0%  11.1%
XXXXX024  COMPLETED    00:09:06  88.5%  10.5%
XXXXX025  COMPLETED    00:08:08  95.3%  10.6%
```

This is from at least 3 different users across departments.

Notice how short the jobs are (most <5 minutes) and how little memory is used,
about 500 MB of 4 GB in most cases. Another example is jobs with 4 cores using
25% of CPU. Though batching together short jobs is slightly difficult (nested
for loops with some arithmetic), using the correct number of cores and cutting
memory to improve usage is a simple fix.

Try it out and see if you have been good to your scheduler!
