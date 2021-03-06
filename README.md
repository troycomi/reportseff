[![Build Status](https://travis-ci.com/troycomi/reportseff.svg?branch=master)](https://travis-ci.com/troycomi/reportseff)
[![codecov](https://codecov.io/gh/troycomi/reportseff/branch/master/graph/badge.svg)](https://codecov.io/gh/troycomi/reportseff)

# reportseff

> A python script for tabular display of slurm efficiency information

## Usage
### Installation
`reportseff` runs on python >= 3.7.
The only external dependency is click (>= 7.0).
Calling
```
pip install git+https://github.com/troycomi/reportseff
```
will create command line bindings and install click.

### Sample Usage
#### Single job
Calling reportseff with a single jobid will provide equivalent information to
seff for that job.  `reportseff 24371789` and `reportseff map_fastq_24371789`
produce the following output:
<pre><b>
      JobID      State          Elapsed   CPUEff   MemEff  </b>
   24371789    COMPLETED       03:08:03   71.2%    45.7%
</pre>

#### Single array job
Providing either the raw job id or the array job id will get efficiency
information for a single element of the array job.  `reportseff 24220929_421`
and `reportseff 24221219` generate:
<pre><b>
          JobID      State          Elapsed    CPUEff   MemEff  </b>
   24220929_421    COMPLETED       00:09:34    99.0%    34.6%
</pre>

#### Array job group
If the base job id of an array is provided, all elements of the array will
be added to the output. `reportseff 24220929`
<pre><b>
          JobID      State          Elapsed    CPUEff   MemEff  </b>
     24220929_1    COMPLETED       00:10:43    99.2%    33.4%
    24220929_11    COMPLETED       00:10:10    99.2%    37.5%
    24220929_21    COMPLETED       00:09:25    98.8%    36.1%
    24220929_31    COMPLETED       00:09:19    98.9%    33.3%
    24220929_41    COMPLETED       00:09:23    98.9%    33.3%
    24220929_51    COMPLETED       00:08:02    98.5%    36.3%
	...
   24220929_951    COMPLETED       00:25:12    99.5%    33.5%
   24220929_961    COMPLETED       00:39:26    99.7%    34.1%
   24220929_971    COMPLETED       00:24:11    99.5%    34.2%
   24220929_981    COMPLETED       00:24:50    99.5%    44.3%
   24220929_991    COMPLETED       00:13:05    98.7%    33.7%
</pre>

#### Glob expansion of slurm outputs
Because slurm output files can act as job id inputs, the following can
get all seff information for a given job name:

<pre>slurm_out  ❯❯❯ reportseff split_ubam_24\*<b>
                 JobID      State          Elapsed   CPUEff   MemEff  </b>
   split_ubam_24342816    COMPLETED       23:30:32   99.9%    4.5%
   split_ubam_24342914    COMPLETED       22:40:51   99.9%    4.6%
   split_ubam_24393599    COMPLETED       23:43:36   99.4%    4.4%
   split_ubam_24393655    COMPLETED       21:36:58   99.3%    4.5%
   split_ubam_24418960     RUNNING        02:53:11    ---      ---
   split_ubam_24419972     RUNNING        01:26:26    ---      ---
</pre>

#### No arguments
Without arguments, reportseff will try to find slurm output files in the
current directory.  Combine with `watch` to monitor job progress:
`watch -cn 300 reportseff --modified-sort`
<pre><b>
                    JobID           State          Elapsed   CPUEff   MemEff  </b>
      split_ubam_24418960          RUNNING        02:56:14    ---      ---
   fastq_to_ubam_24419971          RUNNING        01:29:29    ---      ---
      split_ubam_24419972          RUNNING        01:29:29    ---      ---
   fastq_to_ubam_24393600         COMPLETED     1-02:00:47   58.3%    41.1%
       map_fastq_24419330          RUNNING        02:14:53    ---      ---
       map_fastq_24419323          RUNNING        02:15:24    ---      ---
       map_fastq_24419324          RUNNING        02:15:24    ---      ---
       map_fastq_24419322          RUNNING        02:15:24    ---      ---
   mark_adapters_24418437         COMPLETED       01:29:23   99.8%    48.2%
   mark_adapters_24418436         COMPLETED       01:29:03   99.9%    47.4%
</pre>

#### Filtering slurm output files
One useful application of reportseff is filtering a directory of slurm output
files based on the state or time since running.  Additionally, if only the
jobid is specified as a format output, the filenames will be returned in a
pipe-friendly manner:
<pre>old_runs   ❯❯❯ reportseff --since d=4 --state Timeout
<b>
                   JobID   State      Elapsed  CPUEff   MemEff </b>
  call_variants_31550458  TIMEOUT    20:05:17  99.5%     0.0%
  call_variants_31550474  TIMEOUT    20:05:17  99.6%     0.0%
  call_variants_31550500  TIMEOUT    20:05:08  99.7%     0.0%
old_runs   ❯❯❯ reportseff --since d=4 --state Timeout --format jobid
call_variants_31550458
call_variants_31550474
call_variants_31550500
</pre>
To find all lines with `output:` in jobs which have timed out or failed
in the last 4 days:
```
reportseff --since 'd=4' --state TO,F --format jobid | xargs grep output:
```

### Arguments
Jobs can be passed as arguments in the following ways:
- Job ID such as 1234567.  If the id is part of an array job, only the element
for that ID will be displayed.  If the id is the base part of an array job,
all elements in the array will be displayed.
- Array Job ID such as 1234567\_89.  Will display only the element specified.
- Slurm output file.  Format must be BASE\_%A\_%a.  BASE is optional as is a
'.out' suffix.  Unix glob expansions can also be used to filter which jobs
are displayed.
- From current directory.  If no argument is supplied, reportseff will attempt
to find slurm output files in the current directory as described above.
- Supplying a directory as a single argument will override the current
directory to check for slurm outputs.

### Options
- --color/--no-color: Force color output or not.  By default, will force color
  output.  With the no-color flag, click will strip color codes for everything
  besides stdout.
- --modified-sort: Instead of sorting by filename/jobid, sort by last
  modification time of the slurm output file.
- --debug: Write sacct result to stderr.
- --user: Ignore job arguments and instead query sacct with provided user.
  Returns all jobs from the last week.
- --state: Output only jobs with states matching one of the provided options.
  Accepts comma separated values of job codes (e.g. 'R') or full names
  (e.g. RUNNING).  Case insensitive.
- --format: Provide a comma separated list of columns to produce. Prefixing the
  argument with `+` adds the specified values to the defaults.  Values can
  be any valid column name to sacct and the custom efficiency values: TimeEff,
  cpuEff, MemEff.  Can also optionally set alignment (<, ^, >) and maximum width.
  Default is center-aligned with a width of the maximum column entry.  For
  example, `--format 'jobid%>,state%10,memeff%<5'` produces 3 columns with:
  - JobId aligned right, width set automatically
  - State with width 10 (center aligned by default)
  - MemEff aligned left, width 5
- --since: Limit results to those occurring after the specified time.  Accepts
  sacct formats and a comma separated list of key/value pairs.  To get jobs in
  the last hour and a half, can pass `h=1,m=30`.

## Acknowledgements
The code for calling sacct and parsing the returning information was taken
from [Slurmee](https://github.com/PrincetonUniversity/slurmee).
