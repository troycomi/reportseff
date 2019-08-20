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
<pre>
<b>   Name         State          Time       CPU    Memory  </b>
   24371789<font color="#4E9A06">   COMPLETED    </font>   03:08:03   71.2%    45.7%  
</pre>

#### Single array job
Providing either the raw job id or the array job id will get efficiency 
information for a single element of the array job.  `reportseff 24220929_421` 
and `reportseff 24221219` generate:
<pre>
<b>     Name           State          Time       CPU    Memory  </b>
   24220929_421<font color="#4E9A06">   COMPLETED    </font>   00:09:34 <font color="#4E9A06">  99.0%  </font>  34.6%  
</pre>

#### Array job group
If the base job id of an array is provided, all elements of the array will
be added to the output. `reportseff 24220929`
<pre><b>     Name           State          Time       CPU    Memory  </b>
     24220929_1<font color="#4E9A06">   COMPLETED    </font>   00:10:43 <font color="#4E9A06">  99.2%  </font>  33.4%  
    24220929_11<font color="#4E9A06">   COMPLETED    </font>   00:10:10 <font color="#4E9A06">  99.2%  </font>  37.5%  
    24220929_21<font color="#4E9A06">   COMPLETED    </font>   00:09:25 <font color="#4E9A06">  98.8%  </font>  36.1%  
    24220929_31<font color="#4E9A06">   COMPLETED    </font>   00:09:19 <font color="#4E9A06">  98.9%  </font>  33.3%  
    24220929_41<font color="#4E9A06">   COMPLETED    </font>   00:09:23 <font color="#4E9A06">  98.9%  </font>  33.3%  
    24220929_51<font color="#4E9A06">   COMPLETED    </font>   00:08:02 <font color="#4E9A06">  98.5%  </font>  36.3%  
	...
   24220929_951<font color="#4E9A06">   COMPLETED    </font>   00:25:12 <font color="#4E9A06">  99.5%  </font>  33.5%  
   24220929_961<font color="#4E9A06">   COMPLETED    </font>   00:39:26 <font color="#4E9A06">  99.7%  </font>  34.1%  
   24220929_971<font color="#4E9A06">   COMPLETED    </font>   00:24:11 <font color="#4E9A06">  99.5%  </font>  34.2%  
   24220929_981<font color="#4E9A06">   COMPLETED    </font>   00:24:50 <font color="#4E9A06">  99.5%  </font>  44.3%  
   24220929_991<font color="#4E9A06">   COMPLETED    </font>   00:13:05 <font color="#4E9A06">  98.7%  </font>  33.7%  
</pre>

#### Glob expansion of slurm outputs
Because slurm output files can act as job id inputs, the following can
get all seff information for a given job name:

<pre>slurm_out  <font color="#CC0000">❯</font><font color="#C4A000">❯</font><font color="#4E9A06">❯</font> reportseff split_ubam_24*
<b>         Name              State          Time       CPU    Memory  </b>
   split_ubam_24342816<font color="#4E9A06">   COMPLETED    </font>   23:30:32 <font color="#4E9A06">  99.9%  </font><font color="#CC0000">  4.5%   </font>
   split_ubam_24342914<font color="#4E9A06">   COMPLETED    </font>   22:40:51 <font color="#4E9A06">  99.9%  </font><font color="#CC0000">  4.6%   </font>
   split_ubam_24393599<font color="#4E9A06">   COMPLETED    </font>   23:43:36 <font color="#4E9A06">  99.4%  </font><font color="#CC0000">  4.4%   </font>
   split_ubam_24393655<font color="#4E9A06">   COMPLETED    </font>   21:36:58 <font color="#4E9A06">  99.3%  </font><font color="#CC0000">  4.5%   </font>
   split_ubam_24418960<font color="#06989A">    RUNNING     </font>   02:53:11    ---      ---   
   split_ubam_24419972<font color="#06989A">    RUNNING     </font>   01:26:26    ---      ---   
</pre>

#### No arguments
Without arguments, reportseff will try to find slurm output files in the
current directory.  Combine with `watch` to monitor job progress:
`watch -cn 300 reportseff --modified-sort`
<pre><b>          Name                State          Time       CPU    Memory  </b>
      split_ubam_24418960<font color="#06989A">    RUNNING     </font>   02:56:14    ---      ---
   fastq_to_ubam_24419971<font color="#06989A">    RUNNING     </font>   01:29:29    ---      ---
      split_ubam_24419972<font color="#06989A">    RUNNING     </font>   01:29:29    ---      ---
   fastq_to_ubam_24393600<font color="#4E9A06">   COMPLETED    </font> 1-02:00:47   58.3%    41.1%
       map_fastq_24419330<font color="#06989A">    RUNNING     </font>   02:14:53    ---      ---
       map_fastq_24419323<font color="#06989A">    RUNNING     </font>   02:15:24    ---      ---
       map_fastq_24419324<font color="#06989A">    RUNNING     </font>   02:15:24    ---      ---
       map_fastq_24419322<font color="#06989A">    RUNNING     </font>   02:15:24    ---      ---
   mark_adapters_24418437<font color="#4E9A06">   COMPLETED    </font>   01:29:23 <font color="#4E9A06">  99.8%  </font>  48.2%
   mark_adapters_24418436<font color="#4E9A06">   COMPLETED    </font>   01:29:03 <font color="#4E9A06">  99.9%  </font>  47.4%
</pre>

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
- --format: Provide a comma separated list of columns to produce.  Values can
be any valid column name to sacct and the custom efficiency values: TimeEff,
cpuEff, MemEff.  Can also optionally set alignment (<, ^, >) and maximum width.
Default is center-aligned with a width of the maximum column entry.  For
example, `--format 'jobid%>,state%10,memeff%<5'` produces 3 columns with:
  - JobId aligned right, width set automatically
  - State with width 10 (center aligned by default)
  - MemEff aligned left, width 5

## Acknowledgements
The code for calling sacct and parsing the returning information was taken
from [Slurmee](https://github.com/PrincetonUniversity/slurmee).
