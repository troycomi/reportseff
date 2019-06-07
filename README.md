# reportseff

> A python script for tabular display of slurm efficiency information

## Usage
`reportseff` runs on python >= 3.7.
The only external dependency is click (>= 7.0).
Calling `pip install --editable .` will create command line bindings and
install click.

### Sample Usage

### Arguments
Jobs can be passed as arguments in the following ways:
- Job ID such as 1234567.  If the id is part of an array job, only the element
for that ID will be displayed.  If the id is the base part of an array job,
all elements in the array will be displayed.
- Array Job ID such as 1234567\_89.  Will display only the element specified
- Slurm output file.  Format must be BASE\_%A\_%a.  BASE is optional as is a
'.out' suffix.  Unix glob expansions can also be used to filter which jobs
are displayed.
- From current directory.  If no argument is supplied, reportseff will attempt
to find slurm output files in the current directory as described above

### Options
- --directory: Override current directory to check for slurm outputs.  Will
preempt any supplied arguments.
- --color/--no-color: Force color output or not.  By default, will force color
output.  With the no-color flag, click will strip color codes for everything
besides stdout.
- --modified-sort: Instead of sorting by filename/jobid, sort by last 
modification time of the slurm output file.
- --debug: Write sacct result to stderr.

## Acknowledgements
The code for calling sacct and parsing the returning information was taken
from (Slurmee)[https://github.com/PrincetonUniversity/slurmee].
