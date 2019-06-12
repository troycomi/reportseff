import click
import subprocess
import sys
from reportseff.job_collection import Job_Collection


@click.command()
@click.option('--modified-sort', default=False, is_flag=True,
              help='If set, will sort outputs by modified time of files')
@click.option('--color/--no-color', default=True,
              help='Force color output. No color will use click defaults')
@click.option('--directory', default='')
@click.option('--debug', default=False, is_flag=True,
              help='Print raw sacct information to stderr')
@click.argument('jobs', nargs=-1)
def reportseff(modified_sort, color, directory, jobs, debug):
    job_collection = Job_Collection()
    try:
        if jobs == ():
            job_collection.set_slurm_out_dir(directory)
        else:
            job_collection.set_slurm_jobs(jobs)

    except ValueError as e:
        click.secho(str(e), fg='red', err=True)
        sys.exit(1)

    command_args = []
    command_args.append('sacct')
    command_args.append('-P')
    command_args.append('-n')
    command_args.append('--format=' + job_collection.get_slurm_format())
    command_args.append('--jobs=' + job_collection.get_slurm_jobs())

    result = subprocess.run(
        args=command_args,
        stdout=subprocess.PIPE,
        encoding='utf8',
        check=True,
        universal_newlines=True)

    if result.returncode != 0:
        click.secho('Error running sacct!', fg='red', err=True)
        sys.exit(1)

    if debug:
        click.echo(result.stdout, err=True)

    lines = result.stdout.split('\n')
    for i, line in enumerate(lines[:-1]):  # remove last, blank newline
        try:
            job_collection.process_line(line)
        except Exception as e:
            click.echo(f'SACCT:\n{line}', err=True)
            raise(e)

    output, entries = job_collection.get_output(modified_sort)

    if entries > 20:
        click.echo_via_pager(output, color=color)
    else:
        click.echo(output, color=color)
