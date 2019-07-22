import click
import sys
from shutil import which
from reportseff.job_collection import Job_Collection
from reportseff.db_inquirer import Sacct_Inquirer


@click.command()
@click.option('--modified-sort', default=False, is_flag=True,
              help='If set, will sort outputs by modified time of files')
@click.option('--color/--no-color', default=True,
              help='Force color output. No color will use click defaults')
@click.option('--debug', default=False, is_flag=True,
              help='Print raw db information to stderr')
@click.argument('jobs', nargs=-1)
def reportseff(modified_sort, color, jobs, debug):
    job_collection = Job_Collection()
    if which('sacct') is not None:
        inquirer = Sacct_Inquirer()
    else:
        click.secho('No supported scheduling systems found!',
                    fg='red', err=True)
        sys.exit(1)
    try:
        job_collection.set_jobs(jobs)

    except ValueError as e:
        click.secho(str(e), fg='red', err=True)
        sys.exit(1)

    try:
        result = inquirer.get_db_output(
            job_collection.get_columns(),
            job_collection.get_jobs(),
            debug)
    except Exception as e:
        click.secho(str(e), fg='red', err=True)
        sys.exit(1)

    if debug:
        click.echo(result[1], err=True)
        result = result[0]

    for entry in result:
        try:
            job_collection.process_entry(entry)
        except Exception as e:
            click.echo(entry, err=True)
            raise(e)

    output, entries = job_collection.get_output(modified_sort)

    if entries > 20:
        click.echo_via_pager(output, color=color)
    else:
        click.echo(output, color=color)
