from shutil import which
import sys

import click

from . import __version__
from .db_inquirer import Sacct_Inquirer
from .job_collection import Job_Collection
from .output_renderer import Output_Renderer


@click.command()
@click.option(
    "--modified-sort",
    default=False,
    is_flag=True,
    help="If set, will sort outputs by modified time of files",
)
@click.option(
    "--color/--no-color",
    default=True,
    help="Force color output. No color will use click defaults",
)
@click.option(
    "--format",
    "format_str",
    default="JobID%>,State,Elapsed%>,CPUEff,MemEff",
    help="Comma-separated list of columns to include.  Options "
    "are any valide sacct input along with CPUEff, MemEff, and "
    "TimeEff.  A width and alignment may optionally be provided "
    'after "%", e.g. JobID%>15 aligns job id right with max '
    "width of 15 characters. Generally NAME[%[ALIGNMENT][WIDTH]].  "
    "Prefix with a + to add to the defaults. "
    "A single format token will suppress the header line.",
)
@click.option(
    "--debug", default=False, is_flag=True, help="Print raw db query to stderr"
)
@click.option(
    "-u",
    "--user",
    default="",
    help="Ignore jobs, return all jobs in last week from user",
)
@click.option(
    "-s", "--state", default="", help="Only include jobs with the specified state"
)
@click.option(
    "-S",
    "--since",
    default="",
    help="Only include jobs before this time.  Can be valid sacct "
    "or as a comma separated list of time deltas, e.g. d=2,h=1 "
    "means 2 days, 1 hour before current time.  Weeks, days, "
    "hours, and minutes can use case-insensitive abbreviations. "
    "Minutes is the minimum resolution, while weeks is the coarsest.",
)
@click.version_option(version=__version__)
@click.argument("jobs", nargs=-1)
def main(modified_sort, color, format_str, debug, user, jobs, state, since):

    if format_str.startswith("+"):
        format_str = "JobID%>,State,Elapsed%>,CPUEff,MemEff," + format_str[1:]

    output, entries = get_jobs(
        jobs=jobs,
        format_str=format_str,
        user=user,
        modified_sort=modified_sort,
        state=state,
        since=since,
        debug=debug,
    )

    if entries > 20:
        click.echo_via_pager(output, color=color)
    else:
        click.echo(output, color=color)


def get_jobs(
    jobs, format_str="", user="", debug=False, modified_sort=False, state="", since=""
):
    job_collection = Job_Collection()

    inquirer, renderer = get_implementation(format_str)

    inquirer.set_state(state)

    inquirer.set_since(since)

    try:
        if user:
            inquirer.set_user(user)
        else:
            job_collection.set_jobs(jobs)

    except ValueError as error:
        click.secho(str(error), fg="red", err=True)
        sys.exit(1)

    try:
        result = inquirer.get_db_output(
            renderer.query_columns, job_collection.get_jobs(), debug
        )
    except Exception as error:
        click.secho(str(error), fg="red", err=True)
        sys.exit(1)

    if debug:
        click.echo(result[1], err=True)
        result = result[0]

    for entry in result:
        try:
            job_collection.process_entry(entry, user_provided=(user != ""))
        except Exception as error:
            click.echo(f"Error processing entry: {entry}", err=True)
            raise error

    jobs = job_collection.get_sorted_jobs(modified_sort)
    jobs = [j for j in jobs if j.state]

    return renderer.format_jobs(jobs), len(jobs)


def get_implementation(format_str):
    if which("sacct") is not None:
        inquirer = Sacct_Inquirer()
        renderer = Output_Renderer(inquirer.get_valid_formats(), format_str)
    else:
        click.secho("No supported scheduling systems found!", fg="red", err=True)
        sys.exit(1)

    return inquirer, renderer