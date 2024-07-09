"""Nox sessions."""

import tempfile

import nox


locations = "src", "tests", "noxfile.py"
nox.options.sessions = "lint", "safety", "mypy", "pytype", "tests", "tests_old_click"
package = "reportseff"


def install_with_constraints(session, *args, **kwargs):
    """Install packages with poetry's lock file."""
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--with",
            "dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        # strip extras
        session.run(
            "sed",
            "-i",
            r"s/\[.*\]//g",
            f"{requirements.name}",
            external=True,
        )
        session.install(f"--constraint={requirements.name}", *args, **kwargs)


@nox.session(python=["3.8", "3.9", "3.10", "3.11"])
def tests(session):
    """Run test suite with pytest and coverage."""
    args = session.posargs
    session.install(".")
    install_with_constraints(
        session, "coverage[toml]", "pytest", "pytest-cov", "pytest-mock"
    )
    session.run("pytest", "--cov", *args)


@nox.session(python=["3.8", "3.9", "3.10", "3.11"])
def tests_old_click(session):
    """Run test suite with pytest and coverage, using click 6.7."""
    args = session.posargs
    session.install(".")
    session.run("pip", "install", "click==6.7")
    install_with_constraints(
        session, "coverage[toml]", "pytest", "pytest-cov", "pytest-mock"
    )
    session.run("pytest", "--cov", *args)


@nox.session(python="3.10")
def black(session):
    """Format code with black."""
    args = session.posargs or locations
    install_with_constraints(session, "black")
    session.run("black", *args)


@nox.session(python="3.10")
def lint(session):
    """Lint code with flake8."""
    args = session.posargs or locations
    install_with_constraints(
        session,
        "flake8",
        "flake8-annotations",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-import-order",
        "darglint",
    )
    session.run("flake8", *args)


@nox.session(python="3.10")
def safety(session):
    """Scan dependencies for insecure packages."""
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--with",
            "dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        install_with_constraints(session, "safety")
        session.run(
            "safety",
            "check",
            "--ignore=70612",  # jinja template injection
            f"--file={requirements.name}",
            "--full-report",
        )


@nox.session(python=["3.8", "3.9"])
def mypy(session):
    """Type-check with mypy."""
    args = session.posargs or locations
    install_with_constraints(session, "mypy", "types-click")
    session.run("mypy", *args)


@nox.session(python="3.10")
def pytype(session):
    """Run the static type checker pytype."""
    args = session.posargs or ["--disable=import-error", *locations]
    install_with_constraints(session, "pytype")
    session.run("pytype", *args)


@nox.session(python="3.10")
def typeguard(session):
    """Runtime type checking during unit tests."""
    args = session.posargs
    session.run("poetry", "install", "--only", "main", external=True)
    install_with_constraints(session, "pytest", "pytest-mock", "typeguard")
    session.run("pytest", f"--typeguard-packages={package}", *args)


@nox.session(python="3.10")
def coverage(session):
    """Upload coverage data."""
    install_with_constraints(session, "coverage[toml]", "codecov")
    session.run("coverage", "xml", "--fail-under=0")
    session.run("codecov", *session.posargs)
