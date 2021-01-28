import nox  # type: ignore
from pathlib import Path

nox.options.sessions = ["tests", "lint", "build"]

python = ["3.7", "3.8", "3.9"]


lint_dependencies = [
    "flake8",
    "flake8-comprehensions",
    "flake8-bugbear",
    "check-manifest",
]


@nox.session(python=python)
def tests(session):
    session.install(".", "pytest", "pytest-cov", "docker")
    session.run("pytest", "--cov=src")
    session.notify("cover")


@nox.session
def cover(session):
    """Coverage analysis."""
    session.install("coverage")
    session.run("coverage", "report", "--show-missing", "--fail-under=85")
    session.run("coverage", "erase")


@nox.session(python="3.8")
def lint(session):
    """Lint using flake8."""
    session.install(*lint_dependencies)
    files = ["tests", "src"] + [str(p) for p in Path(".").glob("*.py")]
    session.run("flake8", *files, '--statistics', '--count')

    # Lint docstrings only in the actual package (i.e. src/)
    session.install("flake8-docstrings")
    session.run("flake8", "src", '--statistics', '--count')

    session.run("python", "setup.py", "check", "--metadata", "--strict")
    if "--skip_manifest_check" in session.posargs:
        pass
    else:
        session.run("check-manifest")


@nox.session(python="3.8")
def build(session):
    session.install("setuptools")
    session.install("wheel")
    session.install("twine")
    session.run("rm", "-rf", "dist", "build", external=True)
    session.run("python", "setup.py", "--quiet", "sdist", "bdist_wheel")


@nox.session(python="3.8")
def publish(session):
    """Build and publish on PyPI."""
    build(session)
    print("REMINDER: Has the changelog been updated?")
    session.run("python", "-m", "twine", "upload", "dist/*")


@nox.session(python="3.8")
def publish_test(session):
    """Build and publish on TestPyPI."""
    build(session)
    print("REMINDER: Has the changelog been updated?")
    session.run("python", "-m", "twine", "upload",
                "--repository", "testpypi", "dist/*")
