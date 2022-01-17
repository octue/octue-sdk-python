import os

from setuptools import setup


def git_version():
    """Get the commit hash of the HEAD commit to use as a package version.

    :return str:
    """
    return os.system("git rev-parse HEAD")


# This file makes your module installable as a library. It's not essential for running apps with twined.

setup(
    name="template-python-fractal",
    version=git_version(),
    py_modules=["app"],
)
