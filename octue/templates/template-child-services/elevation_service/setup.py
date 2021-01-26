import os
from setuptools import setup


def git_version():
    return os.system("git rev-parse HEAD")


# This file makes your module installable as a library. It's not essential for running apps with twined.
setup(name="elevation-service", version=git_version(), py_modules=["app"])
