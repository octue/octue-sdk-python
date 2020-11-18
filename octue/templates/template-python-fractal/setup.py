import os
from setuptools import setup


def git_version():
    return os.system("git rev-parse HEAD")


setup(
    name="template-python-fractal",  # Change this if you want to make your module installable as a library
    version=git_version(),
    py_modules=["app"],
)
