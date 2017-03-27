import os
from setuptools import setup


def git_version():
    return os.system('git rev-parse HEAD')


setup(
    name="my_application_name",
    version=git_version(),
    py_modules=['app'],
    entry_points='''
    [console_scripts]
    oasys-app=app:oasys_app
    ''',
)
