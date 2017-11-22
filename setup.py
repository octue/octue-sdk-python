from setuptools import setup

setup(
    name="octue",
    version=0.1,
    py_modules=['cli'],
    install_requires=['Click'],
    entry_points='''
    [console_scripts]
    octue=octue_app
    ''',
)
