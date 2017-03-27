from setuptools import setup

setup(
    name="oasys",
    version=0.1,
    py_modules=['cli'],
    install_requires=['Click'],
    entry_points='''
    [console_scripts]
    oasys-app=oasys_app
    ''',
)
