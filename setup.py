from setuptools import find_packages, setup

# Note:
#   The Hitchiker's guide to python provides an excellent, standard, method for creating python packages:
#       http://docs.python-guide.org/en/latest/writing/structure/
#
#   To deploy on PYPI follow the instructions at the bottom of:
#       https://packaging.python.org/tutorials/distributing-packages/#uploading-your-project-to-pypi
#   where the username on pypi for all octue modules is 'octue'

with open("README.md") as f:
    readme_text = f.read()

setup(
    name="octue",
    version="0.1.2",
    py_modules=["cli"],
    install_requires=["click>=7.1.2", "twined==0.0.10"],
    url="https://www.github.com/octue/octue-sdk-python",
    license="MIT",
    author="Thomas Clark (github: thclark)",
    author_email="support@octue.com",
    description="A package providing a python SDK to the Octue API and supporting functions for Octue applications",
    long_description=readme_text,
    packages=find_packages(exclude=("tests", "docs")),
)
