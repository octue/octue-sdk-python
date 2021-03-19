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

with open("LICENSE") as f:
    license_text = f.read()

setup(
    name="octue",
    version="0.1.11",
    py_modules=["cli"],
    install_requires=[
        "blake3>=0.1.8",
        "click>=7.1.2",
        "coolname>=1.1.0",
        "crcmod>=1.7",
        "google-cloud-pubsub>=2.2.0",
        "google-cloud-storage>=1.35.1",
        "twined==0.0.16",
    ],
    url="https://www.github.com/octue/octue-sdk-python",
    license="MIT",
    author="Thomas Clark (github: thclark)",
    author_email="support@octue.com",
    description="A package providing template applications for data services, and a python SDK to the Octue API",
    long_description=readme_text,
    zip_safe=False,  # Allows copying of templates as whole directory trees
    packages=find_packages(exclude=("tests", "docs")),
    include_package_data=True,
    entry_points="""
    [console_scripts]
    octue-app=octue.cli:octue_cli
    """,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    keywords=["digital", "twins", "twined", "data", "services", "science", "api", "apps", "ml"],
)
