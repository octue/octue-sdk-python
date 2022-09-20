import subprocess


VERSIONS = (
    "0.35.0",
    "0.34.1",
    "0.34.0",
    "0.33.0",
    "0.32.0",
    "0.31.0",
    "0.30.0",
    "0.29.11",
    "0.29.10",
    "0.29.9",
    "0.29.8",
    "0.29.7",
    "0.29.6",
    "0.29.5",
    "0.29.4",
    "0.29.3",
    "0.29.2",
    "0.29.1",
    "0.29.0",
    "0.28.2",
    "0.28.1",
    "0.28.0",
    "0.27.3",
    "0.27.2",
    "0.27.1",
    "0.27.0",
    "0.26.2",
    "0.26.1",
    "0.26.0",
    "0.25.0",
    "0.24.1",
    "0.24.0",
    "0.17.0",
    "0.16.0",  # The first version installable using `poetry` is `0.16.0`.
)


def record_questions_across_versions():
    """Checkout, install, and record questions from the versions of `octue` given in `VERSIONS`. Questions are recorded
    to the file given in the `record_questions.py` script.

    :return None:
    """
    for version in VERSIONS:
        version_string = f"\nVERSION {version}"
        print(version_string)
        print("=" * (len(version_string) - 1))

        print("Installing version...")
        checkout_process = subprocess.run(["git", "checkout", version], capture_output=True)

        if checkout_process.returncode != 0:
            raise ChildProcessError(f"Git checkout of version {version} failed.")

        install_process = subprocess.run(["poetry", "install", "--all-extras"], capture_output=True)

        if install_process.returncode != 0:
            raise ChildProcessError(f"Installation of version {version} failed.")

        print("Creating and recording question...")
        subprocess.run(["python", "record_question.py"])


if __name__ == "__main__":
    record_questions_across_versions()
