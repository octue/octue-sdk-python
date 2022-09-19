import subprocess


versions = (
    "0.35.0",
    # "0.34.1",
    # "0.34.0",
    # "0.33.0",
    # "0.32.0",
    # "0.31.0",
    # "0.30.0",
    # "0.29.11",
    # "0.29.10",
    # "0.29.9",
    # "0.29.8",
    # "0.29.7",
    # "0.29.6",
    # "0.29.5",
    # "0.29.4",
    # "0.29.3",
    # "0.29.2",
    # "0.29.1",
    # "0.29.0",
    # "0.28.2",
    # "0.28.1",
    # "0.28.0",
    # "0.27.3",
    # "0.27.2",
    # "0.27.1",
    # "0.27.0",
    # "0.26.2",
    # "0.26.1",
    # "0.26.0",
    # "0.25.0",
    # "0.24.1",
    # "0.24.0",
    # "0.17.0",
    "0.16.0",
)

# os.environ["TEST_PROJECT_NAME"] = "blah"


for version in versions:
    version_string = f"\nVERSION {version}"
    print(version_string)
    print("=" * (len(version_string) - 1))

    print("Installing version...")
    subprocess.run(["git", "checkout", version], capture_output=False)
    subprocess.run(["poetry", "install", "--all-extras"], capture_output=False)

    print("Recording question...")
    subprocess.run(["python", "record_question.py"])
