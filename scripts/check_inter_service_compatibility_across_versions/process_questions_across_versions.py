import json
import subprocess
import tempfile


RECORDING_FILE = "recorded_questions.jsonl"

CHILD_VERSIONS = [
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
]


with open(RECORDING_FILE) as f:
    questions = f.readlines()


for child_version in CHILD_VERSIONS:
    version_string = f"\nCHILD VERSION {child_version}"
    print(version_string)
    print("=" * (len(version_string) - 1))

    print("Installing version...")
    subprocess.run(["git", "checkout", child_version], capture_output=True)
    subprocess.run(["poetry", "install", "--all-extras"], capture_output=True)

    for question in questions:
        with tempfile.NamedTemporaryFile() as temporary_file:
            with open(temporary_file.name, "w") as f:
                f.write(question)

            process = subprocess.run(
                ["python", "process_questions_across_versions/process_question.py", temporary_file.name],
                capture_output=False,
            )

            if process.returncode != 0:
                parent_sdk_version = json.loads(question)["parent_sdk_version"]

                print(
                    f"Questions from parent SDK version {parent_sdk_version} are incompatible with child SDK version "
                    f"{child_version}."
                )
