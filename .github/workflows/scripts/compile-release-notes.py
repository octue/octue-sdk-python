import re
import subprocess


RED = '\033[0;31m'
GREEN = "\033[0;32m"
NO_COLOUR = '\033[0m'


SEMANTIC_VERSION_PATTERN = r"tag: (\d+\.\d+\.\d+)"


COMMIT_CODES_TO_HEADINGS_MAPPING = {
    "FEA": "### New features",
    "ENH": "### Enhancements",
    "FIX": "### Fixes",
    "OPS": "### Operations",
    "DEP": "### Dependencies",
    "REF": "### Refactoring",
    "TST": "### Testing",
    "MRG": "### Other",
    "REV": "### Reversions",
    "CHO": "### Chores",
    "WIP": "### Other",
    "DOC": "### Other",
    "STY": "### Other",
}


def compile_release_notes():
    process = subprocess.run(["git", "log", "--pretty=format:%s|%d"], capture_output=True)
    oneline_git_log = process.stdout.strip().decode()

    parsed_commits = []

    for commit in oneline_git_log.splitlines():
        split_commit = commit.split("|")

        if len(split_commit) == 2:
            message, decoration = split_commit

            try:
                code, message, decoration = (*message.split(":"), decoration)
            except ValueError:
                print(f"{RED}Warning:{NO_COLOUR} {commit} not in correct format; ignoring.")

            if "tag" in decoration:
                if re.compile(SEMANTIC_VERSION_PATTERN).search(decoration):
                    break

            parsed_commits.append((code.strip(), message.strip(), decoration.strip()))

    release_notes = {heading: [] for heading in COMMIT_CODES_TO_HEADINGS_MAPPING.values()}

    for code, message, _ in parsed_commits:
        try:
            release_notes[COMMIT_CODES_TO_HEADINGS_MAPPING[code]].append(message)
        except KeyError:
            release_notes["### Other"].append(message)

    release_notes_for_printing = "## Contents\n\n"

    for heading, notes in release_notes.items():

        if len(notes) == 0:
            continue

        note_lines = "\n".join("- [x] " + note for note in notes)
        release_notes_for_printing += f"{heading}\n{note_lines}\n\n"

    return release_notes_for_printing.strip()
