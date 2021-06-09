import sys
import re
import subprocess


SEMANTIC_VERSION_PATTERN = r"tag: (\d+\.\d+\.\d+)"

TAG = "tag"

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


class ReleaseNoteCompiler:

    def __init__(self, header="## Contents", list_item_symbol="- [x] ", commit_codes_to_headings_mapping=None, stop_point=TAG):
        self.header = header
        self.list_item_symbol = list_item_symbol
        self.commit_codes_to_headings_mapping = commit_codes_to_headings_mapping or COMMIT_CODES_TO_HEADINGS_MAPPING
        self.stop_point = stop_point

    def compile_release_notes(self):
        git_log = subprocess.run(["git", "log", "--pretty=format:%s|%d"], capture_output=True).stdout.strip().decode()
        parsed_commits, unparsed_commits = self._parse_commits(git_log)
        release_notes = self._build_release_notes(parsed_commits, unparsed_commits)
        return self._format_release_notes(release_notes)

    def _parse_commits(self, oneline_git_log):
        parsed_commits = []
        unparsed_commits = []

        for commit in oneline_git_log.splitlines():
            split_commit = commit.split("|")

            if len(split_commit) == 2:
                message, decoration = split_commit

                try:
                    code, message = message.split(":")
                except ValueError:
                    unparsed_commits.append(message.strip())
                    continue

                if self.stop_point == TAG:
                    if TAG in decoration:
                        if re.compile(SEMANTIC_VERSION_PATTERN).search(decoration):
                            break

                if code == self.stop_point:
                    break

                parsed_commits.append((code.strip(), message.strip(), decoration.strip()))

        return parsed_commits, unparsed_commits

    def _build_release_notes(self, parsed_commits, unparsed_commits):
        release_notes = {heading: [] for heading in self.commit_codes_to_headings_mapping.values()}

        for code, message, _ in parsed_commits:
            try:
                release_notes[self.commit_codes_to_headings_mapping[code]].append(message)
            except KeyError:
                release_notes["### Other"].append(message)

        release_notes["### Uncategorised"] = unparsed_commits
        return release_notes

    def _format_release_notes(self, release_notes):
        release_notes_for_printing = f"{self.header}\n\n"

        for heading, notes in release_notes.items():

            if len(notes) == 0:
                continue

            note_lines = "\n".join(self.list_item_symbol + note for note in notes)
            release_notes_for_printing += f"{heading}\n{note_lines}\n\n"

        return release_notes_for_printing.strip()


if __name__ == '__main__':
    try:
        stop_point = sys.argv[1]
    except IndexError:
        stop_point = TAG

    release_notes = ReleaseNoteCompiler(stop_point=stop_point).compile_release_notes()
    print(release_notes)
