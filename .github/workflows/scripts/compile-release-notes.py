import re
import subprocess
import sys


LAST_RELEASE = "LAST_RELEASE"
LAST_PULL_REQUEST = "LAST_PULL_REQUEST"

SEMANTIC_VERSION_PATTERN = r"tag: (\d+\.\d+\.\d+)"
PULL_REQUEST_INDICATOR = "Merge pull request #"

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
    """A release/pull request notes compiler. The notes are pulled together from Conventional Commit messages, stopping
    at the specified stop point. The stop point can either be the last merged pull request in the branch or the last
    semantically-versioned release tagged in the branch.

    :param str header:
    :param str list_item_symbol:
    :param dict|None commit_codes_to_headings_mapping:
    :param str stop_point:
    :return None:
    """

    def __init__(
        self,
        stop_point,
        header="## Contents",
        list_item_symbol="- [x] ",
        commit_codes_to_headings_mapping=None,
    ):
        if stop_point.upper() not in {LAST_RELEASE, LAST_PULL_REQUEST}:
            raise ValueError(f"`stop_point` must be one of {LAST_RELEASE, LAST_PULL_REQUEST!r}; received {stop_point!r}.")

        self.stop_point = stop_point.upper()
        self.header = header
        self.list_item_symbol = list_item_symbol
        self.commit_codes_to_headings_mapping = commit_codes_to_headings_mapping or COMMIT_CODES_TO_HEADINGS_MAPPING

    def compile_release_notes(self):
        """Compile the release or pull request notes into a multiline string, sorting the commit messages into headed
        sections according to their commit codes and the commit-codes-to-headings mapping.

        :return str:
        """
        git_log = subprocess.run(["git", "log", "--pretty=format:%s|%d"], capture_output=True).stdout.strip().decode()
        parsed_commits, unparsed_commits = self._parse_commit_messages(git_log)
        categorised_commit_messages = self._categorise_commit_messages(parsed_commits, unparsed_commits)
        return self._build_release_notes(categorised_commit_messages)

    def _parse_commit_messages(self, formatted_oneline_git_log):
        """Parse commit messages from the git log (formatted using `--pretty=format:%s|%d`) until the stop point is
        reached. The parsed commit messages are returned separately to any that fail to parse.

        :param str formatted_oneline_git_log:
        :return list(tuple), list(str):
        """
        parsed_commits = []
        unparsed_commits = []

        for commit in formatted_oneline_git_log.splitlines():
            split_commit = commit.split("|")

            if len(split_commit) == 2:
                message, decoration = split_commit

                try:
                    code, message = message.split(":")
                except ValueError:
                    unparsed_commits.append(message.strip())
                    continue

                if self.stop_point == LAST_RELEASE:
                    if "tag" in decoration:
                        if re.compile(SEMANTIC_VERSION_PATTERN).search(decoration):
                            break

                if self.stop_point == LAST_PULL_REQUEST:
                    if PULL_REQUEST_INDICATOR in message:
                        break

                parsed_commits.append((code.strip(), message.strip(), decoration.strip()))

        return parsed_commits, unparsed_commits

    def _categorise_commit_messages(self, parsed_commits, unparsed_commits):
        """Categorise the commit messages into headed sections. Unparsed commits are put under an "uncategorised"
        header.

        :param iter(tuple)) parsed_commits:
        :param iter(str) unparsed_commits:
        :return dict:
        """
        release_notes = {heading: [] for heading in self.commit_codes_to_headings_mapping.values()}

        for code, message, _ in parsed_commits:
            try:
                release_notes[self.commit_codes_to_headings_mapping[code]].append(message)
            except KeyError:
                release_notes["### Other"].append(message)

        release_notes["### Uncategorised!"] = unparsed_commits
        return release_notes

    def _build_release_notes(self, categorised_commit_messages):
        """Build the the categorised commit messages into a single multi-line string ready to be used as formatted
        release notes.

        :param dict categorised_commit_messages:
        :return str:
        """
        release_notes_for_printing = f"{self.header}\n\n"

        for heading, notes in categorised_commit_messages.items():

            if len(notes) == 0:
                continue

            note_lines = "\n".join(self.list_item_symbol + note for note in notes)
            release_notes_for_printing += f"{heading}\n{note_lines}\n\n"

        return release_notes_for_printing.strip()


if __name__ == "__main__":
    release_notes = ReleaseNoteCompiler(*sys.argv[1:]).compile_release_notes()
    print(release_notes)
