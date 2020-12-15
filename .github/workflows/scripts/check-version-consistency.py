import os
import subprocess
import sys


PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


def release_branch_version_matches_setup_version(package_root):
    os.chdir(package_root)
    process = subprocess.run(["python", os.path.join(package_root, "setup.py"), "--version"], capture_output=True)
    setup_version = process.stdout.strip().decode('utf8')

    process = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True)
    full_branch_name = process.stdout.strip().decode('utf8')
    branch_type, branch_name = full_branch_name.split('/')

    if branch_type != 'release':
        raise TypeError(f'The branch is not a release branch: {full_branch_name!r}')

    if branch_name == setup_version:
        return True

    return False


if __name__ == '__main__':

    try:
        if release_branch_version_matches_setup_version(PACKAGE_ROOT):
            sys.exit(0)

    except TypeError:
        sys.exit(0)

    sys.exit(1)
