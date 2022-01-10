import os

from octue.resources.dataset import Dataset


def create_dataset_with_two_files(dataset_directory_path):
    """Create a dataset in the given directory containing two files.

    :param str dataset_directory_path:
    :return octue.resources.dataset.Dataset:
    """
    paths = [os.path.join(dataset_directory_path, filename) for filename in ("file_0.txt", "file_1.txt")]

    for path, data in zip(paths, range(len(paths))):
        with open(path, "w") as f:
            f.write(str(data))

    return Dataset.from_local_directory(dataset_directory_path)
