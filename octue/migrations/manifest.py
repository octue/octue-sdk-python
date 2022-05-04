import warnings


def translate_datasets_list_to_dictionary(datasets, keys=None):
    """Translate the old datasets list format to the new dictionary format for use in a manifest, and issue a
    deprecation warning.

    :param list(octue.resources.dataset.Dataset|str|dict) datasets:
    :param dict(str, int) keys: a mapping of dataset name/key to the index of the dataset in the `datasets` parameter
    :return dict: datasets and keys combined as a dictionary of keys mapped to datasets
    """
    keys = keys or {}
    keys = {index: name for name, index in keys.items()}

    translated_datasets = {}

    for index, dataset in enumerate(datasets):
        try:
            key = keys.get(index) or getattr(dataset, "name", None) or dataset.get("name") or f"dataset_{index}"
        except AttributeError:
            key = f"dataset_{index}"

        translated_datasets[key] = dataset

    warnings.warn(
        message=(
            "Datasets belonging to a manifest should be provided as a dictionary mapping their name/key to "
            "themselves. Support for providing a list of datasets will be phased out soon."
        ),
        category=DeprecationWarning,
    )

    return translated_datasets
