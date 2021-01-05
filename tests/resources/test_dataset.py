import copy
import warnings
from tests.base import BaseTestCase

from octue import exceptions
from octue.resources import Datafile, Dataset
from octue.resources.filter_containers import FilterSet


class DatasetTestCase(BaseTestCase):
    def test_instantiates_with_no_args(self):
        """ Ensures a Datafile instantiates using only a path and generates a uuid ID
        """
        Dataset()

    def test_instantiates_with_kwargs(self):
        """ Ensures that keyword arguments can be used to construct the dataset initially
        """
        files = [Datafile(path="path-within-dataset/a_test_file.csv")]
        resource = Dataset(files=files, tags="one two")
        self.assertEqual(len(resource.files), 1)

    def test_len(self):
        """ Test that the length of a Dataset is the number of files it contains. """
        dataset = self.create_valid_dataset()
        self.assertEqual(len(dataset), len(dataset.files))

    def test_iter(self):
        """ Test that iterating over a Dataset is equivalent to iterating over its files. """
        dataset = self.create_valid_dataset()
        iterated_files = {file for file in dataset}
        self.assertEqual(iterated_files, dataset.files)

    def test_using_append_raises_deprecation_warning(self):
        """ Test that Dataset.append is deprecated but gets redirected to Dataset.add. """
        resource = Dataset()

        with warnings.catch_warnings(record=True) as warning:
            resource.append(Datafile(path="path-within-dataset/a_test_file.csv"))
            self.assertEqual(len(warning), 1)
            self.assertTrue(issubclass(warning[-1].category, DeprecationWarning))
            self.assertIn("deprecated", str(warning[-1].message))
            self.assertEqual(len(resource.files), 1)

    def test_add_single_file_to_empty_dataset(self):
        """ Ensures that when a dataset is empty, it can be added to
        """
        resource = Dataset()
        resource.add(Datafile(path="path-within-dataset/a_test_file.csv"))
        self.assertEqual(len(resource.files), 1)

    def test_add_single_file_to_existing_dataset(self):
        """ Ensures that when a dataset is not empty, it can be added to
        """
        files = [Datafile(path="path-within-dataset/a_test_file.csv")]
        resource = Dataset(files=files, tags="one two")
        resource.add(Datafile(path="path-within-dataset/a_test_file.csv"))
        self.assertEqual(len(resource.files), 2)

    def test_add_with_datafile_creation_shortcut(self):
        """ Ensures that when a dataset is not empty, it can be added to
        """
        resource = Dataset()
        resource.add(path="path-within-dataset/a_test_file.csv")
        self.assertEqual(len(resource.files), 1)

    def test_add_multiple_files(self):
        """ Ensures that when a dataset is not empty, it can be added to
        """
        files = [
            Datafile(path="path-within-dataset/a_test_file.csv"),
            Datafile(path="path-within-dataset/a_test_file.csv"),
        ]
        resource = Dataset()
        resource.add(*files)
        self.assertEqual(len(resource.files), 2)

    def test_cannot_add_non_datafiles(self):
        """ Ensures that exception will be raised if adding a non-datafile object
        """

        class NotADatafile:
            pass

        resource = Dataset()
        with self.assertRaises(exceptions.InvalidInputException) as e:
            resource.add(NotADatafile())

        self.assertIn("must be of class Datafile to add it to a Dataset", e.exception.args[0])

    def test_filter_catches_single_underscore_mistake(self):
        """ Ensures that if the field name is a single underscore, that gets caught as an error
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/A_Test_file.csv"),
                Datafile(path="path-within-dataset/a_test_file.txt"),
            ]
        )

        with self.assertRaises(exceptions.InvalidInputException) as e:
            resource.files.filter("name_icontains", filter_value="Test")

        self.assertIn(
            "Invalid filter name 'name_icontains'. Filter names should be in the form "
            "'<attribute_name>__<filter_kind>'.",
            e.exception.args[0],
        )

    def test_filter_name_contains(self):
        """ Ensures that filter works with the name_contains and name_icontains lookups
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/A_Test_file.csv"),
                Datafile(path="path-within-dataset/a_test_file.txt"),
            ]
        )
        files = resource.files.filter("name__icontains", filter_value="Test")
        self.assertEqual(2, len(files))
        files = resource.files.filter("name__icontains", filter_value="A")
        self.assertEqual(2, len(files))
        files = resource.files.filter("name__contains", filter_value="Test")
        self.assertEqual(1, len(files))
        files = resource.files.filter("name__icontains", filter_value="test")
        self.assertEqual(2, len(files))
        files = resource.files.filter("name__icontains", filter_value="file")
        self.assertEqual(2, len(files))

    def test_filter_name_with(self):
        """ Ensures that filter works with the name_endswith and name_startswith lookups
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/a_my_file.csv"),
                Datafile(path="path-within-dataset/a_your_file.csv"),
            ]
        )
        files = resource.files.filter("name__starts_with", filter_value="a_my")
        self.assertEqual(1, len(files))
        files = resource.files.filter("name__starts_with", filter_value="a_your")
        self.assertEqual(1, len(files))
        files = resource.files.filter("name__starts_with", filter_value="a_")
        self.assertEqual(2, len(files))
        files = resource.files.filter("name__starts_with", filter_value="b")
        self.assertEqual(0, len(files))
        files = resource.files.filter("name__ends_with", filter_value="_file.csv")
        self.assertEqual(2, len(files))
        files = resource.files.filter("name__ends_with", filter_value="r_file.csv")
        self.assertEqual(1, len(files))
        files = resource.files.filter("name__ends_with", filter_value="y_file.csv")
        self.assertEqual(1, len(files))
        files = resource.files.filter("name__ends_with", filter_value="other.csv")
        self.assertEqual(0, len(files))

    def test_filter_by_tag(self):
        """ Ensures that filter works with tag lookups
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/a_my_file.csv", tags="one a:2 b:3 all"),
                Datafile(path="path-within-dataset/a_your_file.csv", tags="two a:2 b:3 all"),
                Datafile(path="path-within-dataset/a_your_file.csv", tags="three all"),
            ]
        )

        files = resource.files.filter("tags__contains", filter_value="a")
        self.assertEqual(0, len(files))
        files = resource.files.filter("tags__contains", filter_value="one")
        self.assertEqual(1, len(files))
        files = resource.files.filter("tags__contains", filter_value="all")
        self.assertEqual(3, len(files))
        files = resource.files.filter("tags__any_tag_starts_with", filter_value="b")
        self.assertEqual(2, len(files))
        files = resource.files.filter("tags__any_tag_ends_with", filter_value="3")
        self.assertEqual(2, len(files))
        # files = resource.files.filter("tags__contains", filter_value="hre")
        # self.assertEqual(1, len(files))

    def test_get_file_by_tag(self):
        """ Ensures that get_files works with tag lookups
        """
        files = [
            Datafile(path="path-within-dataset/a_my_file.csv", tags="one a:2 b:3 all"),
            Datafile(path="path-within-dataset/a_your_file.csv", tags="two a:2 b:3 all"),
            Datafile(path="path-within-dataset/a_your_file.csv", tags="three all"),
        ]

        resource = Dataset(files=files)

        # Check working for single result
        self.assertIs(resource.get_file_by_tag("three"), files[2])

        # Check raises for too many results
        with self.assertRaises(exceptions.UnexpectedNumberOfResultsException) as e:
            resource.get_file_by_tag("all")

        self.assertIn("More than one result found", e.exception.args[0])

        # Check raises for no result
        with self.assertRaises(exceptions.UnexpectedNumberOfResultsException) as e:
            resource.get_file_by_tag("billyjeanisnotmylover")

        self.assertIn("No files found with this tag", e.exception.args[0])

    def test_filter_by_sequence_not_none(self):
        """ Ensures that filter works with sequence lookups
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/a_my_file.csv", sequence=0),
                Datafile(path="path-within-dataset/a_your_file.csv", sequence=1),
                Datafile(path="path-within-dataset/a_your_file.csv", sequence=None),
            ]
        )
        files = resource.files.filter("sequence__is_not", None)
        self.assertEqual(2, len(files))

    def test_get_file_sequence(self):
        """ Ensures that get_files works with sequence lookups
        """
        files = [
            Datafile(path="path-within-dataset/a_my_file.csv", sequence=0),
            Datafile(path="path-within-dataset/a_your_file.csv", sequence=1),
            Datafile(path="path-within-dataset/a_your_file.csv", sequence=None),
        ]

        got_files = Dataset(files=files).get_file_sequence("name__ends_with", filter_value=".csv", strict=True)
        self.assertEqual(got_files, files[:2])

    def test_get_broken_file_sequence(self):
        """ Ensures that get_files works with sequence lookups
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/a_my_file.csv", sequence=2),
                Datafile(path="path-within-dataset/a_your_file.csv", sequence=4),
                Datafile(path="path-within-dataset/a_your_file.csv", sequence=None),
            ]
        )
        with self.assertRaises(exceptions.BrokenSequenceException):
            resource.get_file_sequence("name__ends_with", filter_value=".csv", strict=True)

    def test_filter_name_filters_include_extension(self):
        """ Ensures that filters applied to the name will catch terms in the extension
        """
        files = [
            Datafile(path="path-within-dataset/a_test_file.csv"),
            Datafile(path="path-within-dataset/a_test_file.txt"),
        ]

        self.assertEqual(
            Dataset(files=files).files.filter("name__icontains", filter_value="txt"), FilterSet({files[1]})
        )

    def test_filter_name_filters_exclude_path(self):
        """ Ensures that filters applied to the name will not catch terms in the extension
        """
        resource = Dataset(
            files=[
                Datafile(path="first-path-within-dataset/a_test_file.csv"),
                Datafile(path="second-path-within-dataset/a_test_file.txt"),
            ]
        )
        files = resource.files.filter("name__icontains", filter_value="second")
        self.assertEqual(0, len(files))

    def test_using_get_files_raises_deprecation_warning(self):
        """ Test that Dataset.get_files is deprecated but gets redirected to Dataset.files.filter. """
        resource = Dataset(
            files=[
                Datafile(path="first-path-within-dataset/a_test_file.csv"),
                Datafile(path="second-path-within-dataset/a_test_file.txt"),
            ]
        )

        with warnings.catch_warnings(record=True) as warning:
            filtered_files = resource.get_files("name__icontains", filter_value="second")
            self.assertEqual(len(warning), 1)
            self.assertTrue(issubclass(warning[-1].category, DeprecationWarning))
            self.assertIn("deprecated", str(warning[-1].message))
            self.assertEqual(len(filtered_files), 0)

    def test_hash_value(self):
        """ Test hashing a dataset with multiple files gives a hash of length 128. """
        hash_ = self.create_valid_dataset().hash_value
        self.assertTrue(isinstance(hash_, str))
        self.assertTrue(len(hash_) == 64)

    def test_hashes_for_the_same_dataset_are_the_same(self):
        """ Ensure the hashes for two datasets that are exactly the same are the same."""
        first_dataset = self.create_valid_dataset()
        second_dataset = copy.deepcopy(first_dataset)
        self.assertEqual(first_dataset.hash_value, second_dataset.hash_value)
