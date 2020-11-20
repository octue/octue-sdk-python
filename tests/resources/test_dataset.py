from octue import exceptions
from octue.resources import Datafile, Dataset
from ..base import BaseTestCase


class DatafileTestCase(BaseTestCase):
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

    def test_append_single_file_to_empty_dataset(self):
        """ Ensures that when a dataset is empty, it can be appended to
        """
        resource = Dataset()
        resource.append(Datafile(path="path-within-dataset/a_test_file.csv"))
        self.assertEqual(len(resource.files), 1)

    def test_append_single_file_to_existing_dataset(self):
        """ Ensures that when a dataset is not empty, it can be appended to
        """
        files = [Datafile(path="path-within-dataset/a_test_file.csv")]
        resource = Dataset(files=files, tags="one two")
        resource.append(Datafile(path="path-within-dataset/a_test_file.csv"))
        self.assertEqual(len(resource.files), 2)

    def test_append_with_datafile_creation_shortcut(self):
        """ Ensures that when a dataset is not empty, it can be appended to
        """
        resource = Dataset()
        resource.append(path="path-within-dataset/a_test_file.csv")
        self.assertEqual(len(resource.files), 1)

    def test_append_multiple_files(self):
        """ Ensures that when a dataset is not empty, it can be appended to
        """
        files = [
            Datafile(path="path-within-dataset/a_test_file.csv"),
            Datafile(path="path-within-dataset/a_test_file.csv"),
        ]
        resource = Dataset()
        resource.append(*files)
        self.assertEqual(len(resource.files), 2)

    def test_cannot_append_non_datafiles(self):
        """ Ensures that exception will be raised if appending a non-datafile object
        """

        class NotADatafile:
            pass

        resource = Dataset()
        with self.assertRaises(exceptions.InvalidInputException) as e:
            resource.append(NotADatafile())

        self.assertIn("must be of class Datafile to append it to a Dataset", e.exception.args[0])

    def test_get_files_catches_single_underscore_mistake(self):
        """ Ensures that if the field name is a single underscore, that gets caught as an error
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/A_Test_file.csv"),
                Datafile(path="path-within-dataset/a_test_file.txt"),
            ]
        )
        with self.assertRaises(exceptions.InvalidInputException) as e:
            resource.get_files("name_icontains", filter_value="Test")

        self.assertIn("Field lookups should be in the form '<field_name>__'<filter_kind>", e.exception.args[0])

    def test_get_files_name_contains(self):
        """ Ensures that get_files works with the name_contains and name_icontains lookups
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/A_Test_file.csv"),
                Datafile(path="path-within-dataset/a_test_file.txt"),
            ]
        )
        files = resource.get_files("name__icontains", filter_value="Test")
        self.assertEqual(2, len(files))
        files = resource.get_files("name__icontains", filter_value="A")
        self.assertEqual(2, len(files))
        files = resource.get_files("name__contains", filter_value="Test")
        self.assertEqual(1, len(files))
        files = resource.get_files("name__icontains", filter_value="test")
        self.assertEqual(2, len(files))
        files = resource.get_files("name__icontains", filter_value="file")
        self.assertEqual(2, len(files))

    def test_get_files_name_with(self):
        """ Ensures that get_files works with the name_endswith and name_startswith lookups
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/a_my_file.csv"),
                Datafile(path="path-within-dataset/a_your_file.csv"),
            ]
        )
        files = resource.get_files("name__startswith", filter_value="a_my")
        self.assertEqual(1, len(files))
        files = resource.get_files("name__startswith", filter_value="a_your")
        self.assertEqual(1, len(files))
        files = resource.get_files("name__startswith", filter_value="a_")
        self.assertEqual(2, len(files))
        files = resource.get_files("name__startswith", filter_value="b")
        self.assertEqual(0, len(files))
        files = resource.get_files("name__endswith", filter_value="_file.csv")
        self.assertEqual(2, len(files))
        files = resource.get_files("name__endswith", filter_value="r_file.csv")
        self.assertEqual(1, len(files))
        files = resource.get_files("name__endswith", filter_value="y_file.csv")
        self.assertEqual(1, len(files))
        files = resource.get_files("name__endswith", filter_value="other.csv")
        self.assertEqual(0, len(files))

    def test_get_files_by_tag(self):
        """ Ensures that get_files works with tag lookups
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/a_my_file.csv", tags="one a:2 b:3 all"),
                Datafile(path="path-within-dataset/a_your_file.csv", tags="two a:2 b:3 all"),
                Datafile(path="path-within-dataset/a_your_file.csv", tags="three all"),
            ]
        )
        files = resource.get_files("tag__exact", filter_value="a")
        self.assertEqual(0, len(files))
        files = resource.get_files("tag__exact", filter_value="one")
        self.assertEqual(1, len(files))
        files = resource.get_files("tag__exact", filter_value="all")
        self.assertEqual(3, len(files))
        files = resource.get_files("tag__startswith", filter_value="b")
        self.assertEqual(2, len(files))
        files = resource.get_files("tag__endswith", filter_value="3")
        self.assertEqual(2, len(files))
        files = resource.get_files("tag__contains", filter_value="hre")
        self.assertEqual(1, len(files))

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

    def test_get_files_by_sequence_notnone(self):
        """ Ensures that get_files works with sequence lookups
        """
        resource = Dataset(
            files=[
                Datafile(path="path-within-dataset/a_my_file.csv", sequence=0),
                Datafile(path="path-within-dataset/a_your_file.csv", sequence=1),
                Datafile(path="path-within-dataset/a_your_file.csv", sequence=None),
            ]
        )
        files = resource.get_files("sequence__notnone")
        self.assertEqual(2, len(files))

    def test_get_file_sequence(self):
        """ Ensures that get_files works with sequence lookups
        """
        files = [
            Datafile(path="path-within-dataset/a_my_file.csv", sequence=0),
            Datafile(path="path-within-dataset/a_your_file.csv", sequence=1),
            Datafile(path="path-within-dataset/a_your_file.csv", sequence=None),
        ]

        got_files = Dataset(files=files).get_file_sequence("name__endswith", filter_value=".csv", strict=True)
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
            resource.get_file_sequence("name__endswith", filter_value=".csv", strict=True)

    def test_get_files_name_filters_include_extension(self):
        """ Ensures that filters applied to the name will catch terms in the extension
        """
        files = [
            Datafile(path="path-within-dataset/a_test_file.csv"),
            Datafile(path="path-within-dataset/a_test_file.txt"),
        ]

        self.assertEqual(Dataset(files=files).get_files("name__icontains", filter_value="txt"), [files[1]])

    def test_get_files_name_filters_exclude_path(self):
        """ Ensures that filters applied to the name will not catch terms in the extension
        """
        resource = Dataset(
            files=[
                Datafile(path="first-path-within-dataset/a_test_file.csv"),
                Datafile(path="second-path-within-dataset/a_test_file.txt"),
            ]
        )
        files = resource.get_files("name__icontains", filter_value="second")
        self.assertEqual(0, len(files))
