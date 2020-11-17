import twined
from octue import Runner
from .base import BaseTestCase


class RunnerTestCase(BaseTestCase):
    def test_instantiate_runner(self):
        """ Ensures that runner whose twine requires configuration can be instantiated
        """
        runner = Runner(twine="{}")
        self.assertEqual(runner.__class__.__name__, "Runner")

    def test_run_with_configuration_passes(self):
        """ Ensures that runs can be made with configuration only
        """
        runner = Runner(
            twine="""{
            "configuration_values_schema": {
                "type": "object",
                "properties": {
                    "n_iterations": {
                        "type": "integer"
                    }
                }
            }
        }""",
            configuration_values="{}",
        )

        def fcn(analysis):
            pass

        runner.run(fcn)

    def test_instantiation_without_configuration_fails(self):
        """ Ensures that runner can be instantiated with a string that points to a path
        """
        with self.assertRaises(twined.exceptions.TwineValueException) as error:
            Runner(
                twine="""{
                "configuration_values_schema": {
                    "type": "object",
                    "properties": {
                        "n_iterations": {
                            "type": "integer"
                        }
                    }
                }
            }"""
            )

        self.assertIn(
            "The 'configuration_values' strand is defined in the twine, but no data is provided in sources",
            error.exception.args[0],
        )

    def test_run_output_values_validation(self):
        """ Ensures that runner can be instantiated with a string that points to a path
        """
        runner = Runner(
            twine="""{
            "output_values_schema": {
                "type": "object",
                "required": ["n_iterations"],
                "properties": {
                    "n_iterations": {
                        "type": "integer"
                    }
                }
            }
        }"""
        )

        # Test for failure with an incorrect output
        def fcn(analysis):
            pass

        with self.assertRaises(twined.exceptions.InvalidValuesContents) as error:
            runner.run(fcn)

        self.assertIn("'n_iterations' is a required property", error.exception.args[0])

        # Test for success with a valid output
        def fcn(analysis):
            analysis.output_values["n_iterations"] = 10

        runner.run(fcn)

    # def test_exception_raised_when_extra_strand_data_present(self):
    #     """ Ensures that protected attributes can't be set
    #     """
    #     with self.assertRaises(twined.exceptions.StrandNotFound) as error:
    #         Runner(
    #             twine="{}", configuration_values={},
    #         )
    #
    #     self.assertIn(
    #         "Source data is provided for 'configuration_values' but no such strand is defined in the twine",
    #         error.exception.args[0],
    #     )

    def test_exception_raised_when_strand_data_missing(self):
        """ Ensures that protected attributes can't be set
        """
        runner = Runner(
            twine="""{
                "configuration_values_schema": {
                    "type": "object",
                    "properties": {
                        "n_iterations": {
                            "type": "integer"
                        }
                    }
                },
                "input_values_schema": {
                    "type": "object",
                    "properties": {
                        "height": {
                            "type": "integer"
                        }
                    },
                    "required": ["height"]
                }
            }""",
            configuration_values={"n_iterations": 5},
        )

        def fcn(analysis):
            pass

        with self.assertRaises(twined.exceptions.TwineValueException) as error:
            runner.run(fcn)

        self.assertIn(
            "The 'input_values' strand is defined in the twine, but no data is provided in sources",
            error.exception.args[0],
        )
