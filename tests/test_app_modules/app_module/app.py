CUSTOM_APP_RUN_MESSAGE = "This is a custom app run function"


def run(analysis, *args, **kwargs):
    """Run a mock analysis that simply prints a message.

    :param analysis:
    :return None:
    """
    print(CUSTOM_APP_RUN_MESSAGE)  # noqa:T001
    analysis.output_values = "App as a module works!"
