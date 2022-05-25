def run(analysis, *args, **kwargs):
    """Run a mock analysis.

    :param analysis:
    :return None:
    """
    analysis.send_monitor_message({"status": "hello"})
    analysis.output_values = [1, 2, 3, 4]
