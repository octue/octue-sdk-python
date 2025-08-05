def run(analysis):
    """Run a mock analysis.

    :param octue.resources.analysis.Analysis analysis:
    :return None:
    """
    analysis.send_monitor_message({"status": "hello"})
    analysis.output_values = {"width": 3}
