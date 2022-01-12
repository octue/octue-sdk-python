"""A module containing helper functions for sending monitor messages that conform to the Octue essential monitor
message schema https://refs.schema.octue.com/octue/essential-monitors/0.0.2.json
"""
import logging
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


def send_status_text(analysis, text, service_name):
    """Send a status-type monitor message and additionally log it to the info level.

    :param octue.resources.analysis.Analysis analysis: the analysis from which to send the status text
    :param str text: the text of the status message
    :param str service_name: the name of the service/child running the analysis
    :return None:
    """
    analysis.send_monitor_message(
        {"service": service_name, "status_text": text, "date_time": datetime.now(timezone.utc).isoformat()}
    )
    logger.info(text)


def send_estimated_seconds_remaining(analysis, estimated_seconds_remaining, service_name):
    """Send an estimated-seconds-remaining monitor message.

    :param octue.resources.analysis.Analysis analysis: the analysis from which to send the estimate
    :param float estimated_seconds_remaining:
    :param str service_name: the name of the service/child running the analysis
    """
    analysis.send_monitor_message(
        {
            "service": service_name,
            "estimated_seconds_remaining": estimated_seconds_remaining,
            "date_time": datetime.now(timezone.utc).isoformat(),
        }
    )
