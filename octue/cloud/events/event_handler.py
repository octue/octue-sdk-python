import logging
import os
import re
from datetime import datetime

from octue.cloud import EXCEPTIONS_MAPPING
from octue.cloud.validation import SERVICE_COMMUNICATION_SCHEMA
from octue.definitions import GOOGLE_COMPUTE_PROVIDERS
from octue.log_handlers import COLOUR_PALETTE
from octue.resources.manifest import Manifest


logger = logging.getLogger(__name__)


if os.environ.get("COMPUTE_PROVIDER", "UNKNOWN") in GOOGLE_COMPUTE_PROVIDERS:
    # Google Cloud logs don't support colour currently - provide a no-operation function.
    colourise = lambda string, text_colour=None, background_colour=None: string
else:
    from octue.utils.colour import colourise


class EventHandler:
    question_uuid: str

    def __init__(
        self,
        receiving_service,
        handle_monitor_message=None,
        record_messages=True,
        service_name="REMOTE",
        message_handlers=None,
        schema=SERVICE_COMMUNICATION_SCHEMA,
    ):
        self.receiving_service = receiving_service
        self.handle_monitor_message = handle_monitor_message
        self.record_messages = record_messages
        self.service_name = service_name
        self.schema = schema

        self.handled_messages = []
        self._previous_message_number = -1

        self._message_handlers = message_handlers or {
            "delivery_acknowledgement": self._handle_delivery_acknowledgement,
            "heartbeat": self._handle_heartbeat,
            "monitor_message": self._handle_monitor_message,
            "log_record": self._handle_log_message,
            "exception": self._handle_exception,
            "result": self._handle_result,
        }

        self._log_message_colours = [COLOUR_PALETTE[1], *COLOUR_PALETTE[3:]]

    def _handle_message(self, message):
        """Pass a message to its handler and update the previous message number.

        :param dict message:
        :return dict|None:
        """
        self._previous_message_number += 1

        if self.record_messages:
            self.handled_messages.append(message)

        handler = self._message_handlers[message["kind"]]
        return handler(message)

    def _handle_delivery_acknowledgement(self, message):
        """Mark the question as delivered to prevent resending it.

        :param dict message:
        :return None:
        """
        logger.info("%r's question was delivered at %s.", self.receiving_service, message["datetime"])

    def _handle_heartbeat(self, message):
        """Record the time the heartbeat was received.

        :param dict message:
        :return None:
        """
        self._last_heartbeat = datetime.now()
        logger.info("Heartbeat received from service %r for question %r.", self.service_name, self.question_uuid)

    def _handle_monitor_message(self, message):
        """Send a monitor message to the handler if one has been provided.

        :param dict message:
        :return None:
        """
        logger.debug("%r received a monitor message.", self.receiving_service)

        if self.handle_monitor_message is not None:
            self.handle_monitor_message(message["data"])

    def _handle_log_message(self, message):
        """Deserialise the message into a log record and pass it to the local log handlers, adding [<service-name>] to
        the start of the log message.

        :param dict message:
        :return None:
        """
        record = logging.makeLogRecord(message["log_record"])

        # Add information about the immediate child sending the message and colour it with the first colour in the
        # colour palette.
        immediate_child_analysis_section = colourise(
            f"[{self.service_name} | analysis-{self.question_uuid}]",
            text_colour=self._log_message_colours[0],
        )

        # Colour any analysis sections from children of the immediate child with the rest of the colour palette.
        subchild_analysis_sections = [section.strip("[") for section in re.split("] ", record.msg)]
        final_message = subchild_analysis_sections.pop(-1)

        for i in range(len(subchild_analysis_sections)):
            subchild_analysis_sections[i] = colourise(
                "[" + subchild_analysis_sections[i] + "]",
                text_colour=self._log_message_colours[1:][i % len(self._log_message_colours[1:])],
            )

        record.msg = " ".join([immediate_child_analysis_section, *subchild_analysis_sections, final_message])
        logger.handle(record)

    def _handle_exception(self, message):
        """Raise the exception from the responding service that is serialised in `data`.

        :param dict message:
        :raise Exception:
        :return None:
        """
        exception_message = "\n\n".join(
            (
                message["exception_message"],
                f"The following traceback was captured from the remote service {self.service_name!r}:",
                "".join(message["exception_traceback"]),
            )
        )

        try:
            exception_type = EXCEPTIONS_MAPPING[message["exception_type"]]

        # Allow unknown exception types to still be raised.
        except KeyError:
            exception_type = type(message["exception_type"], (Exception,), {})

        raise exception_type(exception_message)

    def _handle_result(self, message):
        """Convert the result to the correct form, deserialising the output manifest if it is present in the message.

        :param dict message:
        :return dict:
        """
        logger.info("%r received an answer to question %r.", self.receiving_service, self.question_uuid)

        if message.get("output_manifest"):
            output_manifest = Manifest.deserialise(message["output_manifest"])
        else:
            output_manifest = None

        return {"output_values": message.get("output_values"), "output_manifest": output_manifest}
