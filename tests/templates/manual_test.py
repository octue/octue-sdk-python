import logging
import os
from unittest.mock import patch

from octue import Runner


TEMPLATE_PATH = "/Users/Marcus1/repos/octue-sdk-python/octue/templates/template-child-services/parent_app"
TEMPLATE_TWINE_PATH = os.path.join(TEMPLATE_PATH, "twine.json")


runner = Runner(twine=TEMPLATE_TWINE_PATH, log_level=logging.DEBUG, show_twined_logs=True)

with patch("octue.resources.Service.ask") as mock_service_ask:
    mock_service_ask.return_value = [0, 7]

    analysis = runner.run(
        app_src=TEMPLATE_PATH,
        children=os.path.join(TEMPLATE_PATH, "data", "configuration", "children.json"),
        input_values=os.path.join(TEMPLATE_PATH, "data", "input", "values.json"),
    )

assert analysis.output_values == {
    "wind_speeds": mock_service_ask.return_value,
    "elevations": mock_service_ask.return_value,
}

exit()
