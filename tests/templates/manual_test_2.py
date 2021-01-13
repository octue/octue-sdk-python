import logging
import os

from octue import Runner


# from octue.resources.service import Service


PARENT_APP_PATH = "/Users/Marcus1/repos/octue-sdk-python/octue/templates/template-child-services/parent_app"
TEMPLATE_TWINE_PATH = os.path.join(PARENT_APP_PATH, "twine.json")


# service = Service(name="hello", id=0, uri="http://0.0.0.0:8080")
# service.connect()
# service.client.emit('question', {}, callback=print)


runner = Runner(twine=TEMPLATE_TWINE_PATH, log_level=logging.DEBUG, show_twined_logs=False)

analysis = runner.run(
    app_src=PARENT_APP_PATH,
    children=os.path.join(PARENT_APP_PATH, "data", "configuration", "children.json"),
    input_values=os.path.join(PARENT_APP_PATH, "data", "input", "values.json"),
)

pass
