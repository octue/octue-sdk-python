import os
import shutil
import sys
import uuid

from octue.definitions import DATA_PATH, TEMPLATES_PATH


class Template:
    def __init__(self):
        self.start_path = os.getcwd()
        self.template_twine = None
        self.template_path = None
        self.app_test_path = None
        self.teardown_templates = []

    def set_template(self, template):
        """Set up the working directory and data paths to run one of the provided templates."""
        self.template_path = os.path.join(TEMPLATES_PATH, template)
        self.template_twine = os.path.join(TEMPLATES_PATH, template, "twine.json")

        # Duplicate the template's data/ directory to a test-specific replica
        self.app_test_path = os.path.join(DATA_PATH, str(uuid.uuid4()))
        shutil.copytree(self.template_path, self.app_test_path)

        # Add this template to the list to remove in teardown
        self.teardown_templates.append(self.app_test_path)
        sys.path.insert(0, self.app_test_path)

        # Run from within the app folder context
        os.chdir(self.app_test_path)

    def cleanup(self):
        os.chdir(self.start_path)
        for path in self.teardown_templates:
            sys.path.remove(path)
            shutil.rmtree(path)
