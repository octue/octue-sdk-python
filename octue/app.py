import importlib
import logging
import os
import sys


logger = logging.getLogger(__name__)


class AppFrom:
    """A context manager that imports the module "app" from a file named "app.py" in the given directory on entry (by
    making a temporary addition to the system path) and unloads it (by deleting it from `sys.modules`) on exit. It will
    issue a warning if an existing module called "app" is already loaded. Usage example:

    ```python3
    with AppFrom('/path/to/dir') as app:
        Runner().run(app)
    ```

    :param str app_path: path to directory containing module named "app.py".
    :return None:
    """

    def __init__(self, app_path="."):
        self.app_path = os.path.abspath(os.path.normpath(app_path))
        logger.debug("Initialising AppFrom context at app_path %s", self.app_path)
        self.app_module = None

    def __enter__(self):
        # Warn on an app present on the system path
        if "app" in sys.modules.keys():
            logger.warning(
                "Module 'app' already on system path. Using 'AppFrom' context will yield unexpected results. Avoid "
                "using 'app' as a python module, except for your main entrypoint."
            )

        # Insert the present directory first on the system path.
        sys.path.insert(0, self.app_path)

        # Import the app from the present directory.
        self.app_module = importlib.import_module("app")

        # Immediately clean up the entry to the system path (don't use "remove" because if the user has it in their
        # path, this'll be an unexpected side effect, and don't do it in cleanup in case the called code inserts a path)
        sys.path.pop(0)
        logger.debug("Imported app at app_path and cleaned up temporary modification to sys.path %s", self.app_path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Unload the imported module.

        :return None:
        """
        try:
            del sys.modules["app"]
            logger.debug("Deleted app from sys.modules")

        except KeyError:
            context_manager_name = type(self).__name__
            logger.warning(
                f"Module 'app' was already removed from the system path prior to exiting the {context_manager_name} "
                f"context manager. Using the {context_manager_name} context may yield unexpected results."
            )
