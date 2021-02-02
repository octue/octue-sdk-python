import os
from shutil import copytree
from pkg_resources import resource_filename, resource_listdir

from octue import exceptions


# TODO add ONLINE_TEMPLATES and combine to AVAILABLE_TEMPLATES, to enable us to seamlessly deliver templates that are
#  more complex (or have lots of data that we don't want packaged with the module).

PACKAGED_TEMPLATES = tuple(name for name in resource_listdir("octue", "templates") if name.startswith("template-"))


def copy_template(template_name, destination_dir="."):
    """Copies one of the application templates from the octue/templates to a destination directory (current dir by default)

    :parameter template_name: The name of the template to copy, must be one of AVAILABLE_TEMPLATES.
    :type template_name: str

    :parameter destination_dir: The destination directory to copy to, default "." (current directory)
    :type destination_dir: path-like
    """

    if template_name not in PACKAGED_TEMPLATES:
        raise exceptions.InvalidInputException(
            f"Unknown template name '{template_name}', try one of {PACKAGED_TEMPLATES}"
        )

    resource = resource_filename("octue.templates", template_name)
    dest = os.path.join(destination_dir, template_name)
    copytree(resource, dest)
