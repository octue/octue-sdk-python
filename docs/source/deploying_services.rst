.. _deploying_services:

================================
Deploying services automatically
================================

Automated deployment with Octue means:

* Your service runs in Google Cloud, ready to accept questions from and return answers to other services.
* You don't need to do anything to update your deployed service with new code changes - the service simply gets rebuilt
  and re-deployed each time you push a commit to your ``main`` branch, or merge a pull request into it (other branches
  and deployment strategies are available, but this is the default).
* Your service only runs when questions from other services are sent to it, meaning there is no cost to having it
  deployed but not in use (or in low use).

All you need to enable automated deployments are the following files in your repository root:

* A ``requirements.txt`` file that includes ``octue>=0.1.17`` and the rest of your service's dependencies
* A ``twine.json`` file
* A ``deployment_configuration.json`` file (optional)

Apart from that, all you need to do is contact us so we can connect your git repository as a Google Cloud Build
trigger. This requires no work from you, apart from authorising the connection to GitHub (or another git provider).


-----------------------------
Deployment configuration file
-----------------------------
The ``deployment_configuration.json`` file is not required unless the defaults below do not apply. The file should be a
JSON object with the following fields (all paths should be relative to the repository root):

* ``app_dir`` (default value: ``"."``) - the path of the directory containing your ``app.py``
* ``twine`` (default value: ``"twine.json"``) - the path of your Twine file
* ``configuration_values`` (default value: ``None``)
* ``configuration_manifest`` (default value: ``None``)
* ``output_manifest`` (default value: ``None``)
* ``children`` (default value: ``None``)
* ``skip_checks`` (default value: ``False``)
* ``log_level`` (default value: ``INFO``)
* ``log_handler`` (default value: ``None``)


--------------------
Advanced deployments
--------------------
If your service requires extra system dependencies or other complexities that can't be encapsulated in a
``requirements.txt`` file, you'll also need to write a ``Dockerfile`` of a certain form. We can provide you with a
starting point.
