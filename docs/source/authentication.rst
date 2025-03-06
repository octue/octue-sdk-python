==============
Authentication
==============
You need authentication while using ``octue`` to:

- Access data from Google Cloud Storage
- Use, run, or deploy Twined services

Authentication can be provided by using one of:

- A service account
- Application Default Credentials

Creating a service account
==========================
1. Create a service account (see Google's `getting started guide <https://cloud.google.com/docs/authentication/getting-started>`__)
2. Make sure your service account has access to any buckets you need, Google Pub/Sub, and Google Cloud Run if your
   service is deployed on it (`see here <https://cloud.google.com/storage/docs/access-control/using-iam-permissions>`_)

Using a service account
=======================

Locally
-------
1. Create and download a key for your service account - it will be called ``your-project-XXXXX.json``.

.. DANGER::

    It's best not to store this in your project to prevent accidentally committing it or building it into a docker
    image layer. Instead, bind mount it into your docker image from somewhere else on your local system.

    If you must keep within your project, it's good practice to name the file ``gha-greds-<whatever>.json`` and make
    sure that ``gha-creds-*`` is in your ``.gitignore`` and ``.dockerignore`` files.

2. If you're developing in a container (like a VSCode ``.devcontainer``), mount the file into the container. You can
   make gcloud available too - check out `this tutorial
   <https://medium.com/datamindedbe/application-default-credentials-477879e31cb5>`_.

3. Set the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable to the path of the key file.

On GCP infrastructure
---------------------
- Credentials are provided when running code on GCP infrastructure (e.g. Google Cloud Run)
- ``octue`` uses these when when running on these platforms
- You should ensure the correct service account is being used by the deployed instance
