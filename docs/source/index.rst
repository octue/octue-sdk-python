==================
Octue SDK (Python)
==================

The python SDK for `Octue <https://octue.com>`_ data services, digital twins, and applications - get faster data
groundwork so you have more time for the science!

.. _service_definition:

.. admonition:: Definition

    Octue service
        An Octue data service, digital twin, or application that can be asked questions, process them, and send answers.
        Octue services can communicate with each other with no extra setup.


Key features
============

**Unified cloud/local file, dataset, and manifest operations**

- Create and build datasets easily
- Organise them with timestamps, labels, and tags
- Filter and combine them using this metadata
- Store them locally or in the cloud (or both for low-latency reading/writing with cloud-guaranteed data availability)
- Use internet/cloud-based datasets as if they were local e.g.

  - ``https://example.com/important_dataset.dat``
  - ``gs://example-bucket/important_dataset.dat``

- Create manifests (a set of datasets needed for a particular analysis) to modularise your dataset input/output

**Ask existing services questions from anywhere**

- Send them data to process from anywhere
- Automatically have their logs, monitor messages, and any errors forwarded to you and displayed as if they were local
- Receive their output data as JSON
- Receive a manifest of any output datasets they produce for you to download or access as you wish

**Create, run, and deploy your apps as services**

- No need to change your app - just wrap it
- Use the ``octue`` CLI to run your service locally or deploy it to Google Cloud Run or Google Dataflow
- Create JSON-schema interfaces to explicitly define the form of configuration, input, and output data
- Ask other services questions as part of your app (i.e. build trees of services)
- Automatically display readable, colourised logs, or use your own log handler
- Avoid time-consuming and confusing devops, cloud configuration, and backend maintenance

**High standards, quick responses, and good intentions**

- Open-source and transparent on GitHub - anyone can see the code and raise an issue
- Automated testing, standards, releases, and deployment
- High test coverage
- Works on MacOS, Linux, and Windows
- Developed not-for-profit for the renewable energy industry


Need help, found a bug, or want to request a new feature?
=========================================================
We use `GitHub Issues <https://github.com/octue/octue-sdk-python/issues>`_ [#]_ to manage:

- Bug reports
- Feature requests
- Support requests


.. rubric:: Footnotes

.. [#] Bug reports, feature requests and support requests, may also be made directly to your Octue support contact, or
   via the `support pages <https://www.octue.com/contact>`_.


..
   The table of contents tree is hidden in the page content but will show up as a navigation bar for all pages.

.. toctree::
   :maxdepth: 1
   :hidden:

   installation
   data_containers
   datafile
   dataset
   manifest
   asking_questions
   creating_services
   logging
   child_services
   deploying_services_advanced
   api
   license
   version_history
   bibliography
