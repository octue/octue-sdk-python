.. _manifest:

========
Manifest
========
A manifest is a group of datasets needed for an analysis. The datasets can be from one location in cloud storage or
many, and can even include local datasets if you can guarantee all services that need them can access them (on,
for example, a supercomputer cluster running several ``octue`` services, it will be much faster to access the required
datasets locally than upload them to the cloud and then download them again for each service).

.. code-block:: python
    manifest =
