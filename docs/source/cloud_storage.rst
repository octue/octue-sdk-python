.. _cloud_storage:

=============
Cloud storage
=============

Octue SDK is currently integrated with Google Cloud Storage and the intention is to integrate with other cloud providers
as time goes on.

Data container classes
----------------------

All of the data container classes in the SDK have a ``to_cloud`` and a ``from_cloud`` method, which
completely handles their upload/download to/from the cloud, including all relevant metadata. Data integrity is checked
before and after upload to ensure any data corruption is avoided, while Google ensures the same is true for download.

Files and strings
-----------------
