.. _analysis_objects:

================
Analysis objects
================

An ``Analysis`` object is the sole argument to the ``app`` function in your ``app.py`` module. Its attributes include
every strand that can be possibly added to a ``Twine``, although only the strands specified in your ``twine.py`` file
will not be ``None``. The attributes are:

-   ``input_values``
-   ``input_manifest``
-   ``configuration_values``
-   ``configuration_manifest``
-   ``output_values``
-   ``output_manifest``
-   ``credentials``
-   ``children``
-   ``monitors``

Additionally, all input and configuration attributes are hashed using a
`BLAKE3 hash <https://github.com/BLAKE3-team/BLAKE3>`_ so the inputs and configuration that produced a given output in
your app can always be verified. These hashes exist on the following attributes:

-   ``input_values_hash``
-   ``input_manifest_hash``
-   ``configuration_values_hash``
-   ``configuration_manifest_hash``

If a strand is ``None``, so will its corresponding hash attribute be. The hash of a datafile is the hash of
its file, while the hash of a manifest or dataset is the cumulative hash of the files it refers to.
