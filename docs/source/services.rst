.. _services:

==============
Octue services
==============

There's a growing range of live :ref:`services <service_definition>` in the Octue ecosystem that you can ask questions
to and get answers from. Currently, all of them are related to wind energy. Here's a quick glossary of terms before we
tell you more:

.. admonition:: Definitions

    Octue service
        See :ref:`here <service_definition>`.

    Child
        An Octue service that can be asked a question. This name reflects the tree structure of services (specifically,
        `a DAG <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_) formed by the service asking the question (the
        parent), the child it asks the question to, any children that the child asks questions to as part of forming
        its answer, and so on.

    Parent
        An Octue service that asks a question to another Octue service (a child).

    Asking a question
        Sending data (input values and/or an input manifest) to a child for processing/analysis.

    Receiving an answer
       Receiving data (output values and/or an output manifest) from a child you asked a question to.

    Octue ecosystem
       The set of services running the Octue SDK as their backend. These services guarantee:

       - Defined input/output JSON schemas and validation
       - An easy and consistent interface for asking them questions and receiving their answers
       - Logs, exceptions, and monitor messages forwarded to you
       - High availability (if deployed in the cloud)


.. _service_naming:

Service names
=============

Questions are always asked to a *revision* of a service. Services revisions are named in a similar way to docker images.
They look like ``namespace/name:tag`` where the tag is often a semantic version (but doesn't have to be).

.. admonition:: Definitions

    Service revision
        A specific instance of an Octue service that can be individually addressed. The revision could correspond to a
        version of the service, a dynamic development branch for it, or a deliberate duplication or variation of it.

    .. _sruid_definition:

    Service revision unique identifier (SRUID)
        The combination of a service revision's namespace, name, and revision tag that uniquely identifies it. For
        example, ``octue/my-service:1.3.0`` where the namespace is ``octue``, the name is ``my-service``, and the
        revision tag is ``1.3.0``.

    Service namespace
        The group to which the service belongs e.g. your name or your organisation's name. If in doubt, use the GitHub
        handle of the user or organisation publishing the services.

        Namespaces must be lower kebab case (i.e. they may contain the letters [a-z], numbers [0-9], and hyphens [-]).
        They may not begin or end with hyphens.

    Service name
        A name to uniquely identify the service within its namespace. This usually corresponds to the name of the GitHub
        repository for the service. Names must be lower kebab case (i.e. they may contain the letters [a-z], numbers
        [0-9] and hyphens [-]). They may not begin or end with hyphens.

    Service revision tag
        A tag that uniquely identifies a particular revision of a service. The revision tag could be a:

        - Commit hash (e.g. ``a3eb45``)
        - Semantic version (e.g. ``0.12.4``)
        - Branch name (e.g. ``development``)
        - Particular environment the service is deployed in (e.g. ``production``)
        - Combination of these (e.g. ``0.12.4-production``)

        Tags may contain lowercase and uppercase letters, numbers, underscores, periods, and hyphens, but can't start
        with a period or a dash. They can contain a maximum of 128 characters. These requirements are the same as the
        `Docker tag format <https://docs.docker.com/engine/reference/commandline/tag/>`_.

    Service ID
        The SRUID is a special case of a service ID. A service ID can be an SRUID or just the service namespace and
        name. It can be used to ask a question to a service without specifying a specific revision of it. This enables
        asking questions to, for example, the service ``octue/my-service`` and automatically having them routed to its
        default (usually latest) revision. :ref:`See here for more info<using_default_revision_tag>`.
