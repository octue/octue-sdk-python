# Twined services

There's a growing range of live services in the
Twined ecosystem that you can query, mostly related to wind energy and
other renewables.

## Service names

Questions are always asked to a _revision_ of a service. Services
revisions are named in a similar way to docker images. They look like
`namespace/name:tag` where the tag is often a semantic version (but
doesn't have to be).

!!! info "Definitions"

    **Service revision**

    A specific instance of a Twined service that can be individually
    addressed. The revision could correspond to a version of the
    service, a dynamic development branch for it, or a deliberate
    duplication or variation of it.

    **Service revision unique identifier (SRUID)**

    The combination of a service revision's namespace, name, and
    revision tag that uniquely identifies it. For example,
    `octue/my-service:1.3.0` where the namespace is `octue`, the name is
    `my-service`, and the revision tag is `1.3.0`.

    **Service namespace**

    The group to which the service belongs e.g. your name or your
    organisation's name. If in doubt, use the GitHub handle of the user
    or organisation publishing the services.

    Namespaces must be lower kebab case (i.e. they may contain the
    letters \[a-z\], numbers \[0-9\], and hyphens \[-\]). They may not
    begin or end with hyphens.

    **Service name**

    A name to uniquely identify the service within its namespace. This
    usually corresponds to the name of the GitHub repository for the
    service. Names must be lower kebab case (i.e. they may contain the
    letters \[a-z\], numbers \[0-9\] and hyphens \[-\]). They may not
    begin or end with hyphens.

    **Service revision tag**

    A tag that uniquely identifies a particular revision of a service.
    The revision tag could be a:

    - Commit hash (e.g. `a3eb45`)
    - Semantic version (e.g. `0.12.4`)
    - Branch name (e.g. `development`)
    - Particular environment the service is deployed in (e.g.
      `production`)
    - Combination of these (e.g. `0.12.4-production`)

    Tags may contain lowercase and uppercase letters, numbers,
    underscores, periods, and hyphens, but can't start with a period or
    a dash. They can contain a maximum of 128 characters. These
    requirements are the same as the [Docker tag
    format](https://docs.docker.com/engine/reference/commandline/tag/).

    **Service ID**

    The SRUID is a special case of a service ID. A service ID can be an
    SRUID or just the service namespace and name. It can be used to ask
    a question to a service without specifying a specific revision of
    it. This enables asking questions to, for example, the service
    `octue/my-service` and automatically having them routed to its
    default (usually latest) revision.
    [See here for more info](../asking_questions/#asking-a-question)

## Service communication standard

Twined services communicate according to the service communication
standard. The JSON schema defining this can be found
[here](https://strands.octue.com/octue/service-communication). Messages
received by services are validated against it and invalid messages are
rejected. The schema is in beta, so (rare) breaking changes are
reflected in the minor version number.
