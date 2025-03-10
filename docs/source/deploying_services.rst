.. _deploying_services_advanced:

======================================
Deploying services (developer's guide)
======================================
This is a guide for developers that want to deploy Twined services themselves - it is not needed if Octue manages your
services for you or if you are only asking questions to existing Twined services.

What deployment enables
=======================
Deploying a Twined service means the service:

* Is a docker image that is spun up and down in a Kubernetes cluster on demand
* Is ready at any time to answer questions from users and other Twined services in the service network
* Can ask questions to any other Twined service in the service network
* Will automatically spin down after it has finished answering a question
* Will automatically build and redeploy after a relevant code change (e.g. on push or merge into ``main``)

Prerequisites
=============
Twined services are currently deployable to Google Cloud Platform (GCP). You must have "owner" level access to the GCP
project you're deploying to and billing must be set up for it.

Deploying step-by-step
======================
The main part of deployment is deploying the service network infrastructure. Once this is done, services can be easily
added as necessary.

There are three steps to a deployment:

1. Deploy the core infrastructure (e.g. storage bucket, event store, service accounts and roles)
2. Deploy the Kubernetes cluster and partner cloud functions
3. Build and push service docker images to the artifact registry

Deploy core infrastructure
--------------------------

- Deploy the ``terraform-octue-twined-core`` Terraform module
- This only needs to be done once per service network
- Follow the instructions `here <https://github.com/octue/terraform-octue-twined-core>`_

Deploy Kubernetes cluster
-------------------------

- Deploy the ``terraform-octue-twined-cluster`` Terraform module
- This only needs to be done once per service network
- Follow the instructions `here <https://github.com/octue/terraform-octue-twined-cluster>`_

Build and push service docker images
------------------------------------
Your service is available if its docker image is in the service network's artifact registry repository. We recommend
pushing a new image for each merge into the ``main`` branch, corresponding to a new service revision.

- Add the `build-push-twined-service <https://github.com/octue/workflows/blob/main/.github/workflows/build-twined-service.yml>`_
  GitHub Actions workflow to your service's GitHub repository
- This needs to be done for every service you want to deploy
- Follow the instructions `here <https://github.com/octue/workflows#deploying-a-kuberneteskueue-octue-twined-service-revision>`_
- A live example can be `found here <https://github.com/octue/example-service-kueue/blob/main/.github/workflows/release.yml>`_
  including automated pre-deployment testing and release of the code on GitHub

What next?
==========
:doc:`Ask your service some questions <asking_questions>`! It will be available in the service network as
``<namespace>/<name>:<version>`` (e.g. ``octue/example-service-kueue:0.1.1``).
