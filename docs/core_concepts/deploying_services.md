# Deploying services (developer's guide)

This is a guide for developers that want to deploy Twined services
themselves - it is not needed if Octue manages your services for you or
if you are only asking questions to existing Twined services.

## What is deployment?

Deploying a Twined service means the service:

- Is a docker image that is spun up and down in a Kubernetes cluster on
  demand
- Is ready at any time to answer questions from users and other Twined
  services in the service network
- Can ask questions to any other Twined service in the service network
- Will automatically spin down after it has finished answering a
  question
- Will automatically build and redeploy after a relevant code change
  (e.g. on push or merge into `main`)

We can split deployment into service deployment and infrastructure
deployment.

## Deploying a service

Assuming the service network infrastructure already exists, a service
can be deployed by building and pushing its docker image to the service
network's Artifact Registry repository. We recommend pushing a new
image for each release of the code e.g. on merge into the `main` branch.
Each new image is the deployment of a new service revision. This can be
done automatically:

- Follow the
  [instructions](https://github.com/octue/workflows#deploying-a-kuberneteskueue-octue-twined-service-revision)
  to add the
  [build-twined-service](https://github.com/octue/workflows/blob/main/.github/workflows/build-twined-service.yml)
  GitHub Actions workflow to your service's GitHub repository. Set its
  trigger to merge or push to `main` (see example below)
- This needs to be done **once for every service** you want to deploy
- A live example can be [found
  here](https://github.com/octue/example-service-kueue/blob/main/.github/workflows/release.yml)
  including automated pre-deployment testing and creation of a GitHub
  release

You can now [ask your service some questions](asking_questions.md)! It will be available in the service network as
`<namespace>/<name>:<version>` (e.g. `octue/example-service-kueue:0.1.1`).

## Deploying the infrastructure

### Prerequisites

Twined services are currently deployable to Google Cloud Platform (GCP).
You must have "owner" level access to the GCP project you're
deploying to and billing must be set up for it.

### Deploying step-by-step

There are two steps to deploying the infrastructure:

1.  Deploy the core infrastructure (storage bucket, event store, IAM
    service accounts and roles)
2.  Deploy the Kubernetes cluster, event handler, service registry, and
    Pub/Sub topic

#### 1. Deploy core infrastructure

- Follow [the
  instructions](https://github.com/octue/terraform-octue-twined-core) to
  deploy the resources in the `terraform-octue-twined-core` Terraform
  module
- This only needs to be done once per service network

#### 2. Deploy Kubernetes cluster

- Follow the
  [instructions](https://github.com/octue/terraform-octue-twined-cluster)
  to deploy the resources in the `terraform-octue-twined-cluster`
  Terraform module
- This only needs to be done once per service network
