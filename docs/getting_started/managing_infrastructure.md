# Getting started - managing infrastructure

Allow your team to run services in your own private cloud, with just two pre-configured Terraform modules. Control
costs, limit access and integrate with your other systems.

Twined data services can be run locally, but deploying them in the cloud is generally preferable for reliability and
performance. Twined makes it easy to do this with two [Terraform](https://terraform.io) modules (ready-made
infrastructure as code / IaC) that deploy a managed Kubernetes cluster to run services on. The combined infrastructure
is called a **Twined services network**.

This guide walks you through setting up a Twined services network - the infrastructure needed to deploy a Twined
service. It only takes a few commands.

## Prerequisites

Before you begin, ensure you:

<!-- prettier-ignore-start -->

- Are familiar with Terraform/IaC and the command line
- Have the following set up:
    - [Terraform CLI](https://developer.hashicorp.com/terraform/install) >= 1.8.0 installed
    - A Google Cloud account and project with billing set up
    - Optionally, a [Terraform Cloud](https://app.terraform.io/public/signup/account?product_intent=terraform) account

<!-- prettier-ignore-end -->

## Spin up infrastructure

Follow the instructions [here](https://github.com/octue/terraform-octue-twined-core), making sure to follow up with the
[second step](https://github.com/octue/terraform-octue-twined-cluster) mentioned in the "Important" box at the top.

## Next steps

Congratulations on setting up a Twined services network! Next up, [create your first data service](../getting_started/creating_services.md).
