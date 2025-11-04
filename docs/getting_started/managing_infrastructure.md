# Getting started - managing infrastructure

Allow your team to run services in your own private cloud, with just two pre-configured Terraform modules. Control
costs, limit access and integrate with your other systems.

Twined data services can be run locally, but deploying them in the cloud is generally preferable for reliability and
performance. Twined makes it easy to do this with two [Terraform](https://terraform.io) modules (ready-made
infrastructure as code / IaC) that deploy a managed Kubernetes cluster to run services on. The combined infrastructure
is called a **Twined service network**.

This guide walks you through setting up a Twined service network.

## Prerequisites

Before you begin, ensure you:

<!-- prettier-ignore-start -->

- Are familiar with Terraform/IaC and the command line
- Have the following set up:
    - [Terraform CLI](https://developer.hashicorp.com/terraform/install) >= 1.8.0 installed
    - A Google Cloud account and project with billing set up
    - Optionally, a [Terraform Cloud](https://app.terraform.io/public/signup/account?product_intent=terraform) account

<!-- prettier-ignore-end -->

## Enable the Cloud Resource Manager API

The Cloud Resource Manager API must be enabled manually for Terraform to work. Enable it by going
[here](https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com) and clicking "Enable API".
Make sure you've enabled it for the correct Google Cloud project

## Create and clone a GitHub repository

Create a GitHub repository for the infrastructure. Clone this repository to your computer and checkout a new branch
called `add-twined-infrastructure`. Replace `<handle>` with your GitHub account handle.

```shell
git clone https://github.com/<handle>/twined-infrastructure.git
cd twined-infrastructure
git checkout -b add-twined-infrastructure
```

## Create the configuration directories

Create two directories at the top level of the repository:

- `terraform_core`
- `terraform_cluster`

## Authenticate with Google Cloud

1. Access your service accounts [here](https://console.cloud.google.com/iam-admin/serviceaccounts), making sure the
   correct project is selected
2. Create a service account for Terraform and assign it the editor and owner basic IAM permissions
3. Click on the service account, go to the "Keys" tab, and create (download) a JSON key for it.
4. Move the key file into `terraform_core`, renaming it `gcp-credentials.json`
5. Copy the key file into `terraform_cluster`
6. Create `.gitignore` and `.dockerignore` files in the top level of the repository and add `gcp-cred*` to them

!!! danger

    Be careful storing the key file in your repository - you don't want to accidentally commit it or build it into a
    docker image layer. Double check that both copies of the key file are ignored by Git.

## Add the core infrastructure Terraform configuration

Inside the `terraform_core` directory, create a `main.tf` file and add the following to it:

```terraform
terraform {
  required_version = ">= 1.8.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>6.12"
    }
  }
}


provider "google" {
  project = var.google_cloud_project_id
  region  = var.google_cloud_region
  credentials = "gcp-credentials.json"
}


module "octue_twined_core" {
  source = "git::github.com/octue/terraform-octue-twined-core.git?ref=0.1.2"
  google_cloud_project_id          = var.google_cloud_project_id
  google_cloud_region              = var.google_cloud_region
  github_account                   = var.github_account
}
```

Next, create a `variables.tf` file and add the following, replacing the values in `<>` brackets:

```terraform
# Check in the GCP cloud console for your project ID
# (this is not necessarily the same as its name)
variable "google_cloud_project_id" {
  type    = string
  default = "<google-cloud-project-id>"
}

# Choose any region you like from
# https://cloud.google.com/about/locations. We like to
# choose a low carbon region like "europe-west9".
# See https://cloud.google.com/sustainability/region-carbon
variable "google_cloud_region" {
  type    = string
  default = "<google-cloud-region>"
}

# The account handle where your service repositories and your
# infrastructure repository is stored (ours is "octue")
variable "github_account" {
  type    = string
  default = "<handle>"
}
```

## Create the core infrastructure

In the command line, Change directory into `terraform_core` and run:

```shell
terraform init
terraform plan
```

If you're happy with the plan, run:

```shell
terraform apply
```

and approve the run.

## Add the cluster Terraform configuration

Inside the `terraform_cluster` directory, create a `main.tf` file and add the following to it:

```terraform
terraform {
  required_version = ">= 1.8.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>6.12"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
      version = "~>2.35"
    }
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = "~>1.19"
    }
  }

}


provider "google" {
  project     = var.google_cloud_project_id
  region      = var.google_cloud_region
  credentials = "gcp-credentials.json"
}


data "google_client_config" "default" {}


provider "kubernetes" {
  host                   = "https://${module.octue_twined_cluster.kubernetes_cluster.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(module.octue_twined_cluster.kubernetes_cluster.master_auth[0].cluster_ca_certificate)
}


provider "kubectl" {
  load_config_file       = false
  host                   = "https://${module.octue_twined_cluster.kubernetes_cluster.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(module.octue_twined_cluster.kubernetes_cluster.master_auth[0].cluster_ca_certificate)
}


locals {
  workspace_split = split("-", terraform.workspace)
  environment = element(local.workspace_split, length(local.workspace_split) - 1)
}


module "octue_twined_cluster" {
  source = "git::github.com/octue/terraform-octue-twined-cluster.git?ref=0.2.1"
  google_cloud_project_id = var.google_cloud_project_id
  google_cloud_region = var.google_cloud_region
  environment = local.environment
  cluster_queue = var.cluster_queue
}
```

Next, create a `variables.tf` file and add the following, replacing the parts in `<>` brackets with the same values as
before:

```terraform
variable "google_cloud_project_id" {
  type = string
  default = "<google-cloud-project-id>"
}


variable "google_cloud_region" {
  type = string
  default = "<google-cloud-region>"
}


variable "cluster_queue" {
  type = object(
    {
      name                  = string
      max_cpus              = number
      max_memory            = string
      max_ephemeral_storage = string
    }
  )
  default = {
    name                  = "cluster-queue"
    max_cpus              = 100
    max_memory            = "256Gi"
    max_ephemeral_storage = "10Gi"
  }
}
```

## Create the cluster

In the command line, change directory into `terraform_cluster` and run:

```shell
terraform init
terraform plan
```

If you're happy with the plan, run:

```shell
terraform apply
```

and approve the run.

## Next steps

!!! success

    Congratulations on setting up a Twined service network! Next up:

    - [Create the first Twined service on your new service network](../getting_started/creating_services.md)
    - Learn more about the Terraform modules used in this guide:
        - [Core module](https://github.com/octue/terraform-octue-twined-core)
        - [Cluster module](https://github.com/octue/terraform-octue-twined-cluster)
