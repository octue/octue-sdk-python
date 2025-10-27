You need authentication while using Twined to:

- Use or run services
- Access output data from analyses run on services

Authentication is provided by a GCP service account.

## Creating a service account

By setting up your Twined service network with the
[Twined Terraform modules](/deploying_services), a set of maintainer service accounts have already been
created with the required permissions. These will have names starting with `maintainer-`.

## Using a service account

### Locally

1.  Access your service accounts
    [here](https://console.cloud.google.com/iam-admin/serviceaccounts),
    making sure the correct project is selected, or ask your Twined service network administrator
2.  Click on the relevant service account, go to the "Keys" tab, and
    create (download) a JSON key for it - it will be called
    `<project-name>-XXXXX.json`.

    !!! danger

        It's best not to store this in your repository to prevent accidentally
        committing it or building it into a docker image layer. Instead, keep it
        somewhere else on your local system with any other service account keys
        you already have.

        If you must keep within your repository, it's good practice to name the
        file `gcp-credentials.json` and make sure that `gcp-cred*` is in your
        `.gitignore` and `.dockerignore` files.

3.  If you're developing in a container (like a VSCode `devcontainer`),
    mount the file into the container. You can make gcloud available
    too - check out [this
    tutorial](https://medium.com/datamindedbe/application-default-credentials-477879e31cb5).
4.  Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the
    absolute path of the key file. If using a `devcontainer`, make sure
    this is the path inside the container and not the path on your local
    machine.

### On GCP infrastructure / deployed services

- Credentials are automatically provided when running code or services on GCP infrastructure, including the Kubernetes
  cluster
- Twined uses these when running on these platforms, so there's no need to upload a service account key or include one
  in service docker images
