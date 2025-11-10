Twined services run as "pods" on a Kubernetes cluster. If there's a problem with the pod's container image, it can
fail to start on the cluster and fail silently. This is most likely a deployment or infrastructure problem, not
a problem caused by the code running in the service. However, if a
[custom Dockerfile](https://github.com/octue/workflows?tab=readme-ov-file#deploying-a-kuberneteskueue-octue-twined-service-revision)
is specified for the service by the app creator, this is a likely cause of the problem.

## Monitoring Twined services

`kubectl` is a standard Kubernetes CLI tool for interacting with clusters. We can use it to observe Twined
services currently running questions as well as recently successful or failed questions.

### Observing questions

!!! warning

    This tool requires permission to access and interact with the Kubernetes cluster running the Twined service network.
    It's mostly useful for the service network administrator but others can use it, too, if they're given the relevant
    permissions. Be careful who is given these permissions - `kubectl` is a powerful tool.

Follow the
[installation and authentication](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)
instructions (installing `kubectl` and using the `gcloud container clusters get-credentials` command to authenticate
with the cluster) and then run:

```shell
kubectl get pods
```

You'll see something like this:

```
NAME                                                  READY   STATUS              RESTARTS   AGE
question-372a0c94-b95e-4a0e-8a9f-019d0bf3046b-wm7gp   0/1     Error               0          23m
question-5c9a6e86-5431-44fb-bd3a-936fcaf217c1-6cqzb   0/1     ContainerCreating   0          1s
question-5c9a6e86-5431-44fb-bd3a-936fcaf217d1-3cqzc   1/1     Running             0          1s
question-23dfb292-f23e-4524-9676-9deff9d4f1bd-nhb26   0/1     Completed           0          13s
```

Each pod is named like `question-<question-uuid>-wm7gp`, representing a question asked to a service in your service
network with the question UUID `<question-uuid>`. The group of characters after the UUID is non-deterministic
and not relevant.

### Question statuses

There are several possible statuses for a question. The most relevant are:

- `Pending` - the question has yet to be accepted by the cluster
- `ContainerCreating` - the Twined service is starting up and hasn't run the question yet
- `Error` - the question failed or the service's pod failed to start
- `Running` - the question is running in the Twined service
- `Completed` - the question completed successfully and the service returned its results

## Inspecting a failed question

If the question has an `Error` status, you can inspect it to see its logs:

```shell
kubectl describe pod question-372a0c94-b95e-4a0e-8a9f-019d0bf3046b-wm7gp
```

The `Events` section at the bottom is often useful in finding what the issue is:

```
...

Events:
  Type    Reason     Age   From                                   Message
  ----    ------     ----  ----                                   -------
  Normal  Scheduled  47m   gke.io/optimize-utilization-scheduler  Successfully assigned default/question-372a0c94-b95e-4a0e-8a9f-019d0bf3046b-wm7gp to gk3-main-octue-twined-cl-nap-1a9cv5dt-f15cf29a-sdzc
  Normal  Pulled     47m   kubelet                                Container image "europe-west9-docker.pkg.dev/octue-twined-services/octue-twined-services/octue/example-service-kueue:0.1.0" already present on machine
  Normal  Created    47m   kubelet                                Created container: question-372a0c94-b95e-4a0e-8a9f-019d0bf3046b
  Normal  Started    47m   kubelet                                Started container question-372a0c94-b95e-4a0e-8a9f-019d0bf3046b
```

If it's not helpful or looks successful (as above), follow up with the question's logs to see if something went wrong in
the app code:

```shell
kubectl logs question-372a0c94-b95e-4a0e-8a9f-019d0bf3046b-wm7gp
```
