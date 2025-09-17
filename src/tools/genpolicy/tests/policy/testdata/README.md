# Integration Test Data

The test data consists of a Kubernetes resource definition (usually, a pod) and a JSON doc of agent requests.
The requests were obtained by deploying the resource without a policy, fetching the policy logs from the VM and extracting those of relevance to the test case.

If not further specified, the tests were executed on a cluster set up as follows:

```sh
export DOCKER_REGISTRY='ghcr.io'
export DOCKER_REPO='kata-containers/kata-deploy-ci'
export DOCKER_TAG='ca244c726570b91c3160ede171d8216cbe7efa5f-nightly-amd64' # https://github.com/kata-containers/kata-containers/commits/ca244c726570b91c3160ede171d8216cbe7efa5f
export KATA_HOST_OS='ubuntu'
export KATA_HYPERVISOR='qemu-coco-dev'
export KUBERNETES='vanilla'
export PULL_TYPE='guest-pull'
export SNAPSHOTTER='nydus'
export AZ_NODEPOOL_TAGS="AzSecPackAutoConfigReady=true"
# other AZ_ config omitted

bash ./tests/integration/kubernetes/gha-run.sh create-cluster
bash ./tests/integration/kubernetes/gha-run.sh get-cluster-credentials
bash ./tests/integration/kubernetes/gha-run.sh deploy-snapshotter
bash ./tests/integration/kubernetes/gha-run.sh deploy-kata-aks
```
