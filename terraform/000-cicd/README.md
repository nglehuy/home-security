# terraform/000-cicd

This is Terraform root 0. It gives the deploy workflow `.github/workflows/deploy.yml` access to the k3s API. You apply it by hand, once. The workflow never applies it, because a job must not manage its own credentials.

## Contents

- The ServiceAccount `ci-deployer` in `kube-system`, with the `cluster-admin` ClusterRole.
- The Secret `ci-deployer-token`. It holds a long-lived token of `ci-deployer`.
- The sensitive output `ci_kubeconfig`. It is the kubeconfig that the workflow uses.

## How the workflow connects

- The GitHub runner joins the tailnet and connects to `https://<api_host>:6443`.
- `api_host` is the MagicDNS name of the k3s node: the machine name plus the tailnet DNS name. The DNS page of the Tailscale admin console shows the tailnet DNS name ([Tailscale docs](https://tailscale.com/kb/1081/magicdns)). MagicDNS must be on.
- The k3s API certificate does not list the MagicDNS name. So the kubeconfig sets `tls-server-name: kubernetes`, a name that every k3s API certificate lists.
- The token comes from a Secret of type `kubernetes.io/service-account-token`, so it does not expire ([Kubernetes docs](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/#manually-create-an-api-token-for-a-serviceaccount)).
- The workflow applies `010-workload` and `020-app`. They create namespaces, CRDs, ClusterRoles, and their state Secrets in `kube-system`, so `ci-deployer` needs `cluster-admin`.

## Before you apply

- Make sure that your kubeconfig can reach the cluster as a cluster admin.
- Make sure that a device in your tailnet can reach port 6443 of the node.

## Apply

1. Copy `terraform.tfvars.example` to `terraform.tfvars`.
2. Fill in the values.
3. Run `terraform init`.
4. Run `terraform apply`.

To save the kubeconfig as a secret of the GitHub environment `pers-infra`, run this command from the repository root. It writes no file:

```sh
terraform -chdir=terraform/000-cicd output -raw ci_kubeconfig | gh secret set PERS_INFRA_KUBECONFIG --env pers-infra
```

To make sure that the kubeconfig works, run this command on a device in your tailnet. The output must list the node. `<(...)` works in bash and zsh.

```sh
kubectl --kubeconfig <(terraform -chdir=terraform/000-cicd output -raw ci_kubeconfig) get nodes
```

## Revoke the token

To revoke the token and get a new one, run this command. Then save `PERS_INFRA_KUBECONFIG` again.

```sh
terraform -chdir=terraform/000-cicd apply -replace=kubernetes_secret_v1.ci_token
```

## Variables

- Cluster: `kubeconfig_path`, `kube_context`.
- CI kubeconfig: `cluster_name`, the name of the cluster and of the context, for example `pers-infra`.
- Tailnet: `api_host`, for example `vn-exit.tail1234.ts.net`.

## State

Terraform keeps the state in the local file `.terraform/local.tfstate`, in this folder. `.gitignore` excludes the `.terraform` folder. Do not commit the state.

Terraform keeps its backend record in `.terraform/terraform.tfstate`. So the state uses another file name.

Do not delete the `.terraform` folder. The folder holds the state. Without the state, Terraform loses track of the ServiceAccount, the binding, and the Secret.

Warning: the state holds the token, which has `cluster-admin` rights and does not expire. A device that has the token and can reach port 6443 on the tailnet controls the cluster. Limit that port to `tag:ci` in the tailnet policy.

## Test

Run `terraform init` and `terraform test`. The tests use `apply` with a mock Kubernetes provider, so the kubeconfig output gets a mock token and CA. They cover the identity, the token Secret, each field of the kubeconfig, and the `api_host` check.
