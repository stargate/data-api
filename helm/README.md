# Stargate JSON API deployment using Helm

## Pre-requisites

### Stargate GRPC Bridge Instance
You'll need the Stargate Bridge instance deployed with port 8091 accessible

#### Stargate GRPC Bridge Setup
You can deploy the stargate bridge / coordinator node using the helm setup at https://github.com/stargate/stargate/tree/main/helm

### Autoscaling
Autoscaling uses metrics server. Metrics server can be installed by executing the command:

```shell script
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

### Ingress
To expose access to Stargate APIs from clients outside your Kubernetes cluster, make sure an ingress controller is installed. To install the Nginx ingress controller, execute the command:

```shell script
 helm upgrade --install ingress-nginx ingress-nginx \
   --repo https://kubernetes.github.io/ingress-nginx \
   --namespace ingress-nginx --create-namespace 
```

You'll use the name of ingress class name as the `ingress.ingressClassName` in the Stargate Helm chart values.

When using ingress, the API service paths need to be set as specified in the table

| API     | Default path when using ingress       |
|---------|---------------------------------------|
| jsonapi | http://localhost/stargate/health/live |

## Helm installation
Clone this repo to your development machine. Then execute the following commands to install the Stargate Helm chart with default values:

```shell script
cd helm
helm install docsv3 docsv3
```

Note:
- The default values in the Helm values file (`values.yaml`) are set to assume that Stargate GRPC Bridge is setup in `default` kubernetes namespace

To install with overriden values, you can use the `--set` option as shown below:

```shell script
helm install docsv3 docsv3 \
--namespace <ENTER_NAMESPACE_HERE> \
--set sgGrpcBridge.namespace=<SG_GRPC_BRIDGE_NAMESPACE> \
--set replicaCount=2
```