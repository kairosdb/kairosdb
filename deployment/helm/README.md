This is the official helm chart for installing KairosDB in a kubernetes cluster. At the moment,
we support configuring the following backends:

* H2
* Cassandra

# Getting started

In order to test the KairosDB installation within a kubernetes cluster with an external cassandra you can try the following commands:

```bash
kind create cluster
export KUBECONFIG=~/.kube/config

cd deployment/helm
helm install -f values.yaml --set storage.cassandra.enabled=true \
    --set storage.h2.enabled=false \
    --set storage.cassandra.contactPoints=192.168.1.103 \
    --set replicaCount=3 \
    test .
kubectl get pods # wait until the container starts and is in state running
kubectl port-forward svc/test-kairosdb 8080:80 # you should now be able to access kairosdb on localhost:8080
```

You should use **helm 3** and kind **0.7.0** but the chart is also compatible with newer versions of **helm 2** (>v2.14.0).
In the above example we use kind only for demo purposes.
The contact points should be the valid comma separated list of cassandra nodes listening on port 9042.

If you want to test with a local cassandra instance you can use the following command:

```bash
docker run -it --rm -p 0.0.0.0:9042:9042 cassandra:latest # this will run a local instance of cassandra.
```

For more customisation options, please consult the **values.yaml** file.