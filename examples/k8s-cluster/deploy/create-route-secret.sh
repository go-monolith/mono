#!/bin/sh
# create-route-secret.sh: create the Secret holding the NATS cluster route
# password of the k8s-cluster example, with a random password.
#
#   ./create-route-secret.sh [namespace]     default: mono-demo
#
# The pods read it as NATS_ROUTE_PASSWORD and pass it to WithNATSClusterAuth,
# which requires it from every route and adds it to the routes the server
# opens. Running the script again replaces the password; restart the pods
# afterwards (kubectl -n <namespace> rollout restart sts/mono-cluster).
set -eu

NS=${1:-mono-demo}

# Hex only, so the password needs no escaping in a route URL.
PASS=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')

kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"
kubectl -n "$NS" create secret generic mono-cluster-routes \
  --from-literal=password="$PASS" --dry-run=client -o yaml | kubectl apply -f -
echo "Secret $NS/mono-cluster-routes created."
