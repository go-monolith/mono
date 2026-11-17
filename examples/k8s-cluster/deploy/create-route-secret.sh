#!/bin/sh
# create-route-secret.sh: create the Secret holding the NATS passwords of the
# k8s-cluster example, with random values.
#
#   ./create-route-secret.sh [namespace]     default: mono-demo
#
# Keys:
#   password         the cluster route password. The pods read it as
#                    NATS_ROUTE_PASSWORD and pass it to WithNATSClusterAuth,
#                    which requires it from every route and adds it to the
#                    routes the server opens.
#   system-password  the password of the NATS system account user "admin".
#                    The pods read it as NATS_SYSTEM_PASSWORD and pass it to
#                    WithNATSSystemAccount, for operator tooling (nats server
#                    report jetstream, nats server raft peer-remove).
#
# Running the script again replaces both passwords; restart the pods
# afterwards (kubectl -n <namespace> rollout restart sts/mono-cluster). To add
# only the system password to an existing Secret, keeping the route password:
#
#   kubectl -n mono-demo patch secret mono-cluster-routes --type merge \
#     -p "{\"stringData\":{\"system-password\":\"$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')\"}}"
set -eu

NS=${1:-mono-demo}

# Hex only, so the passwords need no escaping in a route URL.
PASS=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
SYS_PASS=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')

kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"
kubectl -n "$NS" create secret generic mono-cluster-routes \
  --from-literal=password="$PASS" --from-literal=system-password="$SYS_PASS" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "Secret $NS/mono-cluster-routes created."
