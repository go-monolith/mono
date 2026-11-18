#!/bin/sh
# create-client-tls-secret.sh: create the Secrets with the client TLS
# certificates of the k8s-cluster example, for clusters without cert-manager
# (see client-tls-cert-manager.yaml for automatic renewal):
#   - a private client CA, separate from the route CA (a client certificate
#     from the route CA would be accepted as a cluster peer);
#   - mono-cluster-client-tls: the servers' client listener certificate, for
#     the Service mono-cluster-client and for localhost/127.0.0.1 (kubectl
#     port-forward), TLS server use;
#   - mono-greeter-client-tls: the client Deployment's certificate, TLS client
#     use.
# Each Secret's ca.crt is the client CA.
#
#   ./create-client-tls-secret.sh [namespace] [days]     defaults: mono-demo 365
#
# Requires openssl. Running it again with the existing CA (kept in
# ./client-ca/ next to where you run it) issues new certificates from the same
# CA: the servers and the clients pick them up without a restart. The script
# will not create a new CA while the Secrets exist unless NEW_CA=1 is set; the
# servers read the client CA once, so a new CA needs them restarted (a rolling
# restart is enough when the old CA stays in ca.crt until the clients moved).
set -eu

NS=${1:-mono-demo}
DAYS=${2:-365}
SVC=mono-cluster-client
CA_DIR=./client-ca
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

if ! kubectl get --raw /version >/dev/null; then
  echo "error: cannot reach the Kubernetes API with kubectl" >&2
  exit 1
fi

if [ -f "$CA_DIR/ca.key" ] && [ ! -f "$CA_DIR/ca.crt" ]; then
  echo "error: $CA_DIR/ca.key exists without $CA_DIR/ca.crt" >&2
  exit 1
fi
if [ ! -f "$CA_DIR/ca.key" ]; then
  if kubectl -n "$NS" get secret mono-cluster-client-tls >/dev/null 2>&1 && [ "${NEW_CA:-}" != "1" ]; then
    echo "error: no $CA_DIR/ca.key here, but the Secret $NS/mono-cluster-client-tls exists." >&2
    echo "Run the script from the directory that holds client-ca/ to renew the certificates," >&2
    echo "or set NEW_CA=1 to create a new CA (then restart the servers)." >&2
    exit 1
  fi
  mkdir -p "$CA_DIR"
  chmod 700 "$CA_DIR"
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout "$CA_DIR/ca.key" -out "$CA_DIR/ca.crt" \
    -subj "/CN=mono-cluster client CA" -days 3650
  echo "Created a new client CA in $CA_DIR (keep ca.key private)."
fi

# issue <name> <subject CN> <extensions>: a certificate signed by the client CA.
issue() {
  printf '%s\nkeyUsage=digitalSignature\nbasicConstraints=CA:FALSE\n' "$3" > "$WORK/$1.cnf"
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout "$WORK/$1.key" -out "$WORK/$1.csr" -subj "/CN=$2"
  openssl x509 -req -in "$WORK/$1.csr" -CA "$CA_DIR/ca.crt" -CAkey "$CA_DIR/ca.key" \
    -CAcreateserial -days "$DAYS" -extfile "$WORK/$1.cnf" -out "$WORK/$1.crt"
}
issue listener "$SVC.$NS.svc.cluster.local" \
  "subjectAltName=DNS:$SVC.$NS.svc.cluster.local,DNS:$SVC.$NS.svc,DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"
issue client mono-greeter-client "extendedKeyUsage=clientAuth"

kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"
for s in "mono-cluster-client-tls listener" "mono-greeter-client-tls client"; do
  set -- $s
  kubectl -n "$NS" create secret generic "$1" \
    --type=kubernetes.io/tls \
    --from-file=tls.crt="$WORK/$2.crt" --from-file=tls.key="$WORK/$2.key" \
    --from-file=ca.crt="$CA_DIR/ca.crt" \
    --dry-run=client -o yaml | kubectl apply -f -
done
echo "Secrets $NS/mono-cluster-client-tls and $NS/mono-greeter-client-tls created (valid ${DAYS} days)."
