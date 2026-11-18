#!/bin/sh
# create-route-tls-secret.sh: create the Secret with the route TLS certificate
# of the k8s-cluster example: a private CA and one certificate for every pod
# of the StatefulSet (wildcard DNS name of the headless Service), usable as
# both TLS server and client, as mutual TLS on routes needs.
#
#   ./create-route-tls-secret.sh [namespace] [days]     defaults: mono-demo 365
#
# Requires openssl. The pods read the files from the Secret
# mono-cluster-route-tls (tls.crt, tls.key, ca.crt) through
# NATS_ROUTE_TLS_DIR and pass them to WithNATSClusterTLS.
#
# Running the script again with the existing CA (kept in ./route-ca/ next to
# where you run it) issues a new certificate from the same CA: the running
# servers pick it up for new routes without a restart. A running server
# refuses a certificate from another CA (it keeps its previous one), so the
# script will not create a new CA while the Secret exists unless NEW_CA=1 is
# set; a new CA then needs every pod restarted at once, since servers with the
# old and the new CA cannot route to each other.
#
# For automatic renewal, use cert-manager instead: route-tls-cert-manager.yaml.
set -eu

NS=${1:-mono-demo}
DAYS=${2:-365}
SVC=mono-cluster-nats
DNS="*.${SVC}.${NS}.svc.cluster.local"
CA_DIR=./route-ca
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Fail early when the cluster cannot be reached: the checks below need it.
if ! kubectl get --raw /version >/dev/null; then
  echo "error: cannot reach the Kubernetes API with kubectl" >&2
  exit 1
fi

if [ -f "$CA_DIR/ca.key" ] && [ ! -f "$CA_DIR/ca.crt" ]; then
  echo "error: $CA_DIR/ca.key exists without $CA_DIR/ca.crt" >&2
  exit 1
fi
if [ ! -f "$CA_DIR/ca.key" ]; then
  # A new CA is not trusted by servers already running with the old one:
  # refuse to replace the CA of an existing Secret unless asked to.
  if kubectl -n "$NS" get secret mono-cluster-route-tls >/dev/null 2>&1 && [ "${NEW_CA:-}" != "1" ]; then
    echo "error: no $CA_DIR/ca.key here, but the Secret $NS/mono-cluster-route-tls exists." >&2
    echo "Run the script from the directory that holds route-ca/ to renew the certificate," >&2
    echo "or set NEW_CA=1 to create a new CA (then restart every pod at once)." >&2
    exit 1
  fi
  mkdir -p "$CA_DIR"
  chmod 700 "$CA_DIR"
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout "$CA_DIR/ca.key" -out "$CA_DIR/ca.crt" \
    -subj "/CN=mono-cluster route CA" -days 3650
  echo "Created a new route CA in $CA_DIR (keep ca.key private)."
fi

cat > "$WORK/ext.cnf" <<EOF
subjectAltName=DNS:${DNS}
extendedKeyUsage=serverAuth,clientAuth
keyUsage=digitalSignature
basicConstraints=CA:FALSE
EOF

openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$WORK/tls.key" -out "$WORK/tls.csr" -subj "/CN=${DNS}"
openssl x509 -req -in "$WORK/tls.csr" -CA "$CA_DIR/ca.crt" -CAkey "$CA_DIR/ca.key" \
  -CAcreateserial -days "$DAYS" -extfile "$WORK/ext.cnf" -out "$WORK/tls.crt"

kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"
kubectl -n "$NS" create secret generic mono-cluster-route-tls \
  --type=kubernetes.io/tls \
  --from-file=tls.crt="$WORK/tls.crt" --from-file=tls.key="$WORK/tls.key" \
  --from-file=ca.crt="$CA_DIR/ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "Secret $NS/mono-cluster-route-tls created for ${DNS} (valid ${DAYS} days)."
