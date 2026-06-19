# How task pods get blob-store access (FLYTE_AWS_*)

## Mechanism (from flyte/storage/_config.py)
Flyte's a0 runtime reads storage config via `flyte.storage.S3.auto()`, which reads these env vars:
- `FLYTE_AWS_ENDPOINT`
- `FLYTE_AWS_ACCESS_KEY_ID`
- `FLYTE_AWS_SECRET_ACCESS_KEY`
- `FLYTE_AWS_S3_ADDRESSING_STYLE` (optional)

Resolution order: (1) static creds from FLYTE_AWS_* (above), (2) AWS_PROFILE+AWS_CONFIG_FILE,
(3) obstore default chain (IAM/workload-identity/IMDS, and generic AWS_* env as fallback).
There is also an `anonymous`/`skip_signature` mode, and `S3.for_sandbox()` (minio/miniostorage).
obstore allows plain HTTP by default (client_options allow_http=True), so no extra flag is needed.

## How the Flyte backend (devbox) does it
FlytePropeller stamps FLYTE_AWS_* onto EVERY in-cluster task pod (sourced from the flyte-binary
storage config). Verified on a devbox driver pod:
  FLYTE_AWS_ENDPOINT=http://rustfs-svc.flyte:9000
  FLYTE_AWS_ACCESS_KEY_ID=rustfs
  FLYTE_AWS_SECRET_ACCESS_KEY=rustfsstorage
No secretRef, no mounted secret, default SA. It is cluster-level config injected at pod creation.

## Why the connector injects it
Armada pods are NOT created by FlytePropeller, so nothing stamps FLYTE_AWS_* on them. The connector
forwards the same storage config (the SAME mechanism, not a workaround). It is a self-hosted S3
store (RustFS/MinIO), not AWS cloud. The only Armada-specific bit is the ENDPOINT: the Armada pod
is on the kind cluster, so it uses the host NodePort (`host:30002`) instead of the in-cluster
`rustfs-svc.flyte:9000`. The endpoint is config; the creds are the platform's.

RustFS creds (rustfs/rustfsstorage) live in the `rustfs-secret` in the devbox `flyte` namespace.
The demo reads them to configure the connector (FLYTE_BLOB_* env on c0); in production the operator
would configure the connector with the org's S3 config directly.

Connector code: `_storage_env()` in src/armada_flyte/connector.py injects FLYTE_AWS_*.
