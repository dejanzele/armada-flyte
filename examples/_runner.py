"""Shared run helper for the examples. Import it INSIDE an example's ``if __name__ == "__main__"``
block only, so the task pod (which imports the example module to load the task, but never runs
__main__) never needs to import this file.

It dispatches to local execution or a Flyte backend. The run_local.sh / demo/run.sh scripts set the
blob-store env (FLYTE_BLOB_*) and BACKEND before launching the example.
"""

from __future__ import annotations

import os

import flyte
import flyte.remote
import flyte.storage


def run(entrypoint, **inputs):
    """Run entrypoint locally (default) or through the Flyte backend, returning its first output."""
    if os.environ.get("BACKEND"):
        flyte.init(endpoint="localhost:30080", insecure=True, project="flytesnacks", domain="development")
        r = flyte.run(entrypoint, **inputs)
        print(f"\nsubmitted run {r.name}\n  UI: {r.url}")
        r.wait()
        return flyte.remote.Run.get(r.name).outputs()[0]
    flyte.init(
        storage=flyte.storage.S3(
            endpoint=os.environ["FLYTE_BLOB_ENDPOINT"],
            access_key_id=os.environ["FLYTE_BLOB_ACCESS_KEY"],
            secret_access_key=os.environ["FLYTE_BLOB_SECRET_KEY"],
            region="us-east-1",
            addressing_style="path",
        ),
    )
    return flyte.with_runcontext(mode="local", raw_data_path="s3://flyte/raw").run(entrypoint, **inputs).outputs()[0]
