# Deployment Guide

This guide covers building, deploying, and updating
`frame-metadata-extractor` on VAST DataEngine. It assumes you have
`vastde` CLI installed, Docker running, and credentials for your VAST
cluster's container registry.

## Prerequisites

### `vastde` CLI

```bash
vastde config init \
  --vms-url https://<VMS_HOST>/ \
  --tenant <TENANT_NAME> \
  --username <USERNAME> \
  --password <PASSWORD> \
  --builder-image-url <REGISTRY_HOST>/vast-builder:latest

vastde config view          # confirm
vastde functions list       # smoke test
```

Sessions expire periodically — re-run `vastde config init` if you get
`401 Unauthorized`.

### Docker

Add `"min-api-version": "1.38"` to Docker Engine config (Docker Desktop:
Settings → Docker Engine → JSON). Restart Docker after editing.

```json
{
  "min-api-version": "1.38"
}
```

### Cached Images

The CNB build needs the builder image and the paketo run image cached
locally. After every Docker restart, re-pull if missing:

```bash
docker pull <REGISTRY_HOST>/vast-builder:latest
docker pull docker.io/paketobuildpacks/run-jammy-full:0.1.76
```

## Build and Deploy (First Time)

### 1. Build the CNB image

Run from the **project root** (where `Dockerfile.fix` and `functions/`
live).

```bash
cd /path/to/frame-metadata-extractor

vastde functions build frame-metadata-extractor \
  --target functions/frame_metadata_extractor \
  --pull-policy never
```

Build typically takes 2–4 minutes on warm caches. Output is the local
image `frame-metadata-extractor:latest`. On failure check
`functions/frame_metadata_extractor/build.log` for the full pip / apt
output.

### 2. Apply Dockerfile.fix

The CNB launcher does not set `LD_LIBRARY_PATH` correctly for apt-installed
codecs. `Dockerfile.fix` is a thin layer that fixes that and bypasses the
launcher to start the VAST runtime directly.

```bash
docker build --platform linux/amd64 --no-cache \
  -t <REGISTRY_HOST>/frame-metadata-extractor:v0.2.0 \
  -f Dockerfile.fix .
```

**Always use a unique version tag** (`v0.2.0`, `v0.2.1`, …). Never reuse
`:latest` — Kubernetes caches images by tag and will keep serving the old
image even after a push.

### 3. Push to the registry

```bash
docker push <REGISTRY_HOST>/frame-metadata-extractor:v0.2.0
```

### 4. Create the function (first deployment only)

```bash
vastde functions create \
  --name frame-metadata-extractor \
  --description "Multi-format VFX frame metadata extractor (EXR, DPX, TIFF, PNG, TGA, HDR, JPEG2000, Cineon) with VastDB persistence" \
  --container-registry <REGISTRY_NAME> \
  --artifact-source frame-metadata-extractor \
  --artifact-type image \
  --image-tag v0.2.0
```

This creates the function record and revision 1 in one call.

### 5. Wire into a pipeline

Pipelines are easiest to create via the VMS UI:
`https://<VMS_HOST>/dataengine/#/pipelines` → Create New Pipeline. Add
the function as an action, attach an Element trigger filtered to the
suffixes you want to handle (`.exr`, `.dpx`, …), choose **unordered**
delivery, set environment variables (see
[`CONFIGURATION.md`](CONFIGURATION.md)), and deploy.

The CLI's `pipelines create` command is known to return 500 errors for
some configurations — use the UI for initial creation.

### 6. Verify

```bash
vastde functions get frame-metadata-extractor
vastde pipelines get <pipeline-name>      # should show "Status: Ready"
vastde logs get <pipeline-name> --since 5m

# Trigger a smoke test by uploading a frame
aws s3 cp test.exr s3://<SOURCE_BUCKET>/ \
  --endpoint-url http://<DATA_VIP>
```

You should see in the logs:

```
INITIALIZING FRAME-METADATA-EXTRACTOR 0.2.0
Supported formats: cineon, dpx, jpeg2000, openexr, png, rgbe, targa, tiff
FRAME-METADATA-EXTRACTOR initialized successfully
...
Processing new frame inspection request
Element event - Trigger: ..., ID: ...
FRAME INSPECTION RESULTS:
  File: s3://<bucket>/test.exr (... bytes, format=openexr)
  Parts: 1
  Channels: 4
  AOVs: 1 (beauty)
  Persistence: success
```

## Updating a Deployed Function

```bash
# 1. Make code changes
# 2. Bump __version__ in functions/frame_metadata_extractor/main.py
# 3. Delete the old local image to force a clean rebuild
docker rmi frame-metadata-extractor:latest

# 4. Build
vastde functions build frame-metadata-extractor \
  --target functions/frame_metadata_extractor \
  --pull-policy never

# 5. Apply Dockerfile.fix with a new version tag
docker build --platform linux/amd64 --no-cache \
  -t <REGISTRY_HOST>/frame-metadata-extractor:v0.X.Y \
  -f Dockerfile.fix .

# 6. Push
docker push <REGISTRY_HOST>/frame-metadata-extractor:v0.X.Y

# 7. Create a new revision pointing at the new tag
vastde functions update frame-metadata-extractor --image-tag v0.X.Y

# 8. Set the new revision as current (CLI or UI)
#    UI: Functions → frame-metadata-extractor → Revisions →
#        select revision N → Set As Current Version → Finish update

# 9. Redeploy the pipeline
vastde pipelines deploy <pipeline-name>

# 10. Verify
vastde pipelines get <pipeline-name>
vastde logs get <pipeline-name> --since 5m
```

Known CLI quirk: see [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) for the
"vastde update reports 'no revisions found'" workaround.

## Rolling Back

The cleanest rollback is to set a previous revision as current via the
VMS UI — no rebuild needed since the older image is still in the registry.

```
Functions → frame-metadata-extractor → Revisions →
  select previous revision → Set As Current Version → Finish update
```

Then redeploy the pipeline:

```bash
vastde pipelines deploy <pipeline-name>
```

## Versioning Convention

The `__version__` constant in `main.py` and the image tag should always
match (e.g., `__version__ = "0.2.0"` ↔ `image_tag = v0.2.0`). The version
is logged on `init()` and emitted in every result payload as
`extraction.tool_version`, which is the canonical way for downstream
consumers to know which extractor produced a row.

Bump rules:

- **patch** (`0.2.x`) — bug fix, no schema change
- **minor** (`0.x.0`) — new field added (additive), or new format support
- **major** (`x.0.0`) — schema change that's not backward-compatible
  (removing a field, changing semantics, renaming a column). Bump
  `schema_version` in the payload too.
