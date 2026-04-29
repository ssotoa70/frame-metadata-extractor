# Troubleshooting Guide

Common failure modes for `frame-metadata-extractor` and how to fix them.

## Build Issues

| Symptom | Cause | Fix |
|---|---|---|
| `client version 1.38 is too old` | Docker API version mismatch | Add `"min-api-version": "1.38"` to Docker Engine config and restart |
| `run-image does not exist on the daemon` | Paketo run image not cached | `docker pull docker.io/paketobuildpacks/run-jammy-full:0.1.76` |
| `builder image does not exist on the daemon` | VAST builder image not cached | `docker pull <REGISTRY_HOST>/vast-builder:latest` |
| `failed to save image` | Disk full | `df -h /` — need ~4 GB free; `docker system prune -f` |
| `handler file not found` | Wrong working directory | Run `vastde functions build` from project root, not from `functions/` |
| `401 Unauthorized` | Session expired | Re-run `vastde config init` with credentials |
| `pip install OpenImageIO fails` | Aptfile changed; codec lib mismatch | Check `Aptfile`; verify `libopenimageio-dev` and `python3-openimageio` versions are pinned to a CNB-compatible release |

## Deployment Issues

### "no revisions found for function" on `vastde functions update`

**Symptom:**
```
$ vastde functions update frame-metadata-extractor --image-tag v0.X.Y
no revisions found for function <guid>
```

**Cause:** the `vastde` CLI's `update` command does a GET-modify-PUT
against `/api/latest/serverless/function-revisions` with no pagination,
fetching only the first ~100 revisions cluster-wide. If your function's
existing revisions land past the cutoff (or were created with
`is_published: false` "local" revisions), the GET returns nothing and
update aborts before it can PUT.

**Workaround:** PUT directly against the function endpoint to register a
new revision, then set it as current via the UI.

```bash
TOKEN=$(curl -sk -X POST "https://<VMS_HOST>/api/token/<TENANT>" \
  -H 'Content-Type: application/json' \
  -d '{"username":"<USERNAME>","password":"<PASSWORD>"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access'])")

curl -sk -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://<VMS_HOST>/api/latest/serverless/functions/<FUNCTION_GUID>" \
  -d '{
    "name": "frame-metadata-extractor",
    "description": "...",
    "artifact_source": "frame-metadata-extractor",
    "artifact_type": "image",
    "image_tag": "v0.X.Y",
    "container_registry_vrn": "vast:dataengine:container-registries:<REGISTRY_NAME>",
    "is_published": true
  }'
```

The PUT response includes `last_revision_number` — the new revision
number it just created. Then in the VMS UI:

```
Functions → frame-metadata-extractor → Revisions →
  select revision N → Set As Current Version → Finish update
```

Then `vastde pipelines deploy <pipeline-name>` to roll the new revision
into the pipeline.

### `RevisionMissing` after deploy

**Cause:** container fails to start. Usually one of:

- Dockerfile.fix wasn't applied (CNB launcher can't find vast_runtime)
- Reused an existing version tag — K8s served the cached old image
- Image was built for the wrong platform

**Fix:** confirm the image was built with `--platform linux/amd64`,
confirm the version tag is unique and was actually pushed, redeploy.

### Pipeline stuck in `InProgress`

K8s is pulling the image. Wait 1–2 minutes. Verify the image tag in the
function record matches what was pushed to the registry. If it stays
stuck >5 min, check pod events via the VMS UI for `ImagePullBackOff` or
`ErrImagePull`.

### Pipeline `create` returns 500

Known CLI bug. Create the pipeline via the VMS UI instead, then use the
CLI for subsequent `deploy` operations.

## Runtime Issues

| Symptom in logs | Cause | Fix |
|---|---|---|
| `OpenImageIO not available in runtime` | OIIO Python bindings missing | Verify `Aptfile` includes `python3-openimageio`; rebuild |
| `cannot open shared object file` (any libimath/libopenexr/etc.) | `LD_LIBRARY_PATH` in Dockerfile.fix is incomplete | Find the lib in the running container with `find /layers/apt-buildpack -name 'lib*.so*'`, add the parent directory to `LD_LIBRARY_PATH` in Dockerfile.fix |
| `S3 client not initialized` | `S3_ENDPOINT` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` missing or empty | Check pipeline env vars |
| `VastDB not configured - persistence will be skipped` | DB endpoint or credentials missing | Set `VAST_DB_*` env vars or inject `vast-db` secret |
| `Failed to extract Element properties` | Event payload doesn't match Element schema | Verify the trigger is an Element trigger (not a generic CloudEvent), and that the bucket is on a properly configured view |
| `Unsupported file extension: ...` | Suffix filter not set on trigger | Add suffix filters to the Element trigger so the function only fires for handled formats |
| `OpenImageIO failed to open file` | Header range read truncated, or file is corrupt | Increase `HEADER_RANGE_BYTES` if a known-good file consistently fails; otherwise the source file is bad |
| `DPX raw header parse failed` | DPX magic bytes don't match | Verify the file is actually DPX (`file shot.dpx`); some DPX writers emit non-standard headers — file an issue with a sample |

## Data Issues

### AOV count looks wrong

The `aovs[]` array is grouped by `(part_index, name, view)`. If a frame
shows fewer rows than expected:

- For multi-part EXRs, each part's `part_name` becomes the AOV name —
  if part names are missing or duplicated, AOV count will be off
- Cryptomatte ranks are intentionally collapsed into one row (`ranks: N`),
  not exposed individually
- Stereo views are NOT collapsed — left and right beauty produce two rows

If you're seeing AOVs labeled `beauty` for everything, the EXR may have
all channels under a single beauty layer with no AOV-style naming. That's
not a bug — that's what the file actually contains.

### `pct_of_frame_logical` doesn't sum to 100

Rounding. Each row is rounded to 4 decimal places. For 50+ AOV frames
the sum will land at 99.9–100.1.

### Re-rendered frames double-count in queries

By design — the function inserts a new row with a fresh `file_id` for
each render. Dedupe in your query:

```sql
WHERE f.mtime = (
  SELECT MAX(f2.mtime) FROM files f2
  WHERE f2.file_path_normalized = f.file_path_normalized
)
```

See [`DATABASE_SCHEMA.md`](DATABASE_SCHEMA.md#re-render-dedup-strategy)
for the full dedup discussion.

## Checking Logs

```bash
# Recent activity
vastde logs get <pipeline-name> --since 5m

# Filter to this function only
vastde logs get <pipeline-name> --since 10m 2>&1 | grep frame-metadata

# Errors only
vastde logs get <pipeline-name> --since 1h 2>&1 | grep -E '\[ERROR\]|FAIL'

# Confirm version in production
vastde logs get <pipeline-name> --since 5m 2>&1 | grep INITIALIZING
```

Log markers:
- `[user]` — code from this function
- `[vast-runtime]` — VAST runtime SDK (event delivery, lifecycle)

## Reporting Bugs

Include the following when filing an issue:

1. Function `__version__` and image tag
2. Source frame format and a representative sample (or anonymized
   metadata)
3. Full log lines around the failure (including the line above and below)
4. The full result payload if the failure is in classification / AOV
   grouping rather than a crash
