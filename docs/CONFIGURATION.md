# Configuration Reference

All configuration is via environment variables, set at the DataEngine
pipeline or function deployment level. Functions intentionally have no
hardcoded endpoints, credentials, or bucket names — every value is
externalized.

## Required Environment Variables

| Variable | Description | Example |
|---|---|---|
| `S3_ENDPOINT` | VAST S3 data VIP. No trailing slash. | `http://10.0.0.10` |
| `S3_ACCESS_KEY` | S3 access key for the source bucket | — |
| `S3_SECRET_KEY` | S3 secret key for the source bucket | — |

If any of the three S3 variables are unset or empty, the function logs a
warning during `init()` and S3 operations will fail at request time. The
function does **not** crash on init for missing S3 config so that pods can
still come up while operators correct configuration.

## VAST DataBase Variables

The function reuses the S3 credentials for VastDB unless DB-specific
overrides are set. This works on most clusters where the VIP serves both
the S3 protocol and the database protocol.

| Variable | Description | Default |
|---|---|---|
| `VAST_DB_ENDPOINT` | VastDB endpoint URL | falls back to `S3_ENDPOINT` |
| `VAST_DB_ACCESS_KEY` | DB access key | falls back to `S3_ACCESS_KEY` |
| `VAST_DB_SECRET_KEY` | DB secret key | falls back to `S3_SECRET_KEY` |
| `VAST_DB_BUCKET` | DataBase-enabled bucket name | `frame-data` |
| `VAST_DB_SCHEMA` | Schema name (auto-created on first run) | `frame_metadata` |
| `VAST_DB_SECRET_NAME` | Name of the `ctx.secrets` entry to use, when running with secrets injected by the pipeline | `vast-db` |

The bucket itself must already exist as a Database-enabled view; the
function creates the schema and tables but cannot create the bucket.

## Secrets Pattern

When running on DataEngine, the function prefers credentials injected via
`ctx.secrets[<VAST_DB_SECRET_NAME>]` over environment variables. The
secrets entry is expected to expose `endpoint`, `access_key`, and
`secret_key`. Falls back cleanly to env vars when secrets are not
available (e.g., for local development).

## Trigger Configuration

Configure suffix filters on the Element trigger so the function only fires
for the formats it can handle:

| Suffix | Format |
|---|---|
| `.exr` | OpenEXR |
| `.dpx` | DPX |
| `.cin` | Cineon |
| `.tif`, `.tiff` | TIFF |
| `.png` | PNG |
| `.tga` | TARGA |
| `.hdr`, `.rgbe` | Radiance HDR |
| `.jp2`, `.j2c`, `.j2k` | JPEG2000 |

The function also rejects unsupported extensions defensively, but suffix
filtering at the trigger level avoids unnecessary cold starts.

### Delivery Mode

Use **unordered** delivery — frame inspections are independent and
parallelizable. Ordered delivery would serialize frames and is unnecessary
for this workload.

## Tuning

The function's runtime tuning lives in `main.py`:

| Constant | Default | Purpose |
|---|---|---|
| `HEADER_RANGE_BYTES` | `256 * 1024` | First-range read size; covers every supported format's metadata |
| `TGA_FOOTER_BYTES` | `26` | Tail-range read size for TGA developer area probing |
| boto3 `max_pool_connections` | `25` | S3 client connection pool |
| boto3 `connect_timeout` | `5s` | S3 connect timeout |
| boto3 `read_timeout` | `15s` | S3 read timeout |
| boto3 retries | `3` (adaptive) | S3 retry policy |

These are sized for typical 4K-8K still frames with rich attribute bags.
Increase `HEADER_RANGE_BYTES` only if a specific format / writer combo
puts metadata past 256 KB (rare; flag a warning if you find one).

## Resource Limits

Memory and CPU limits are set at the function deployment level via the
DataEngine UI or pipeline YAML. Recommended starting point:

- **Memory**: 2 GiB (OIIO peak working set on multi-part EXRs is
  ~500 MiB; leave headroom for boto3 + vastdb)
- **CPU**: 1 vCPU (workload is I/O-bound)

The function does not stream large pixel data — it works on header bytes
only — so it is unlikely to need more than the above for 99% of frames.

## Development Mode

There is no `DEV_MODE` flag in this function. To run it locally without a
VAST cluster:

1. Set `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` to point at any
   S3-compatible endpoint (MinIO, AWS S3) holding test fixtures
2. Leave the `VAST_DB_*` variables unset; `persist_to_vast_database`
   detects the missing config and returns `status: "skipped"` without
   attempting to connect
3. Use `vastde functions invoke` or the `localrun` command to send a
   synthetic Element event
