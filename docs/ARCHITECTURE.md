# Architecture

## Overview

`frame-metadata-extractor` is a stateless serverless function that runs on
VAST DataEngine. It is triggered by `Element.ObjectCreated` events on a VAST
S3 view whenever a frame file lands. For each event the function range-reads
the file's header, parses the metadata with OpenImageIO plus format-specific
fallbacks, normalizes the output to a shared schema, derives renderer-
meaningful AOV groupings, and persists the result to VAST DataBase.

The container has no persistent state. Every pod-level resource (S3 client,
VastDB session, table existence) is set up once in `init(ctx)` and reused
for every event the pod processes.

## Event Flow

```
VAST S3 view
   │
   │  PUT shot.0042.exr
   ▼
ElementTrigger ──→ pipeline ──→ frame-metadata-extractor pod
                                     │
                                     │  1. event.as_element_event()
                                     │     → bucket + object_key + elementpath
                                     │
                                     │  2. _detect_format(key)
                                     │     → openexr / dpx / tiff / ...
                                     │
                                     │  3. S3 range GET (first 256 KB)
                                     │     → temp file with original suffix
                                     │     → if TGA: tail GET for 26-byte footer
                                     │
                                     │  4. Format dispatch:
                                     │     - openexr / cineon / tiff / png
                                     │       / tga / hdr / jp2  → _inspect_oiio
                                     │     - dpx → _inspect_oiio + parse_dpx_header
                                     │
                                     │  5. _extract_aovs(parts, channels, format)
                                     │     → grouped, classified, sized AOV records
                                     │
                                     │  6. persist_to_vast_database(payload)
                                     │     → idempotent upsert by file_id
                                     │     → insert across files / parts / channels
                                     │       / attributes / aovs in one transaction
                                     │
                                     ▼
                                  return result dict (logged + traced)
```

## Format Dispatch

`SUPPORTED_EXTENSIONS` (in `main.py`) maps file extensions to canonical
format names. Most formats route through the generic OIIO path
(`_inspect_oiio`) which iterates every subimage and produces:

- per-part geometry, tiling, compression, color space
- per-channel name / type / sampling
- the full OIIO `extra_attribs` bag

DPX additionally invokes `parse_dpx_header(raw)` from `dpx_header.py`,
which `struct.unpack`s the fixed 2048-byte SMPTE 268M header and merges
fields under a `dpx:` prefix in the part-0 attribute bag. Selected fields
(timecode, frame rate, creator, project) are also promoted to the
normalized cross-format blocks (`timecode`, `production`).

TGA is the only format whose metadata may live at end-of-file. The
inspector probes the last 26 bytes for the TRUEVISION-XFILE signature and
handles the developer / extension area when present.

## Normalized Output Shape

Every payload has the same top-level keys regardless of input format. See
[`FRAMES_METADATA_SCHEMA.MD`](FRAMES_METADATA_SCHEMA.MD) for the full
field-level reference. The blocks are:

- `file` — identity and physical attributes
- `parts[]` — per-subimage geometry
- `channels[]` — flat per-channel pixel layout (use `part_index` to
  re-associate with parts)
- `aovs[]` — renderer-meaningful AOV groupings (see below)
- `attributes` — raw OIIO + format-specific bag, one list per part
- `color`, `timecode`, `sequence`, `camera`, `production` — normalized
  convenience projections so downstream queries don't have to know which
  format carries which fact
- `extraction` — tool name, version, timestamp, warnings
- `errors[]` — non-fatal parse errors

A `persistence` block is appended by the writer reporting `status` /
`file_id` / `inserted`.

## AOV Grouping

The `_extract_aovs(parts, channels, format)` helper turns the flat channel
list into renderer-meaningful AOV records suitable for UI display and
queryable storage. Two EXR conventions are handled:

1. **Single-part EXRs** — channels named `<aov>.<component>` (e.g.
   `diffuse.R`) share a layer. Bare `R/G/B/A` become the implicit
   `beauty` AOV; bare `Z` becomes `depth`.
2. **Multi-part EXRs** — each part's `part_name` is the AOV name; the
   in-part channels (`R/G/B/A`) become the components.

Stereo views are part of the grouping key — left and right beauty produce
two AOV rows distinguished by `view`, not collapsed.

### Cryptomatte Rollup

Cryptomatte ranks (`uCryptoObject00`, `uCryptoObject01`, …) are detected by
regex and **collapsed into a single AOV row** with `name = uCryptoObject`,
`ranks = N`, and `category = crypto`. This matches how compositors think
about cryptomatte (one AOV with multiple ranks) rather than exposing each
rank as a separate row.

### Channel Canonicalization

EXR stores channels alphabetically (so an RGB layer reads back as
`B, G, R`). The grouping helper canonicalizes components to semantic order
(R, G, B, A; X, Y, Z; U, V) before building the `channel_group` UI label
(`RGB`, `RGBA`, `XYZ`, `UV`, `Z`, …).

### Classification

Each AOV is tagged with a `category`:

| Category | Trigger |
|---|---|
| `beauty` | name in {beauty, rgba, main, combined} |
| `crypto` | matches cryptomatte rank regex |
| `light_group` | name matches `<base>_<group>` for known bases (diffuse, specular, sss, transmission, emission, indirect, direct, volume, reflection, refraction, coat) |
| `data` | name in {z, depth, n, normal(s), p, position, motion, motion_vec, uv, st, id, objectid, matteid} |
| `matte` | name contains "matte" or "mask" |
| `denoise` | name contains "denoise_albedo", "denoise_normal", or "variance" |
| `deep` | the part is a deep-pixel EXR part |
| `utility` | fallback |

For light-grouped AOVs, the `light_group` field captures the suffix as
sidecar metadata while `name` keeps the full layer name (e.g. `diffuse_key`
becomes `name="diffuse_key"`, `light_group="key"`). This avoids row
collisions when multiple light-group AOVs share a base.

### Color-Space Inference

Data AOVs (depth, normals, motion vectors, IDs, cryptomatte) are forced to
`color_space = "raw"` regardless of what the part attribute says, so
viewers and DI tools know not to apply a color transform.

### Per-Frame Size Estimate

For each AOV the helper computes
`uncompressed_bytes = pixels × channels × bytes_per_channel` honoring
per-channel subsampling. This is the **uncompressed** logical size — the
honest, deterministic number. Per-AOV compressed bytes are not knowable
from EXR headers (all AOVs share the compressor block-by-block), so the
function deliberately does not pretend to compute them.

`pct_of_frame_logical` is the AOV's share of that frame's total
uncompressed footprint, suitable for UI bars without requiring a join.

### Synthetic AOV for Non-EXR Formats

DPX, TIFF, PNG, TGA, HDR, JPEG2000, and Cineon don't have a real AOV
concept. For these the helper emits one AOV row labeled `beauty` with
`synthetic = true` so downstream UIs can render the same panel uniformly
across formats.

## Re-render Dedup

Re-rendering the same frame produces a new `file_id`
(`sha256(path + mtime + ...)`), so each render becomes a new row across
all five tables. The UI is expected to dedupe by joining to the `files`
table and keeping `MAX(mtime)` per `file_path_normalized`. The existing
`files.mtime` column is ISO 8601 UTC with `+00:00`, which is
lexicographically sortable — no schema change needed for dedup.

## Persistence Layer

All five tables are written in one VAST DataBase transaction per event so
that AOV / channel rows can never become orphans of a missing files row.
See [`DATABASE_SCHEMA.md`](DATABASE_SCHEMA.md) for the full table set and
common query patterns.

The function also computes two embedding vectors at write time:

- `metadata_embedding` (384-dim, in `files`) — deterministic structural
  fingerprint suitable for "find similar files" lookups
- `channel_fingerprint` (128-dim, in `channels`) — channel-layout
  fingerprint, written on the first channel row only

Both are derived deterministically from the payload (no ML model
dependency in the runtime).

## What's Out of Scope

- Movie containers (R3D / ARRIRAW / X-OCN / MOV / MP4) — use a
  transcoder function and a separate metadata extractor for those
- Pixel-level analysis (histograms, defect detection, matte coverage) —
  needs a heavier pipeline that downloads full files
- Color-transform application — this function is read-only on the source
  data; OCIO transforms belong in proxy / review functions
