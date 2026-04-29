# frame-metadata-extractor

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12-green.svg)](https://www.python.org/downloads/)
[![VAST DataEngine](https://img.shields.io/badge/VAST-DataEngine-blue.svg)](https://www.vastdata.com/)
[![OpenImageIO](https://img.shields.io/badge/OpenImageIO-3.x-orange.svg)](https://openimageio.readthedocs.io/)
[![VAST DataBase](https://img.shields.io/badge/VAST-DataBase-green.svg)](https://www.vastdata.com/)

A VAST DataEngine serverless function that extracts rich, normalized metadata
from VFX / post-production frame files the moment they land in a VAST S3
view. EXR, DPX, Cineon, TIFF, PNG, TGA, HDR/RGBE, and JPEG2000 — one
function, one schema, one downstream table set.

For each frame written to the watched bucket, the function:

1. Range-GETs the first 256 KB of the object (no full download)
2. Parses the header with OpenImageIO plus format-specific fallbacks
   (SMPTE 268M for DPX, TGA developer area, etc.)
3. Groups channels into AOVs (beauty, diffuse, AO, depth, normals,
   cryptomatte, light groups, …) with renderer-meaningful classification
4. Writes normalized records to VAST DataBase across `files`, `parts`,
   `channels`, `attributes`, and `aovs` tables

## What It Captures

- **File geometry** — width / height, display vs data window, tiling,
  multi-part / multi-view EXR, deep flag
- **Channels** — full per-channel layout with layer / component split,
  pixel type (HALF/FLOAT/UINT8/16/32), subsampling
- **AOVs** — first-class renderer-meaningful groups with channel composition,
  bit depth label (16f/32f/8u), color space, category (beauty / light_group
  / utility / matte / crypto / denoise / deep / data), light group, and
  per-frame uncompressed-bytes estimate. Cryptomatte ranks roll up to a
  single AOV row with `ranks: N`. Stereo views are preserved as separate
  AOV rows
- **DPX SMPTE 268M fields** OpenImageIO drops silently — film
  manufacturer, frame position, slate info, raw timecode, shutter angle,
  scanner serial number — parsed from the raw 2048-byte header
- **EXIF / ICC / tEXt** — TIFF, JPEG2000, and PNG metadata in addition to
  the raw OIIO attribute bag
- **Normalized cross-format blocks** — `color`, `timecode`, `camera`,
  `production` so downstream queries don't have to know which format
  carries which fact

## Project Structure

```
frame-metadata-extractor/
├── Dockerfile.fix              # CNB launcher fix (LD_LIBRARY_PATH, entrypoint)
├── LICENSE
├── README.md                   # ← this file
├── docs/
│   ├── ARCHITECTURE.md
│   ├── CONFIGURATION.md
│   ├── DATABASE_SCHEMA.md
│   ├── DEPLOYMENT.md
│   ├── TROUBLESHOOTING.md
│   ├── FRAMES_METADATA_SCHEMA.MD   # field-by-field schema + per-format samples
│   └── frame_metadata.schema.json  # JSON Schema (draft 2020-12)
└── functions/
    └── frame_metadata_extractor/
        ├── main.py                  # init() + handler()
        ├── dpx_header.py            # SMPTE 268M raw parser
        ├── vast_db_persistence.py   # VAST DataBase writes
        ├── requirements.txt
        ├── Aptfile                  # OIIO + per-format codecs
        └── README.md                # function-level quick reference
```

## Quick Start

```bash
# Build the CNB image
vastde functions build frame-metadata-extractor \
    --target functions/frame_metadata_extractor \
    --pull-policy never

# Apply the launcher fix and tag with a unique version
docker build --platform linux/amd64 --no-cache \
    -t <REGISTRY_HOST>/frame-metadata-extractor:v0.2.0 \
    -f Dockerfile.fix .

# Push and create / update the function
docker push <REGISTRY_HOST>/frame-metadata-extractor:v0.2.0
vastde functions update frame-metadata-extractor --image-tag v0.2.0

# Redeploy the pipeline
vastde pipelines deploy <pipeline-name>
```

Full deployment walkthrough, including the Docker prerequisites and the
common gotchas, lives in [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

## Documentation

| Document | What it covers |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design, event flow, format dispatch, AOV grouping logic |
| [CONFIGURATION.md](docs/CONFIGURATION.md) | Environment variables, defaults, secrets pattern |
| [DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md) | All five VastDB tables — columns, types, query patterns, dedup |
| [DEPLOYMENT.md](docs/DEPLOYMENT.md) | Build, push, deploy, verify, update workflows |
| [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Build / deploy / runtime failure modes and fixes |
| [FRAMES_METADATA_SCHEMA.MD](docs/FRAMES_METADATA_SCHEMA.MD) | Field-level prose + per-format sample payloads |
| [frame_metadata.schema.json](docs/frame_metadata.schema.json) | Machine-readable JSON Schema (draft 2020-12) |

## Supported Formats

| Extension(s) | Format | Notes |
|---|---|---|
| `.exr` | OpenEXR | Multi-part, multi-view (stereo), deep, ACES-aware. AOV-rich. |
| `.dpx` | DPX | OIIO + custom SMPTE 268M parser for the film/tv fields OIIO drops |
| `.cin` | Cineon | OIIO only — passive support |
| `.tif`, `.tiff` | TIFF | EXIF IFD, ICC profile, multi-page (subimage) |
| `.png` | PNG | gAMA/cHRM/iCCP, tEXt/iTXt key-values |
| `.tga` | TARGA | OIIO + footer range GET probe |
| `.hdr`, `.rgbe` | Radiance | FORMAT, EXPOSURE, primaries |
| `.jp2`, `.j2c`, `.j2k` | JPEG2000 | SIZ / COD markers |

Movie containers (R3D, ARRIRAW, X-OCN, MOV, MP4) are **not** supported —
those need a transcoder function.

## Output Schema

Every payload has the same top-level shape regardless of input format:

```
file        { path, format, size_bytes, mtime, frame_number, multipart_count, is_deep }
parts[]     # one per OIIO subimage
channels[]  # per-channel pixel layout
aovs[]      # one per AOV per part per view, with category + size + color_space
attributes  { parts: [[{name,type,value}, ...], ...] }
color       { color_space, transfer_function, primaries }
timecode    { value, rate }
sequence    { frame_number }
camera      { make, model, lens, exposure, fnumber, iso }
production  { creator, copyright, description, software }
extraction  { tool, tool_version, timestamp, warnings[] }
errors[]
persistence { status, file_id, inserted, message }   # appended by writer
```

Detailed field reference and per-format sample payloads:
[`docs/FRAMES_METADATA_SCHEMA.MD`](docs/FRAMES_METADATA_SCHEMA.MD).

## Requirements

- VAST cluster with DataEngine and DataBase enabled
- VAST S3 view with Element triggers configured for the frame extensions you
  want to process
- A DataBase-enabled bucket for persistence (auto-creates schema and tables
  on first run)

## Companion Functions

This function complements rather than replaces other media processing
functions in the same pipeline:

- `oiio-proxy-generator` — JPEG thumbnail + proxy generation
- `video-metadata-extractor` / `video-proxy-generator` — for movie containers

Run them in parallel on the same Element triggers; each writes to its own
table set.

## License

MIT — see [LICENSE](LICENSE).
