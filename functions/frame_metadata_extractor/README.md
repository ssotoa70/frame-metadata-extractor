# frame-metadata-extractor Function

VAST DataEngine serverless function for multi-format VFX / post-production
frame metadata extraction. Generalizes the single-format `exr-inspector`
into a format-dispatching inspector covering the still-frame formats used in
scan, editorial, color, and finishing pipelines.

> Function-level quick reference. For the full project documentation
> (architecture, deployment, troubleshooting, schema), see the docs in
> the project root: `../../docs/`.

## Supported Formats

| Extension(s)            | Format     | Primary reader | Notes |
|-------------------------|------------|----------------|-------|
| `.exr`                  | OpenEXR    | OpenImageIO    | Multi-part, deep, ACES-aware |
| `.dpx`                  | DPX        | OIIO + raw SMPTE 268M parser | Captures film/tv fields OIIO drops (timecode, frame rate, creator, project, film gauge) |
| `.cin`                  | Cineon     | OpenImageIO    | Legacy path — passive support |
| `.tif` / `.tiff`        | TIFF       | OpenImageIO    | Baseline tags, EXIF IFD, ICC profile |
| `.png`                  | PNG        | OpenImageIO    | IHDR, gAMA/cHRM/iCCP, tEXt/iTXt chunks |
| `.tga`                  | TARGA      | OpenImageIO (+ footer range GET) | Developer area detected via 26-byte EOF read |
| `.hdr` / `.rgbe`        | Radiance   | OpenImageIO    | FORMAT, EXPOSURE, primaries |
| `.jp2` / `.j2c` / `.j2k`| JPEG2000   | OpenImageIO    | SIZ / COD markers |

Movie containers (R3D, ARRIRAW, X-OCN) are intentionally **not** supported
— use a dedicated transcoder function for those.

## Files

| File | Purpose |
|------|---------|
| `main.py`                 | Handler: event parsing, S3 range GET, format dispatch, AOV grouping, normalized output |
| `dpx_header.py`           | SMPTE 268M raw header parser (fields OIIO drops) |
| `vast_db_persistence.py`  | VAST DataBase persistence (5 tables: files / parts / channels / attributes / aovs), vector embeddings, table auto-creation |
| `requirements.txt`        | Python dependencies (OpenImageIO, boto3, pyarrow, vastdb) |
| `Aptfile`                 | System packages (OIIO + per-format codecs) |

## Handler Signature

```python
def init(ctx):    # One-time setup: S3 client, VastDB session, DDL
def handler(ctx, event):  # Per-request: range-GET header, dispatch, inspect, persist
```

## Output Schema (summary)

```
file        { path, format, size_bytes, mtime, frame_number, multipart_count, is_deep }
parts[]     # per-subimage (single entry for most formats)
channels[]  # per-channel, with layer/component split
aovs[]      # per-AOV per-part per-view: name, components, channel_group,
            #   data_type, bit_depth, depth_label, category, color_space,
            #   light_group, synthetic, uncompressed_bytes, pct_of_frame_logical
            #   (cryptomatte ranks roll up to one row with `ranks: N`)
attributes  { parts: [[{name,type,value}, ...], ...] }
color       { color_space, transfer_function, primaries }
timecode    { value, rate }
sequence    { frame_number }
camera      { make, model, lens, exposure, fnumber, iso }
production  { creator, copyright, description, software }
extraction  { tool, tool_version, timestamp, warnings[] }
errors[]
```

Full field reference + per-format samples: `../../docs/FRAMES_METADATA_SCHEMA.MD`.
JSON Schema: `../../docs/frame_metadata.schema.json`.

## Header-Only Reads

Like `exr-inspector`, the function issues a single 256 KB S3 range GET and
feeds the resulting buffer to OIIO. This works for every supported format
because all of them keep metadata at the start of the file. TGA's
developer-area footer is probed with a second 26-byte tail range GET.

## VAST DataBase Storage

Schema: `frame_metadata` (tables: `files`, `parts`, `channels`,
`attributes`, `aovs`). All five inserts run in a single transaction per
event so AOV / channel rows can never become orphans of a missing files
row. The `files` table adds a `format` column on top of the
`exr-inspector` schema so downstream queries can filter by container type
without re-parsing paths.

Set `VAST_DB_BUCKET` (default `frame-data`) and `VAST_DB_SCHEMA` (default
`frame_metadata`) environment variables, or override via
`ctx.secrets['vast-db']`. Full table reference and query patterns:
`../../docs/DATABASE_SCHEMA.md`.

## Build

```bash
vastde functions build frame-metadata-extractor \
    --target functions/frame_metadata_extractor \
    --pull-policy never
```
