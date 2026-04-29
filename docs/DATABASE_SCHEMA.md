# Database Schema Reference

`frame-metadata-extractor` writes to **five tables** in the configured
VastDB schema (default `frame_metadata`). All tables are auto-created on
first run via the get-or-create pattern in `vast_db_persistence.py`. The
DataBase-enabled bucket must already exist; everything else is provisioned
by the function.

## Schema Overview

```
<VAST_DB_BUCKET>/
└── <VAST_DB_SCHEMA>/   (default: frame_metadata)
    ├── files          1 row per inspected frame
    ├── parts          1 row per OIIO subimage  (usually 1; N for multi-part EXR)
    ├── channels       1 row per channel
    ├── attributes     1 row per attribute per part
    └── aovs           1 row per AOV per part per view
```

All five inserts run in a single VastDB transaction per event so that AOV
and channel rows can never become orphans of a missing files row.

## `files`

One row per inspected frame. Re-inspection of the same path with a new
mtime mints a new `file_id`; queries should dedupe by joining on
`MAX(mtime) GROUP BY file_path_normalized`.

| Column | Type | Notes |
|---|---|---|
| `file_id` | string | `sha256(path + mtime + path_md5)[:16]` |
| `file_path` | string | VAST view path or S3 key |
| `file_path_normalized` | string | lowercased, forward-slash normalized — dedup key |
| `format` | string | `openexr` / `dpx` / `cineon` / `tiff` / `png` / `targa` / `rgbe` / `jpeg2000` |
| `header_hash` | string | sha256 over part geometry + multipart_count + is_deep |
| `size_bytes` | int64 | full file size from `Content-Range`, not the range read |
| `mtime` | string | ISO 8601 UTC; lexicographically sortable for dedup |
| `multipart_count` | int32 | number of OIIO subimages |
| `is_deep` | bool | true for deep EXR |
| `metadata_embedding` | list<float32, 384> | deterministic structural fingerprint |
| `frame_number` | int32 | parsed from filename, nullable |
| `inspection_timestamp` | string | first-inspection time |
| `inspection_count` | int32 | re-inspection counter |
| `last_inspected` | string | last-inspection time |

## `parts`

One row per OIIO subimage. Most formats produce a single part; multi-part
EXR produces N rows. Window fields are stored as JSON strings to preserve
the full `{min:{x,y}, max:{x,y}}` shape.

| Column | Type | Notes |
|---|---|---|
| `file_id` | string | foreign key |
| `file_path` | string | denormalized for query convenience |
| `part_index` | int32 | 0-based |
| `width`, `height` | int32 | data window extent |
| `display_width`, `display_height` | int32 | display window extent |
| `data_x_offset`, `data_y_offset` | int32 | data window origin |
| `part_name` | string | EXR part name |
| `view_name` | string | EXR multi-view name (left/right for stereo) |
| `multi_view` | bool | |
| `data_window` | string | JSON-encoded `{min:{x,y}, max:{x,y}}` |
| `display_window` | string | JSON-encoded `{min:{x,y}, max:{x,y}}` |
| `pixel_aspect_ratio` | float32 | |
| `line_order` | string | e.g. `increasingY` |
| `compression` | string | format-specific (`dwab`, `zips`, `zip`, `none`, `rle`, ...) |
| `color_space` | string | from `oiio:ColorSpace` attribute |
| `render_software` | string | from `Software` attribute |
| `is_tiled` | bool | |
| `tile_width`, `tile_height`, `tile_depth` | int32 | 0 when not tiled |
| `is_deep` | bool | |

## `channels`

One row per channel across all parts. The 128-dim fingerprint is written
on the first row only (zero-padded on the rest) to avoid duplication.

| Column | Type | Notes |
|---|---|---|
| `file_id` | string | foreign key |
| `file_path` | string | denormalized |
| `part_index` | int32 | which subimage |
| `channel_name` | string | full name e.g. `diffuse.R` |
| `layer_name` | string | portion before last `.` |
| `component_name` | string | portion after last `.` |
| `channel_type` | string | `HALF` / `FLOAT` / `UINT8/16/32` / ... |
| `x_sampling`, `y_sampling` | int32 | usually 1 |
| `channel_fingerprint` | list<float32, 128> | first row only; zeros elsewhere |

## `attributes`

One row per attribute per part. Values are stored four ways for query
flexibility — pick the column that matches how you want to filter.

| Column | Type | Notes |
|---|---|---|
| `file_id` | string | foreign key |
| `file_path` | string | denormalized |
| `part_index` | int32 | |
| `attr_name` | string | e.g. `dpx:frame_rate`, `chromaticities`, `Make` |
| `attr_type` | string | OIIO TypeDesc (STRING, INT, FLOAT, V2F, FLOAT[8], TIMECODE) |
| `value_json` | string | JSON-encoded value, always populated |
| `value_text` | string | populated when value is a string |
| `value_int` | int64 | populated when value is an int |
| `value_float` | float64 | populated when value is numeric |

## `aovs`

One row per AOV per part per view. Powers the per-shot AOV display panels
without re-deriving from `channels` at query time.

| Column | Type | Notes |
|---|---|---|
| `file_id` | string | foreign key |
| `file_path` | string | denormalized |
| `frame_number` | int32 | for sequence rollups |
| `aov_index` | int32 | ordering within the frame |
| `part_index` | int32 | which subimage the AOV lives in |
| `view` | string | EXR view (`left` / `right`); empty for non-stereo |
| `name` | string | AOV name e.g. `beauty`, `diffuse`, `uCryptoObject` |
| `name_normalized` | string | lowercased; for cross-renderer joins |
| `channel_group` | string | `RGBA` / `RGB` / `XYZ` / `UV` / `Z` / `R` / ... |
| `components` | list<string> | canonicalized order e.g. `[R,G,B,A]` |
| `channel_count` | int32 | |
| `data_type` | string | dominant pixel type (`MIXED` if heterogeneous) |
| `bit_depth` | int32 | 16 / 32 / 8 |
| `depth_label` | string | `16f` / `32f` / `8u` / ... |
| `category` | string | `beauty` / `light_group` / `utility` / `matte` / `crypto` / `denoise` / `deep` / `data` |
| `is_beauty` | bool | derived from category |
| `is_data` | bool | true for `data` / `crypto` / `matte` / `deep` |
| `color_space` | string | `raw` for data AOVs; otherwise inherits part color_space |
| `light_group` | string | suffix from `<base>_<group>` AOV names; empty otherwise |
| `synthetic` | bool | true for non-EXR formats with implicit single beauty AOV |
| `uncompressed_bytes` | int64 | per-frame, derived deterministically |
| `pct_of_frame_logical` | float64 | share of the frame's total uncompressed footprint |
| `ranks` | int32 | cryptomatte rank count; 0 for non-crypto AOVs |

## Common Query Patterns

### Latest version of every frame in a sequence

```sql
SELECT *
FROM files f
WHERE f.file_path_normalized LIKE 'shows/show_a/seq010/%'
  AND f.mtime = (
    SELECT MAX(f2.mtime)
    FROM files f2
    WHERE f2.file_path_normalized = f.file_path_normalized
  );
```

### AOV layer map for a shot (UI panel query)

```sql
SELECT a.name,
       ANY_VALUE(a.channel_group)        AS channels,
       ANY_VALUE(a.depth_label)          AS depth,
       SUM(a.uncompressed_bytes)         AS logical_bytes,
       COUNT(DISTINCT a.frame_number)    AS frame_count,
       COUNT(DISTINCT a.depth_label)     AS depth_variants,   -- > 1 means drift
       ANY_VALUE(a.is_beauty)            AS is_beauty,
       ANY_VALUE(a.category)             AS category,
       ANY_VALUE(a.color_space)          AS color_space,
       ANY_VALUE(a.ranks)                AS ranks
FROM aovs a
JOIN files f ON a.file_id = f.file_id
WHERE f.file_path_normalized LIKE 'shows/show_a/shot_010/%.exr'
  AND f.mtime = (
    SELECT MAX(f2.mtime)
    FROM files f2
    WHERE f2.file_path_normalized = f.file_path_normalized
  )
GROUP BY a.name, a.view
ORDER BY is_beauty DESC, a.name;
```

`depth_variants > 1` flags AOVs whose bit depth changed mid-sequence —
worth surfacing in the UI rather than masking with `ANY_VALUE`.

### Find every DPX scan from a specific film stock

```sql
SELECT f.file_path, f.frame_number
FROM files f
JOIN attributes a ON a.file_id = f.file_id
WHERE f.format = 'dpx'
  AND a.attr_name = 'dpx:film_mfg_id'
  AND a.value_text = '10';
```

### Find frames with a similar channel layout (vector search)

```sql
-- Syntax depends on the VastDB vector search API; conceptually:
SELECT file_path, channel_fingerprint <-> :reference_vector AS distance
FROM channels
WHERE part_index = 0
ORDER BY distance ASC
LIMIT 20;
```

## Re-render Dedup Strategy

The function deliberately does not attempt to update existing rows on
re-inspection — it inserts new rows with a new `file_id` derived from
`path + mtime`. This preserves a complete history of every render attempt
at the cost of requiring queries to dedupe.

Two reasons this is the chosen approach:

1. **Auditability** — render farm operators want to know which frames
   were re-rendered and when. Deleting old rows would destroy that signal.
2. **Schema simplicity** — no need for soft-delete columns or update
   triggers; the writer is pure insert-or-skip.

The dedup pattern (`MAX(mtime) GROUP BY file_path_normalized`) works
because `files.mtime` is ISO 8601 UTC with `+00:00`, which is
lexicographically sortable. A future addition could materialize a
`latest_files` view to make UI queries simpler.
