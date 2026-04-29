# Frame Metadata Schema

Reference for every field the `frame-metadata-extractor` function produces
and persists. Each supported frame format lands in the same top-level JSON
shape; what differs is which blocks get populated and what lives in the
per-part `attributes` bag.

- [Top-level shape](#top-level-shape)
- [Sample payloads per format](#sample-payloads-per-format)
  - [OpenEXR](#openexr--multi-part-aces-render)
  - [DPX](#dpx--10-bit-log-film-scan)
  - [TIFF](#tiff--16-bit-scan-with-exif--icc)
  - [PNG](#png--review-frame-with-text)
  - [TGA](#tga--legacy-hero-frame-with-developer-area)
  - [HDR / RGBE](#hdr--rgbe--environment-capture-for-ibl)
  - [JPEG2000](#jpeg2000--dci-2k-master-frame)
  - [Cineon](#cineon--legacy-scan)
- [VastDB table layout](#how-it-lands-in-vastdb)

---

## Top-level shape

Every payload has these top-level keys, always in the same order. Blocks
that a given format doesn't populate are present as empty objects or
arrays — the shape is predictable.

| Key | Purpose | Populated for |
|---|---|---|
| `schema_version` | integer, currently `1` | all |
| `file` | path, format, size, mtime, frame_number, multipart_count, is_deep | all |
| `parts[]` | one entry per subimage (1 for most formats, N for multi-part EXR) | all |
| `channels[]` | per-channel: name, layer, component, type, sampling | all |
| `aovs[]` | renderer-meaningful AOV groupings derived from channels (beauty, diffuse, AO, depth, normals, cryptomatte, light groups, …) with category, color space, bit depth label, per-frame size estimate | all (rich for EXR; single synthetic beauty row for non-EXR) |
| `attributes.parts[][]` | raw OIIO attribute bag per part + format-specific parsed fields (e.g. `dpx:*`) | all |
| `color` | `color_space`, `transfer_function`, `primaries` | EXR (full), DPX (via SMPTE codes), TIFF/PNG (from ICC), others (partial) |
| `timecode` | `value` (`HH:MM:SS:FF`), `rate` | DPX (from BCD field), EXR (from `smpte:TimeCode`), others blank |
| `sequence` | `frame_number` | any file with a numbered suffix (`shot.0042.exr`) |
| `camera` | make / model / lens / exposure / fnumber / iso | TIFF (EXIF IFD), JPEG2000 (APP2), DPX (input_device), others blank |
| `production` | creator / copyright / description / software | EXR, DPX, TIFF, PNG |
| `extraction` | `tool`, `tool_version`, `timestamp`, `warnings[]` | all |
| `errors[]` | non-fatal issues caught during parse | all |

The `persistence` key is appended by the VastDB writer after the handler
returns, reporting `status` (`success` / `skipped` / `error`), `file_id`,
and an `inserted` bool. It is not part of the handler return value itself.

### `file` block

| Field | Type | Source | Notes |
|---|---|---|---|
| `path` | string | VAST view path (elementpath) or S3 key | Use as the canonical path |
| `format` | string | detected from extension | One of `openexr`, `dpx`, `cineon`, `tiff`, `png`, `targa`, `rgbe`, `jpeg2000` |
| `s3_bucket` / `s3_key` | string | event payload | For retrieval / debug |
| `size_bytes` | int | S3 `Content-Range` header (full size, not range read) | |
| `mtime` | ISO-8601 string | S3 `LastModified` | UTC |
| `frame_number` | int or null | filename regex `[._](\d{3,8})\.<ext>$` | |
| `multipart_count` | int | OIIO subimage enumeration | Always 1 for single-part formats |
| `is_deep` | bool | EXR deep flag | Always false for non-EXR |

### `parts[]` block (per-subimage geometry)

| Field | Type | Meaning |
|---|---|---|
| `part_index` | int | 0-based subimage index |
| `width` / `height` | int | Data window extent |
| `display_width` / `display_height` | int | Display window extent (differs from width/height when dataWindow ≠ displayWindow, typical for EXR with overscan) |
| `data_x_offset` / `data_y_offset` | int | Data window origin |
| `part_name` | string | EXR part name or null |
| `view_name` | string | EXR multi-view name or null |
| `data_window` / `display_window` | object | `{min:{x,y}, max:{x,y}}` |
| `pixel_aspect_ratio` | float | Non-1.0 for anamorphic / broadcast |
| `line_order` | string | e.g. `increasingY` |
| `compression` | string | Format-specific token (`dwab`, `zips`, `none`, `deflate`, `rle`, `jpeg2000`, ...) |
| `color_space` | string | From `oiio:ColorSpace` attribute |
| `render_software` | string | From `Software` attribute |
| `is_tiled` | bool | |
| `tile_width` / `tile_height` / `tile_depth` | int or null | Only when tiled |
| `is_deep` | bool | EXR deep scanline or tile |

### `channels[]` block (per-channel)

| Field | Type | Meaning |
|---|---|---|
| `part_index` | int | Parent subimage |
| `name` | string | Full channel name (e.g. `diffuse.R`) |
| `layer_name` | string | Portion before last `.` (`diffuse`); empty for flat channels |
| `component_name` | string | Portion after last `.` (`R`); full name if no `.` |
| `type` | string | `HALF`, `FLOAT`, `UINT8`, `UINT16`, `UINT32` |
| `x_sampling` / `y_sampling` | int | Chroma subsampling, usually 1 |

### `aovs[]` block (per-AOV per-part per-view)

Derived by `_extract_aovs(parts, channels, format)` from the flat channel
list. Two EXR conventions are handled:

1. **Single-part EXRs** — channels named `<aov>.<component>` share a layer
   (`diffuse.R`, `diffuse.G`, …); bare `R/G/B/A` become `beauty`; bare `Z`
   becomes `depth`.
2. **Multi-part EXRs** — each part's `part_name` is the AOV name; the
   in-part channels are the components.

Stereo views are part of the grouping key — left and right beauty produce
two AOV rows distinguished by `view`. Cryptomatte ranks
(`uCryptoObject00`, `uCryptoObject01`, …) collapse into a single row with
`ranks: N`. Non-EXR formats emit one synthetic beauty row marked
`synthetic: true` so downstream UIs can render the same panel uniformly.

| Field | Type | Meaning |
|---|---|---|
| `name` | string | AOV name (e.g. `beauty`, `diffuse`, `uCryptoObject`, `motion_vec`) |
| `part_index` | int | Subimage this AOV lives in |
| `view` | string or null | EXR view (`left` / `right`); null when not stereo |
| `components` | string[] | Canonicalized component order (`R,G,B,A`; `X,Y,Z`; `U,V`) — not EXR's storage order |
| `channel_group` | string | UI label: `RGBA` / `RGB` / `XYZ` / `UV` / `Z` / `R` / single component |
| `channel_count` | int | Number of channels in this AOV |
| `data_type` | string | Dominant pixel type (`HALF` / `FLOAT` / `UINT16` / …); `MIXED` if heterogeneous |
| `bit_depth` | int | 16 / 32 / 8 |
| `depth_label` | string | UI label: `16f` / `32f` / `8u` / `16u` / `MIXED` |
| `category` | string | `beauty` / `light_group` / `utility` / `matte` / `crypto` / `denoise` / `deep` / `data` |
| `is_beauty` | bool | True when category is `beauty` |
| `is_data` | bool | True for non-color AOVs (data, crypto, matte, deep) — viewers should not apply view transforms |
| `color_space` | string | `raw` for data AOVs; otherwise inherits the part's color_space |
| `light_group` | string or null | Suffix from `<base>_<group>` AOV names (e.g. `diffuse_key` → `light_group: "key"`); null otherwise |
| `synthetic` | bool | True for non-EXR formats with implicit single beauty AOV |
| `uncompressed_bytes` | int | Per-frame logical size: `pixels × channels × bytes_per_channel`, honoring subsampling |
| `pct_of_frame_logical` | number | This AOV's share of the frame's total uncompressed footprint, as a percentage |
| `ranks` | int (optional) | Cryptomatte rank count; only present on `category: crypto` rows |

### `attributes.parts[][]` — raw attribute bag

One list per part; each entry is `{name, type, value}`. Values are
serialized via OIIO's `tolist()` / vector / color / window adapters into
JSON-safe structures. Binary attributes (like ICC profiles) are emitted as
`{"encoding": "base64", "data": "..."}`.

For DPX specifically, SMPTE 268M fields that OIIO drops silently are
appended with a `dpx:` prefix by the custom `dpx_header.py` parser — those
show up alongside OIIO's own attribute entries.

### Normalized cross-format blocks

`color`, `timecode`, `sequence`, `camera`, `production`, and `extraction`
are convenience projections of the richest information in the attribute
bag so that downstream queries don't have to guess where a given fact
lives across formats. When a field can't be derived, the key is simply
absent from the block (the block itself is always present).

---

## Sample payloads per format

Each sample below is a full handler return value — what would be emitted
as a JSON sidecar or what gets logged and persisted. The 384-dim
`metadata_embedding` and 128-dim `channel_fingerprint` vectors generated
during persistence are not part of the handler return, so they're omitted
here.

### OpenEXR — multi-part ACES render

```json
{
  "schema_version": 1,
  "file": {
    "path": "shows/OCT/101/comp/shot_010.0042.exr",
    "format": "openexr",
    "s3_key": "shows/OCT/101/comp/shot_010.0042.exr",
    "s3_bucket": "vfx-source",
    "size_bytes": 18432512,
    "mtime": "2026-04-14T18:42:11.000000+00:00",
    "frame_number": 42,
    "multipart_count": 3,
    "is_deep": false
  },
  "parts": [
    {
      "part_index": 0,
      "width": 3840, "height": 2160,
      "display_width": 3840, "display_height": 2160,
      "data_x_offset": -16, "data_y_offset": -16,
      "part_name": "rgba",
      "data_window":    {"min": {"x": -16, "y": -16}, "max": {"x": 3855, "y": 2175}},
      "display_window": {"min": {"x": 0,   "y": 0  }, "max": {"x": 3839, "y": 2159}},
      "pixel_aspect_ratio": 1.0,
      "line_order": "increasingY",
      "compression": "dwab",
      "color_space": "ACEScg",
      "render_software": "Arnold 7.3.1.1 (Maya 2025)",
      "is_tiled": true, "tile_width": 256, "tile_height": 256, "tile_depth": 1,
      "is_deep": false
    },
    {
      "part_index": 1,
      "width": 3840, "height": 2160,
      "part_name": "crypto_object",
      "compression": "zips",
      "is_tiled": true, "tile_width": 256, "tile_height": 256
    },
    {
      "part_index": 2,
      "width": 1920, "height": 1080,
      "part_name": "denoise_normals",
      "compression": "dwab"
    }
  ],
  "channels": [
    {"part_index": 0, "name": "R", "layer_name": "",  "component_name": "R", "type": "HALF", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "G", "layer_name": "",  "component_name": "G", "type": "HALF", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "B", "layer_name": "",  "component_name": "B", "type": "HALF", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "A", "layer_name": "",  "component_name": "A", "type": "HALF", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 1, "name": "crypto_object00.R", "layer_name": "crypto_object00", "component_name": "R", "type": "FLOAT", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 1, "name": "crypto_object00.G", "layer_name": "crypto_object00", "component_name": "G", "type": "FLOAT", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 2, "name": "N.x", "layer_name": "N", "component_name": "x", "type": "HALF",  "x_sampling": 1, "y_sampling": 1},
    {"part_index": 2, "name": "N.y", "layer_name": "N", "component_name": "y", "type": "HALF",  "x_sampling": 1, "y_sampling": 1},
    {"part_index": 2, "name": "N.z", "layer_name": "N", "component_name": "z", "type": "HALF",  "x_sampling": 1, "y_sampling": 1}
  ],
  "aovs": [
    {
      "name": "beauty", "part_index": 0, "view": null,
      "components": ["R","G","B","A"], "channel_group": "RGBA", "channel_count": 4,
      "data_type": "HALF", "bit_depth": 16, "depth_label": "16f",
      "category": "beauty", "is_beauty": true, "is_data": false,
      "color_space": "ACEScg", "light_group": null, "synthetic": false,
      "uncompressed_bytes": 66355200, "pct_of_frame_logical": 25.0
    },
    {
      "name": "uCryptoObject", "part_index": 1, "view": null,
      "components": ["R","G","B","A"], "channel_group": "RGBA", "channel_count": 8,
      "data_type": "FLOAT", "bit_depth": 32, "depth_label": "32f",
      "category": "crypto", "is_beauty": false, "is_data": true,
      "color_space": "raw", "light_group": null, "synthetic": false,
      "uncompressed_bytes": 265420800, "pct_of_frame_logical": 50.0,
      "ranks": 2
    },
    {
      "name": "N", "part_index": 2, "view": null,
      "components": ["X","Y","Z"], "channel_group": "XYZ", "channel_count": 3,
      "data_type": "HALF", "bit_depth": 16, "depth_label": "16f",
      "category": "data", "is_beauty": false, "is_data": true,
      "color_space": "raw", "light_group": null, "synthetic": false,
      "uncompressed_bytes": 12441600, "pct_of_frame_logical": 25.0
    }
  ],
  "attributes": {
    "parts": [
      [
        {"name": "chromaticities", "type": "FLOAT[8]", "value": [0.713, 0.293, 0.165, 0.83, 0.128, 0.044, 0.32168, 0.33767]},
        {"name": "screenWindowCenter", "type": "V2F", "value": {"x": 0.0, "y": 0.0}},
        {"name": "screenWindowWidth",  "type": "FLOAT", "value": 1.0},
        {"name": "adoptedNeutral",     "type": "V2F", "value": {"x": 0.32168, "y": 0.33767}},
        {"name": "renderingTransform", "type": "STRING", "value": "ACES 1.3 / Rec.709"},
        {"name": "smpte:TimeCode", "type": "TIMECODE", "value": {"time": 84210000, "user": 0}},
        {"name": "framesPerSecond", "type": "RATIONAL", "value": {"num": 24000, "den": 1001}},
        {"name": "owner", "type": "STRING", "value": "artist1"},
        {"name": "comments", "type": "STRING", "value": "v012 - neutral + denoise final"},
        {"name": "capDate", "type": "STRING", "value": "2026:04:14 18:42:03"}
      ],
      [],
      []
    ]
  },
  "color": {
    "color_space": "ACEScg",
    "primaries":   [0.713, 0.293, 0.165, 0.83, 0.128, 0.044, 0.32168, 0.33767]
  },
  "timecode": {"value": {"time": 84210000, "user": 0}, "rate": 23.976},
  "sequence": {"frame_number": 42},
  "camera":   {},
  "production": {
    "creator": "artist1",
    "software": "Arnold 7.3.1.1 (Maya 2025)",
    "description": "v012 - neutral + denoise final"
  },
  "extraction": {
    "tool": "frame-metadata-extractor",
    "tool_version": "0.2.0",
    "timestamp": "2026-04-14T22:41:05.812000+00:00",
    "warnings": []
  },
  "errors": [],
  "persistence": {
    "status": "success",
    "file_id": "8a2f11c9d4b60e71",
    "inserted": true,
    "message": "File persisted: 8a2f11c9d4b60e71"
  }
}
```

### DPX — 10-bit log film scan

DPX is where the custom SMPTE 268M parser earns its keep — every
`attributes.parts[0]` entry prefixed `dpx:` comes from the raw 2048-byte
struct-unpack in `dpx_header.py`, not from OIIO.

```json
{
  "schema_version": 1,
  "file": {
    "path": "scans/ER-0042/roll12/take03.000185.dpx",
    "format": "dpx",
    "size_bytes": 52428800,
    "mtime": "2026-04-14T11:02:17.000000+00:00",
    "frame_number": 185,
    "multipart_count": 1,
    "is_deep": false
  },
  "parts": [{
    "part_index": 0,
    "width": 4096, "height": 3112,
    "display_width": 4096, "display_height": 3112,
    "pixel_aspect_ratio": 1.0,
    "compression": "none",
    "color_space": "KodakLog",
    "is_tiled": false,
    "is_deep": false
  }],
  "channels": [
    {"part_index": 0, "name": "R", "layer_name": "", "component_name": "R", "type": "UINT16", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "G", "layer_name": "", "component_name": "G", "type": "UINT16", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "B", "layer_name": "", "component_name": "B", "type": "UINT16", "x_sampling": 1, "y_sampling": 1}
  ],
  "attributes": {
    "parts": [[
      {"name": "oiio:BitsPerSample",         "type": "INT",    "value": 10},
      {"name": "dpx:byte_order",             "type": "STRING", "value": "big"},
      {"name": "dpx:header_version",         "type": "STRING", "value": "V2.0"},
      {"name": "dpx:file_size",              "type": "INT",    "value": 52428800},
      {"name": "dpx:offset_to_image",        "type": "INT",    "value": 8192},
      {"name": "dpx:creator",                "type": "STRING", "value": "ARRISCAN XT / 4.2.8"},
      {"name": "dpx:project",                "type": "STRING", "value": "ER - Episode 0042 - Reel 12"},
      {"name": "dpx:copyright",              "type": "STRING", "value": "(c) 2026 Octo Features"},
      {"name": "dpx:creation_time",          "type": "STRING", "value": "2026:04:14:11:02:17LTC"},
      {"name": "dpx:descriptor_code",        "type": "INT",    "value": 50},
      {"name": "dpx:descriptor_name",        "type": "STRING", "value": "rgb"},
      {"name": "dpx:transfer_code",          "type": "INT",    "value": 3},
      {"name": "dpx:transfer_name",          "type": "STRING", "value": "logarithmic"},
      {"name": "dpx:colorimetric_code",      "type": "INT",    "value": 1},
      {"name": "dpx:colorimetric_name",      "type": "STRING", "value": "printing-density"},
      {"name": "dpx:bit_size",               "type": "INT",    "value": 10},
      {"name": "dpx:packing",                "type": "INT",    "value": 1},
      {"name": "dpx:encoding",               "type": "INT",    "value": 0},
      {"name": "dpx:source_x_original_size", "type": "INT",    "value": 4096},
      {"name": "dpx:source_y_original_size", "type": "INT",    "value": 3112},
      {"name": "dpx:input_device",           "type": "STRING", "value": "ARRISCAN XT"},
      {"name": "dpx:input_device_serial",    "type": "STRING", "value": "AX-1047"},
      {"name": "dpx:film_mfg_id",            "type": "STRING", "value": "10"},
      {"name": "dpx:film_type",              "type": "STRING", "value": "04"},
      {"name": "dpx:film_prefix",            "type": "STRING", "value": "KU1234"},
      {"name": "dpx:film_count",             "type": "STRING", "value": "5678"},
      {"name": "dpx:film_format",            "type": "STRING", "value": "Super 35mm 4-perf"},
      {"name": "dpx:frame_position",         "type": "INT",    "value": 185},
      {"name": "dpx:sequence_length",        "type": "INT",    "value": 1440},
      {"name": "dpx:film_frame_rate",        "type": "FLOAT",  "value": 23.976},
      {"name": "dpx:shutter_angle",          "type": "FLOAT",  "value": 172.8},
      {"name": "dpx:frame_id",               "type": "STRING", "value": "ER_0042_r12_t03_0185"},
      {"name": "dpx:slate_info",             "type": "STRING", "value": "ER 0042 / Scene 14B / Take 3"},
      {"name": "dpx:timecode",               "type": "STRING", "value": "01:00:07:17"},
      {"name": "dpx:frame_rate",             "type": "FLOAT",  "value": 23.976},
      {"name": "dpx:gamma",                  "type": "FLOAT",  "value": 1.0},
      {"name": "dpx:black_level_code",       "type": "FLOAT",  "value": 95.0},
      {"name": "dpx:white_level_code",       "type": "FLOAT",  "value": 685.0}
    ]]
  },
  "color": {"color_space": "KodakLog"},
  "timecode": {"value": "01:00:07:17", "rate": 23.976},
  "sequence": {"frame_number": 185},
  "camera":   {},
  "production": {
    "creator": "ARRISCAN XT / 4.2.8",
    "project": "ER - Episode 0042 - Reel 12"
  },
  "extraction": {
    "tool": "frame-metadata-extractor",
    "tool_version": "0.2.0",
    "timestamp": "2026-04-14T22:41:05.812000+00:00",
    "warnings": []
  },
  "errors": []
}
```

### TIFF — 16-bit scan with EXIF + ICC

```json
{
  "schema_version": 1,
  "file": {
    "path": "scans/set/hero_closeup.0003.tif",
    "format": "tiff",
    "size_bytes": 41943040,
    "mtime": "2026-04-14T09:15:02.000000+00:00",
    "frame_number": 3,
    "multipart_count": 1,
    "is_deep": false
  },
  "parts": [{
    "part_index": 0,
    "width": 4096, "height": 3072,
    "pixel_aspect_ratio": 1.0,
    "compression": "deflate",
    "color_space": "Rec709",
    "render_software": "Adobe Photoshop 2026",
    "is_tiled": false,
    "is_deep": false
  }],
  "channels": [
    {"part_index": 0, "name": "R", "layer_name": "", "component_name": "R", "type": "UINT16", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "G", "layer_name": "", "component_name": "G", "type": "UINT16", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "B", "layer_name": "", "component_name": "B", "type": "UINT16", "x_sampling": 1, "y_sampling": 1}
  ],
  "attributes": {
    "parts": [[
      {"name": "oiio:BitsPerSample",       "type": "INT",    "value": 16},
      {"name": "PhotometricInterpretation","type": "INT",    "value": 2},
      {"name": "PlanarConfiguration",      "type": "INT",    "value": 1},
      {"name": "XResolution",              "type": "FLOAT",  "value": 300.0},
      {"name": "YResolution",              "type": "FLOAT",  "value": 300.0},
      {"name": "ResolutionUnit",           "type": "INT",    "value": 2},
      {"name": "Make",                     "type": "STRING", "value": "Phase One"},
      {"name": "Model",                    "type": "STRING", "value": "IQ4 150MP"},
      {"name": "LensModel",                "type": "STRING", "value": "Schneider LS 80mm f/2.8"},
      {"name": "ExposureTime",             "type": "FLOAT",  "value": 0.008},
      {"name": "FNumber",                  "type": "FLOAT",  "value": 5.6},
      {"name": "Exif:PhotographicSensitivity", "type": "INT", "value": 100},
      {"name": "DateTime",                 "type": "STRING", "value": "2026:04:14 09:15:02"},
      {"name": "ICCProfile",               "type": "UINT8[3144]", "value": {"encoding": "base64", "data": "AAAMSGxjbXMEMAAAbW50clJHQiBYWVog..."}},
      {"name": "ImageDescription",         "type": "STRING", "value": "Set reference - hero prop closeup"},
      {"name": "Artist",                   "type": "STRING", "value": "J. Aragon"},
      {"name": "Copyright",                "type": "STRING", "value": "(c) 2026 Octo Features"},
      {"name": "Software",                 "type": "STRING", "value": "Adobe Photoshop 2026"}
    ]]
  },
  "color":    {"color_space": "Rec709"},
  "timecode": {},
  "sequence": {"frame_number": 3},
  "camera": {
    "make": "Phase One", "model": "IQ4 150MP",
    "lens": "Schneider LS 80mm f/2.8",
    "exposure": 0.008, "fnumber": 5.6, "iso": 100
  },
  "production": {
    "creator": "J. Aragon",
    "copyright": "(c) 2026 Octo Features",
    "description": "Set reference - hero prop closeup",
    "software": "Adobe Photoshop 2026"
  },
  "extraction": {"tool": "frame-metadata-extractor", "tool_version": "0.2.0", "timestamp": "2026-04-14T22:41:05.812000+00:00", "warnings": []},
  "errors": []
}
```

### PNG — review frame with tEXt

```json
{
  "schema_version": 1,
  "file": {
    "path": "dailies/2026-04-14/shot_010_v012.1042.png",
    "format": "png",
    "size_bytes": 8388608,
    "frame_number": 1042,
    "multipart_count": 1, "is_deep": false
  },
  "parts": [{
    "part_index": 0,
    "width": 1920, "height": 1080,
    "compression": "zip",
    "color_space": "sRGB"
  }],
  "channels": [
    {"part_index": 0, "name": "R", "layer_name": "", "component_name": "R", "type": "UINT8", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "G", "layer_name": "", "component_name": "G", "type": "UINT8", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "B", "layer_name": "", "component_name": "B", "type": "UINT8", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "A", "layer_name": "", "component_name": "A", "type": "UINT8", "x_sampling": 1, "y_sampling": 1}
  ],
  "attributes": {
    "parts": [[
      {"name": "oiio:BitsPerSample", "type": "INT",    "value": 8},
      {"name": "oiio:ColorSpace",    "type": "STRING", "value": "sRGB"},
      {"name": "Gamma",              "type": "FLOAT",  "value": 2.2},
      {"name": "png:bitdepth",       "type": "INT",    "value": 8},
      {"name": "png:interlace_type", "type": "INT",    "value": 0},
      {"name": "Software",           "type": "STRING", "value": "Nuke 15.1v5"},
      {"name": "Artist",             "type": "STRING", "value": "artist1"},
      {"name": "ImageDescription",   "type": "STRING", "value": "shot 010 comp v012 - dailies"},
      {"name": "DateTime",           "type": "STRING", "value": "2026:04:14 20:11:34"},
      {"name": "shot",               "type": "STRING", "value": "010"},
      {"name": "version",            "type": "STRING", "value": "v012"}
    ]]
  },
  "color":    {"color_space": "sRGB"},
  "timecode": {},
  "sequence": {"frame_number": 1042},
  "camera":   {},
  "production": {"creator": "artist1", "description": "shot 010 comp v012 - dailies", "software": "Nuke 15.1v5"},
  "extraction": {"tool": "frame-metadata-extractor", "tool_version": "0.2.0", "timestamp": "2026-04-14T22:41:05.812000+00:00", "warnings": []},
  "errors": []
}
```

### TGA — legacy hero frame with developer area

```json
{
  "schema_version": 1,
  "file": {
    "path": "matte_paintings/skydome_v004.tga",
    "format": "targa",
    "size_bytes": 12582912,
    "frame_number": null,
    "multipart_count": 1, "is_deep": false
  },
  "parts": [{
    "part_index": 0,
    "width": 2048, "height": 1024,
    "compression": "rle"
  }],
  "channels": [
    {"part_index": 0, "name": "R", "layer_name": "", "component_name": "R", "type": "UINT8", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "G", "layer_name": "", "component_name": "G", "type": "UINT8", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "B", "layer_name": "", "component_name": "B", "type": "UINT8", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "A", "layer_name": "", "component_name": "A", "type": "UINT8", "x_sampling": 1, "y_sampling": 1}
  ],
  "attributes": {
    "parts": [[
      {"name": "targa:ImageType",     "type": "INT",    "value": 10},
      {"name": "targa:versionNumber", "type": "INT",    "value": 2},
      {"name": "oiio:BitsPerSample",  "type": "INT",    "value": 8},
      {"name": "Software",            "type": "STRING", "value": "Photoshop / TargaExporter 1.4"},
      {"name": "ImageDescription",    "type": "STRING", "value": "skydome_v004 // shot 020"}
    ]]
  },
  "color":    {},
  "timecode": {},
  "sequence": {},
  "camera":   {},
  "production": {"description": "skydome_v004 // shot 020", "software": "Photoshop / TargaExporter 1.4"},
  "extraction": {
    "tool": "frame-metadata-extractor", "tool_version": "0.2.0",
    "timestamp": "2026-04-14T22:41:05.812000+00:00",
    "warnings": ["TGA developer area offset beyond header window - field omitted"]
  },
  "errors": []
}
```

### HDR / RGBE — environment capture for IBL

```json
{
  "schema_version": 1,
  "file": {
    "path": "lighting/ibl/studio_beach_morning.hdr",
    "format": "rgbe",
    "size_bytes": 16777216,
    "frame_number": null,
    "multipart_count": 1, "is_deep": false
  },
  "parts": [{"part_index": 0, "width": 8192, "height": 4096, "compression": "rle"}],
  "channels": [
    {"part_index": 0, "name": "R", "layer_name": "", "component_name": "R", "type": "FLOAT", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "G", "layer_name": "", "component_name": "G", "type": "FLOAT", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "B", "layer_name": "", "component_name": "B", "type": "FLOAT", "x_sampling": 1, "y_sampling": 1}
  ],
  "attributes": {
    "parts": [[
      {"name": "hdr:FORMAT",    "type": "STRING", "value": "32-bit_rle_rgbe"},
      {"name": "hdr:EXPOSURE",  "type": "FLOAT",  "value": 1.0},
      {"name": "hdr:SOFTWARE",  "type": "STRING", "value": "PTGui Pro 13"},
      {"name": "hdr:PRIMARIES", "type": "STRING", "value": "0.640 0.330 0.290 0.600 0.150 0.060 0.333 0.333"},
      {"name": "oiio:ColorSpace", "type": "STRING", "value": "linear"}
    ]]
  },
  "color":    {"color_space": "linear"},
  "timecode": {},
  "sequence": {},
  "camera":   {},
  "production": {"software": "PTGui Pro 13"},
  "extraction": {"tool": "frame-metadata-extractor", "tool_version": "0.2.0", "timestamp": "2026-04-14T22:41:05.812000+00:00", "warnings": []},
  "errors": []
}
```

### JPEG2000 — DCI 2K master frame

```json
{
  "schema_version": 1,
  "file": {
    "path": "dcp/FEATURE_FTR_F_EN-XX_US_51_2K_OC_20260414/video/000512.j2c",
    "format": "jpeg2000",
    "size_bytes": 5242880,
    "frame_number": 512,
    "multipart_count": 1, "is_deep": false
  },
  "parts": [{
    "part_index": 0,
    "width": 2048, "height": 858,
    "pixel_aspect_ratio": 1.0,
    "compression": "jpeg2000",
    "color_space": "XYZ"
  }],
  "channels": [
    {"part_index": 0, "name": "X", "layer_name": "", "component_name": "X", "type": "UINT16", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "Y", "layer_name": "", "component_name": "Y", "type": "UINT16", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "Z", "layer_name": "", "component_name": "Z", "type": "UINT16", "x_sampling": 1, "y_sampling": 1}
  ],
  "attributes": {
    "parts": [[
      {"name": "oiio:BitsPerSample",           "type": "INT",    "value": 12},
      {"name": "jpeg2000:CompressionName",     "type": "STRING", "value": "JPEG 2000"},
      {"name": "jpeg2000:NumberOfComponents",  "type": "INT",    "value": 3},
      {"name": "jpeg2000:NumberOfDecompositionLevels", "type": "INT", "value": 5},
      {"name": "jpeg2000:ProgressionOrder",    "type": "STRING", "value": "CPRL"},
      {"name": "jpeg2000:TileWidth",           "type": "INT",    "value": 2048},
      {"name": "jpeg2000:TileHeight",          "type": "INT",    "value": 858}
    ]]
  },
  "color":    {"color_space": "XYZ"},
  "timecode": {},
  "sequence": {"frame_number": 512},
  "camera":   {},
  "production": {},
  "extraction": {"tool": "frame-metadata-extractor", "tool_version": "0.2.0", "timestamp": "2026-04-14T22:41:05.812000+00:00", "warnings": []},
  "errors": []
}
```

### Cineon — legacy scan

Handled through the generic OIIO path. Shape is the same as DPX but
without the custom SMPTE 268M parser, so film/tv fields are limited to
what OIIO exposes.

```json
{
  "schema_version": 1,
  "file": {
    "path": "archive/1998/tng_reel04.000120.cin",
    "format": "cineon",
    "size_bytes": 47185920,
    "frame_number": 120,
    "multipart_count": 1, "is_deep": false
  },
  "parts": [{
    "part_index": 0, "width": 3656, "height": 2664,
    "compression": "none",
    "color_space": "KodakLog"
  }],
  "channels": [
    {"part_index": 0, "name": "R", "layer_name": "", "component_name": "R", "type": "UINT16", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "G", "layer_name": "", "component_name": "G", "type": "UINT16", "x_sampling": 1, "y_sampling": 1},
    {"part_index": 0, "name": "B", "layer_name": "", "component_name": "B", "type": "UINT16", "x_sampling": 1, "y_sampling": 1}
  ],
  "attributes": {
    "parts": [[
      {"name": "oiio:BitsPerSample",   "type": "INT",    "value": 10},
      {"name": "cineon:Creator",       "type": "STRING", "value": "Kodak Cineon Film Scanner"},
      {"name": "cineon:Project",       "type": "STRING", "value": "Reel 04"}
    ]]
  },
  "color":    {"color_space": "KodakLog"},
  "timecode": {},
  "sequence": {"frame_number": 120},
  "camera":   {},
  "production": {"creator": "Kodak Cineon Film Scanner", "project": "Reel 04"},
  "extraction": {"tool": "frame-metadata-extractor", "tool_version": "0.2.0", "timestamp": "2026-04-14T22:41:05.812000+00:00", "warnings": []},
  "errors": []
}
```

---

## How it lands in VastDB

Each payload fans out to **five** tables in the `frame_metadata` schema
under the bucket configured by `VAST_DB_BUCKET` (default `frame-data`).
All five inserts run in a single VastDB transaction per event, so AOV /
channel rows can never become orphans of a missing files row.

| Table | Rows per file | Key columns |
|---|---|---|
| `files` | 1 | `file_id`, `file_path`, `file_path_normalized`, `format`, `header_hash`, `size_bytes`, `mtime`, `multipart_count`, `is_deep`, **`metadata_embedding`** (384-dim float32), `frame_number`, `inspection_timestamp`, `inspection_count`, `last_inspected` |
| `parts` | 1 per subimage (usually 1; N for multi-part EXR) | `file_id`, `file_path`, `part_index`, `width`, `height`, `display_width`, `display_height`, `data_x_offset`, `data_y_offset`, `part_name`, `view_name`, `multi_view`, `data_window` (JSON string), `display_window` (JSON string), `pixel_aspect_ratio`, `line_order`, `compression`, `color_space`, `render_software`, `is_tiled`, `tile_width`, `tile_height`, `tile_depth`, `is_deep` |
| `channels` | 1 per channel | `file_id`, `file_path`, `part_index`, `channel_name`, `layer_name`, `component_name`, `channel_type`, `x_sampling`, `y_sampling`, **`channel_fingerprint`** (128-dim float32, written on the first row only; zero-filled on the rest to avoid per-channel duplication) |
| `attributes` | 1 per attribute per part | `file_id`, `file_path`, `part_index`, `attr_name`, `attr_type`, `value_json`, `value_text`, `value_int`, `value_float` |
| `aovs` | 1 per AOV per part per view | `file_id`, `file_path`, `frame_number`, `aov_index`, `part_index`, `view`, `name`, `name_normalized`, `channel_group`, `components`, `channel_count`, `data_type`, `bit_depth`, `depth_label`, `category`, `is_beauty`, `is_data`, `color_space`, `light_group`, `synthetic`, `uncompressed_bytes`, `pct_of_frame_logical`, `ranks` |

The `files.format` column is the single new addition versus the
`exr_inspector` schema — it lets downstream consumers filter by container
type without re-parsing paths.

### Example queries enabled by this layout

*Find every DPX frame from film manufacturer 10 shot at 23.976 fps*:

```sql
SELECT f.file_path, f.frame_number
FROM files f
JOIN attributes a ON a.file_id = f.file_id
WHERE f.format = 'dpx'
  AND a.attr_name = 'dpx:film_mfg_id' AND a.value_text = '10'
  AND EXISTS (
    SELECT 1 FROM attributes a2
    WHERE a2.file_id = f.file_id
      AND a2.attr_name = 'dpx:frame_rate'
      AND a2.value_float BETWEEN 23.9 AND 24.0
  );
```

*Find frames with channel layouts similar to a reference file* (via
`channels.channel_fingerprint` vector similarity — syntax will depend on
the vastdb vector search API).

*Find every file across all formats tagged with a specific shot name* by
querying `attributes` for `attr_name = 'shot'`.

*Render the AOV layer map for a shot* (sequence-level rollup, the panel
header "N LAYERS · MULTI-CHANNEL EXR" is `COUNT(*)` over this result):

```sql
SELECT a.name,
       ANY_VALUE(a.channel_group)        AS channels,
       ANY_VALUE(a.depth_label)          AS depth,
       SUM(a.uncompressed_bytes)         AS logical_bytes,
       COUNT(DISTINCT a.frame_number)    AS frame_count,
       COUNT(DISTINCT a.depth_label)     AS depth_variants,   -- > 1 = drift
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

The dedup subquery (`MAX(mtime) GROUP BY file_path_normalized`) ensures
re-rendered frames don't double-count.

---

## Schema version

This document describes `schema_version: 1`. Bumps will be recorded at the
top of this file and in the extractor's `__version__` constant.
