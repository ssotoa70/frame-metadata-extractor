# frame-metadata-extractor

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12-green.svg)](https://www.python.org/downloads/)
[![VAST DataEngine](https://img.shields.io/badge/VAST-DataEngine-blue.svg)](https://www.vastdata.com/)
[![OpenImageIO](https://img.shields.io/badge/OpenImageIO-3.x-orange.svg)](https://openimageio.readthedocs.io/)
[![VAST DataBase](https://img.shields.io/badge/VAST-DataBase-green.svg)](https://www.vastdata.com/)

Multi-format VFX frame metadata extraction for VAST DataEngine. Triggered
by Element events on VAST S3 views, range-reads frame headers (no full
download), parses metadata across **8 still-frame formats** (OpenEXR,
DPX, Cineon, TIFF, PNG, TGA, HDR/RGBE, JPEG2000), groups channels into
renderer-meaningful AOVs, and persists to VAST DataBase.

## 📚 Documentation

**All documentation lives in the [Wiki](https://github.com/ssotoa70/frame-metadata-extractor/wiki).**

| Page | What it covers |
|---|---|
| [Home](https://github.com/ssotoa70/frame-metadata-extractor/wiki) | Project overview, supported formats, output schema, quick start |
| [Architecture](https://github.com/ssotoa70/frame-metadata-extractor/wiki/Architecture) | Event flow, format dispatch, AOV grouping logic |
| [Configuration](https://github.com/ssotoa70/frame-metadata-extractor/wiki/Configuration) | Environment variables, secrets pattern, tuning constants |
| [Database Schema](https://github.com/ssotoa70/frame-metadata-extractor/wiki/Database-Schema) | All five VastDB tables — columns, types, query patterns |
| [Deployment Guide](https://github.com/ssotoa70/frame-metadata-extractor/wiki/Deployment-Guide) | Build, push, deploy, verify, update workflows |
| [Performance Optimization](https://github.com/ssotoa70/frame-metadata-extractor/wiki/Performance-Optimization) | Resource sizing, autoscaling, bottlenecks, tuning tips |
| [Troubleshooting](https://github.com/ssotoa70/frame-metadata-extractor/wiki/Troubleshooting) | Build / deploy / runtime failure modes and fixes |

Field-by-field schema reference and per-format JSON sample payloads:
[`docs/FRAMES_METADATA_SCHEMA.MD`](docs/FRAMES_METADATA_SCHEMA.MD).
Machine-readable JSON Schema (draft 2020-12):
[`docs/frame_metadata.schema.json`](docs/frame_metadata.schema.json).

## Repository Layout

```
frame-metadata-extractor/
├── Dockerfile.fix                       # CNB launcher fix (LD_LIBRARY_PATH, entrypoint)
├── docs/                                # In-repo schema reference (deep)
│   ├── FRAMES_METADATA_SCHEMA.MD        # Field reference + per-format sample payloads
│   └── frame_metadata.schema.json       # JSON Schema (draft 2020-12)
└── functions/frame_metadata_extractor/
    ├── main.py                          # init() + handler() + AOV extraction
    ├── dpx_header.py                    # SMPTE 268M raw parser
    ├── vast_db_persistence.py           # VAST DataBase writes (5 tables)
    ├── requirements.txt
    └── Aptfile                          # OIIO + per-format codecs
```

The Wiki and the in-repo schema reference are intentionally split:

- **Wiki** — operator-facing prose (architecture, ops, troubleshooting)
- **`docs/`** — reference material that lives next to the code it
  describes (machine-readable schema + sample payloads)

## License

[MIT](LICENSE)
