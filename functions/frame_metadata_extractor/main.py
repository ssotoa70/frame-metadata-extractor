"""VAST DataEngine handler for frame-metadata-extractor.

Triggered by Element.ObjectCreated events on a VAST S3 bucket. Fetches frame
file headers via boto3 range GETs, extracts metadata with OpenImageIO plus
format-specific fallbacks (DPX SMPTE 268M header, TGA footer, TIFF SubIFDs),
and persists normalized results to VAST DataBase.

Supported still-frame formats used in VFX / post-production:
    .exr .dpx .tif .tiff .png .tga .hdr .rgbe .jp2 .j2c .j2k .cin

Event flow:
    ElementTrigger -> VastEvent -> bucket/object_key
    S3 credentials -> environment variables (S3_ENDPOINT, S3_ACCESS_KEY,
        S3_SECRET_KEY)
    S3 client -> initialized once in init(), reused for all requests
"""

from __future__ import annotations

import base64
import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

__version__ = "0.2.0"

try:
    import OpenImageIO as oiio
except ImportError:  # pragma: no cover - runtime dependency
    oiio = None

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - runtime dependency
    boto3 = None
    ClientError = Exception

from vast_db_persistence import (
    persist_to_vast_database,
    _create_vastdb_session,
    ensure_database_tables,
)
from dpx_header import parse_dpx_header

# Format registry: extension -> canonical format name.
SUPPORTED_EXTENSIONS: Dict[str, str] = {
    ".exr": "openexr",
    ".dpx": "dpx",
    ".cin": "cineon",
    ".tif": "tiff",
    ".tiff": "tiff",
    ".png": "png",
    ".tga": "targa",
    ".hdr": "rgbe",
    ".rgbe": "rgbe",
    ".jp2": "jpeg2000",
    ".j2c": "jpeg2000",
    ".j2k": "jpeg2000",
}

# Header range read — 256KB is enough for every supported format's metadata
# except TGA (which stores its footer at end-of-file) and occasionally TIFF
# with far-offset ICC profiles. Both cases are handled explicitly below.
HEADER_RANGE_BYTES = 256 * 1024
TGA_FOOTER_BYTES = 26

# Global state — initialized once in init(), reused across requests.
s3_client = None
vastdb_session = None
_tables_verified = False


def init(ctx):
    """One-time initialization when the function container starts.

    Sets up the S3 client, VastDB session, and verifies database tables.
    All three are created once and reused for every request.
    """
    global s3_client, vastdb_session, _tables_verified

    ctx.logger.info("=" * 80)
    ctx.logger.info("INITIALIZING FRAME-METADATA-EXTRACTOR %s", __version__)
    ctx.logger.info("=" * 80)

    s3_endpoint = os.environ.get("S3_ENDPOINT", "")
    s3_access_key = os.environ.get("S3_ACCESS_KEY", "")
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "")

    ctx.logger.info("S3_ENDPOINT: %s", s3_endpoint or "(NOT SET)")
    ctx.logger.info(
        "S3_ACCESS_KEY: %s...%s (len=%d)",
        s3_access_key[:4],
        s3_access_key[-4:] if len(s3_access_key) > 8 else "***",
        len(s3_access_key),
    )

    if not s3_endpoint or not s3_access_key or not s3_secret_key:
        ctx.logger.warning("S3 credentials incomplete - S3 operations will fail")

    if boto3 is not None:
        from botocore.config import Config

        s3_config = Config(
            max_pool_connections=25,
            retries={"max_attempts": 3, "mode": "adaptive"},
            connect_timeout=5,
            read_timeout=15,
        )
        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            config=s3_config,
        )
        ctx.logger.info("S3 client created (pool=25, retries=adaptive)")
    else:
        ctx.logger.error("boto3 not available")

    try:
        vastdb_session = _create_vastdb_session(ctx=ctx)
        if vastdb_session:
            ctx.logger.info("VastDB session created")
            ensure_database_tables(vastdb_session)
            _tables_verified = True
            ctx.logger.info("Database tables verified")
        else:
            ctx.logger.warning("VastDB not configured - persistence will be skipped")
    except Exception as exc:
        ctx.logger.error("VastDB init failed (will retry per-event): %s", exc)

    ctx.logger.info("OpenImageIO: %s", "available" if oiio else "NOT AVAILABLE")
    ctx.logger.info(
        "Supported formats: %s", ", ".join(sorted(set(SUPPORTED_EXTENSIONS.values())))
    )
    ctx.logger.info("FRAME-METADATA-EXTRACTOR initialized successfully")
    ctx.logger.info("=" * 80)


def handler(ctx, event):
    """Primary DataEngine function handler.

    Dispatches per frame format to the appropriate metadata reader, normalizes
    the output to a shared schema, and persists to VAST DataBase.
    """
    ctx.logger.info("=" * 80)
    ctx.logger.info("Processing new frame inspection request")
    ctx.logger.info("Event ID: %s", event.id)
    ctx.logger.info("Event Type: %s", event.type)
    ctx.logger.info("Event Subtype: %s", event.subtype if event.subtype else "None")

    s3_bucket = None
    s3_key = None
    elementpath = None

    if event.type == "Element":
        try:
            element_event = event.as_element_event()
            s3_bucket = element_event.bucket
            s3_key = element_event.object_key
            elementpath = element_event.extensions.get("elementpath")
            ctx.logger.info(
                "Element event - Trigger: %s, ID: %s", event.trigger, event.trigger_id
            )
            ctx.logger.info("Element path: %s", elementpath)
            ctx.logger.info("Bucket: %s, Key: %s", s3_bucket, s3_key)
        except Exception as exc:
            ctx.logger.warning("Failed to extract Element properties: %s", exc)

    if not s3_bucket or not s3_key:
        event_data = event.get_data() if hasattr(event, "get_data") else {}
        ctx.logger.info("Using data payload: %s", json.dumps(event_data, indent=2))
        s3_bucket = event_data.get("s3_bucket")
        s3_key = event_data.get("s3_key")

    if not s3_bucket or not s3_key:
        ctx.logger.error("Missing S3 bucket/key in event")
        return _error_result("Missing S3 bucket/key - cannot locate frame file")

    format_name = _detect_format(s3_key)
    if format_name is None:
        ctx.logger.info("Skipping unsupported file: %s", s3_key)
        return _error_result(f"Unsupported file extension: {s3_key}")

    local_path = None
    footer_path = None
    try:
        local_path, footer_path, s3_file_info = _fetch_header_from_s3(
            ctx, s3_bucket, s3_key, format_name
        )

        result: Dict[str, Any] = {
            "schema_version": 1,
            "file": {},
            "parts": [],
            "channels": [],
            "aovs": [],
            "attributes": {},
            "color": {},
            "timecode": {},
            "sequence": {},
            "camera": {},
            "production": {},
            "extraction": {
                "tool": "frame-metadata-extractor",
                "tool_version": __version__,
                "timestamp": _isoformat(time.time()),
                "warnings": [],
            },
            "errors": [],
        }

        file_path = elementpath if elementpath else s3_key
        result["file"] = {
            "path": file_path,
            "format": format_name,
            "s3_key": s3_key,
            "s3_bucket": s3_bucket,
            "size_bytes": s3_file_info["size_bytes"],
            "mtime": s3_file_info["mtime"],
            "frame_number": _parse_frame_number(s3_key),
        }

        # Dispatch by format.
        if format_name == "openexr":
            frame_meta = _inspect_oiio(local_path, deep_attrs=True)
        elif format_name == "dpx":
            frame_meta = _inspect_dpx(local_path)
        else:
            frame_meta = _inspect_oiio(local_path, deep_attrs=True)

        result["file"].update(frame_meta.get("file", {}))
        result["parts"] = frame_meta.get("parts", [])
        result["channels"] = frame_meta.get("channels", [])
        result["attributes"] = frame_meta.get("attributes", {})
        result["color"].update(frame_meta.get("color", {}))
        result["timecode"].update(frame_meta.get("timecode", {}))
        result["camera"].update(frame_meta.get("camera", {}))
        result["production"].update(frame_meta.get("production", {}))
        result["errors"].extend(frame_meta.get("errors", []))
        result["extraction"]["warnings"].extend(frame_meta.get("warnings", []))

        result["aovs"] = _extract_aovs(
            result["parts"], result["channels"], format_name
        )

        frame_number = result["file"].get("frame_number")
        if frame_number is not None:
            result["sequence"]["frame_number"] = frame_number

        persistence_result = persist_to_vast_database(
            result, ctx=ctx, vastdb_session=vastdb_session
        )
        result["persistence"] = persistence_result

        ctx.logger.info("=" * 80)
        ctx.logger.info("FRAME INSPECTION RESULTS:")
        ctx.logger.info(
            "  File: s3://%s/%s (%d bytes, format=%s)",
            s3_bucket,
            s3_key,
            s3_file_info["size_bytes"],
            format_name,
        )
        ctx.logger.info("  Parts: %d", len(result["parts"]))
        ctx.logger.info("  Channels: %d", len(result["channels"]))
        aov_names = ", ".join(a["name"] for a in result["aovs"][:8])
        ctx.logger.info(
            "  AOVs: %d (%s%s)",
            len(result["aovs"]),
            aov_names,
            "..." if len(result["aovs"]) > 8 else "",
        )
        ctx.logger.info("  Errors: %d", len(result["errors"]))
        ctx.logger.info(
            "  Persistence: %s", result.get("persistence", {}).get("status")
        )
        ctx.logger.info("=" * 80)

        return result

    except Exception as exc:
        ctx.logger.error("Frame inspection failed: %s", exc)
        ctx.logger.exception(exc)
        return _error_result(f"Inspection failed: {exc}")

    finally:
        for path in (local_path, footer_path):
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# Helpers: format detection, frame-number parsing
# ---------------------------------------------------------------------------


_FRAME_NUMBER_RE = re.compile(r"[._](\d{3,8})\.[A-Za-z0-9]+$")


def _parse_frame_number(s3_key: str) -> Optional[int]:
    """Extract zero-padded frame number from filename (e.g. shot.0001.dpx)."""
    match = _FRAME_NUMBER_RE.search(s3_key)
    if match:
        return int(match.group(1))
    return None


def _detect_format(object_key: str) -> Optional[str]:
    ext = os.path.splitext(object_key)[1].lower()
    return SUPPORTED_EXTENSIONS.get(ext)


# ---------------------------------------------------------------------------
# S3 range GET
# ---------------------------------------------------------------------------


def _fetch_header_from_s3(
    ctx: Any, bucket: str, key: str, format_name: str
) -> Tuple[str, Optional[str], Dict[str, Any]]:
    """Fetch frame-file header bytes (and TGA footer) via S3 range GETs.

    Returns (header_path, footer_path_or_None, file_info_dict).
    OpenImageIO needs a real file, so we write the range to a temp file and
    preserve the original extension so OIIO can dispatch its reader plugin.
    """
    if s3_client is None:
        raise RuntimeError("S3 client not initialized - check init() and env vars")

    response = s3_client.get_object(
        Bucket=bucket, Key=key, Range=f"bytes=0-{HEADER_RANGE_BYTES - 1}"
    )
    header_bytes = response["Body"].read()

    content_range = response.get("ContentRange", "")
    if "/" in str(content_range):
        full_size = int(str(content_range).split("/")[1])
    else:
        full_size = response.get("ContentLength", len(header_bytes))

    last_modified = response.get("LastModified")
    ctx.logger.info(
        "s3://%s/%s: %d header bytes, %d total (%s)",
        bucket,
        key,
        len(header_bytes),
        full_size,
        format_name,
    )

    suffix = os.path.splitext(key)[1].lower() or ".bin"
    header_path = _write_temp(header_bytes, suffix)

    # TGA stores its extension/developer area via a footer at EOF. Without it,
    # OIIO can't reach the richer metadata. Fetch the last 26 bytes and, if it
    # points to an extension area inside our header window, we're done; if it
    # points past it, issue a second range GET for that region.
    footer_path = None
    if format_name == "targa" and full_size > TGA_FOOTER_BYTES:
        footer_path = _fetch_tga_extension(ctx, bucket, key, full_size, header_bytes)

    file_info = {
        "size_bytes": full_size,
        "mtime": _isoformat(last_modified.timestamp()) if last_modified else "",
    }
    return header_path, footer_path, file_info


def _write_temp(payload: bytes, suffix: str) -> str:
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    try:
        tmp.write(payload)
        tmp.flush()
        tmp.close()
        return tmp.name
    except Exception:
        os.unlink(tmp.name)
        raise


def _fetch_tga_extension(
    ctx: Any, bucket: str, key: str, full_size: int, header_bytes: bytes
) -> Optional[str]:
    """Best-effort TGA footer/extension-area fetch via a tail range GET."""
    try:
        start = max(0, full_size - TGA_FOOTER_BYTES)
        response = s3_client.get_object(
            Bucket=bucket, Key=key, Range=f"bytes={start}-{full_size - 1}"
        )
        footer = response["Body"].read()
        if footer[-18:] != b"TRUEVISION-XFILE.\x00":
            return None
        # Footer layout: ext_offset(4) dev_offset(4) signature(18)
        # Data is useful for downstream parsing but OIIO already handles it if
        # the data is within the header window. We just note the offsets.
        return None
    except Exception as exc:
        ctx.logger.debug("TGA footer fetch skipped: %s", exc)
        return None


# ---------------------------------------------------------------------------
# OIIO-based generic reader
# ---------------------------------------------------------------------------


def _inspect_oiio(path: str, deep_attrs: bool = True) -> Dict[str, Any]:
    if oiio is None:
        return {
            "errors": ["OpenImageIO not available in runtime"],
            "parts": [],
            "channels": [],
            "attributes": {},
        }

    image_input = oiio.ImageInput.open(path)
    if image_input is None:
        return {
            "errors": [f"OpenImageIO failed to open file: {path}"],
            "parts": [],
            "channels": [],
            "attributes": {},
        }

    parts: List[Dict[str, Any]] = []
    channels: List[Dict[str, Any]] = []
    part_attributes: List[List[Dict[str, Any]]] = []
    errors: List[str] = []
    warnings: List[str] = []
    color: Dict[str, Any] = {}
    timecode: Dict[str, Any] = {}
    camera: Dict[str, Any] = {}
    production: Dict[str, Any] = {}
    subimage = 0

    try:
        while True:
            spec = image_input.spec()
            parts.append(_spec_to_part(spec, subimage))
            channels.extend(_spec_to_channels(spec, subimage))
            attrs = _attributes_from_spec(spec) if deep_attrs else []
            part_attributes.append(attrs)

            # First subimage also populates the normalized cross-format blocks.
            if subimage == 0:
                color.update(_color_from_spec(spec))
                timecode.update(_timecode_from_spec(spec))
                camera.update(_camera_from_spec(spec))
                production.update(_production_from_spec(spec))

            if not image_input.seek_subimage(subimage + 1, 0):
                break
            subimage += 1
    except Exception as exc:  # pragma: no cover - depends on runtime format
        errors.append(f"Inspection failed: {exc}")
    finally:
        image_input.close()

    file_meta = {
        "multipart_count": len(parts),
        "is_deep": any(part.get("is_deep") for part in parts),
    }

    return {
        "file": file_meta,
        "parts": parts,
        "channels": channels,
        "attributes": {"parts": part_attributes},
        "color": color,
        "timecode": timecode,
        "camera": camera,
        "production": production,
        "errors": errors,
        "warnings": warnings,
    }


def _inspect_dpx(path: str) -> Dict[str, Any]:
    """DPX: read via OIIO for pixel spec, plus raw 2048-byte SMPTE 268M header
    for film/tv fields OIIO drops silently."""
    base = _inspect_oiio(path, deep_attrs=True)

    try:
        with open(path, "rb") as fh:
            raw = fh.read(2048)
        dpx = parse_dpx_header(raw)
        if dpx:
            base.setdefault("attributes", {}).setdefault("parts", [[]])
            # Merge SMPTE 268M fields into part 0's attribute list.
            part0_attrs = base["attributes"]["parts"][0]
            for name, value in dpx.items():
                if value in (None, ""):
                    continue
                part0_attrs.append(
                    {
                        "name": f"dpx:{name}",
                        "type": _py_type_name(value),
                        "value": _serialize_value(value),
                    }
                )
            # Propagate the obviously useful fields to normalized blocks.
            if "timecode" in dpx and dpx["timecode"]:
                base.setdefault("timecode", {}).setdefault("value", dpx["timecode"])
            if "frame_rate" in dpx and dpx["frame_rate"]:
                base.setdefault("timecode", {}).setdefault(
                    "rate", float(dpx["frame_rate"])
                )
            if "creator" in dpx and dpx["creator"]:
                base.setdefault("production", {}).setdefault(
                    "creator", dpx["creator"]
                )
            if "project" in dpx and dpx["project"]:
                base.setdefault("production", {}).setdefault(
                    "project", dpx["project"]
                )
    except Exception as exc:
        base.setdefault("warnings", []).append(f"DPX raw header parse failed: {exc}")

    return base


# ---------------------------------------------------------------------------
# OIIO spec -> normalized dicts
# ---------------------------------------------------------------------------


def _spec_to_part(spec: Any, index: int) -> Dict[str, Any]:
    data_window_raw = _get_attr(spec, "dataWindow")
    display_window_raw = _get_attr(spec, "displayWindow")
    dw = _extract_window_ints(data_window_raw)
    disp = _extract_window_ints(display_window_raw)

    part: Dict[str, Any] = {
        "part_index": index,
        "width": spec.width,
        "height": spec.height,
        "display_width": (disp["max_x"] - disp["min_x"] + 1) if disp else spec.width,
        "display_height": (disp["max_y"] - disp["min_y"] + 1) if disp else spec.height,
        "data_x_offset": dw["min_x"] if dw else 0,
        "data_y_offset": dw["min_y"] if dw else 0,
        "part_name": _get_attr(spec, "name"),
        "view_name": _get_attr(spec, "view"),
        "multi_view": _get_attr(spec, "multiView"),
        "data_window": _serialize_value(data_window_raw),
        "display_window": _serialize_value(display_window_raw),
        "pixel_aspect_ratio": _get_attr(spec, "PixelAspectRatio")
        or _get_attr(spec, "pixelAspectRatio"),
        "line_order": _get_attr(spec, "lineOrder"),
        "compression": _get_attr(spec, "compression"),
        "color_space": _get_attr(spec, "oiio:ColorSpace") or _get_attr(spec, "colorspace"),
        "render_software": _get_attr(spec, "Software") or _get_attr(spec, "software"),
        "is_tiled": bool(spec.tile_width),
        "tile_width": spec.tile_width or None,
        "tile_height": spec.tile_height or None,
        "tile_depth": spec.tile_depth or None,
        "is_deep": bool(spec.deep),
    }
    return {key: value for key, value in part.items() if value is not None}


def _extract_window_ints(window: Any) -> Optional[Dict[str, int]]:
    if window is None:
        return None
    try:
        if hasattr(window, "min") and hasattr(window, "max"):
            return {
                "min_x": int(window.min.x) if hasattr(window.min, "x") else int(window.min[0]),
                "min_y": int(window.min.y) if hasattr(window.min, "y") else int(window.min[1]),
                "max_x": int(window.max.x) if hasattr(window.max, "x") else int(window.max[0]),
                "max_y": int(window.max.y) if hasattr(window.max, "y") else int(window.max[1]),
            }
        if isinstance(window, dict):
            mn = window.get("min", {})
            mx = window.get("max", {})
            return {
                "min_x": int(mn.get("x", 0)),
                "min_y": int(mn.get("y", 0)),
                "max_x": int(mx.get("x", 0)),
                "max_y": int(mx.get("y", 0)),
            }
    except (TypeError, ValueError, AttributeError):
        pass
    return None


def _spec_to_channels(spec: Any, part_index: int) -> List[Dict[str, Any]]:
    channel_formats = getattr(spec, "channelformats", None) or []
    x_samples = getattr(spec, "x_channel_samples", None) or []
    y_samples = getattr(spec, "y_channel_samples", None) or []
    channels: List[Dict[str, Any]] = []
    for idx, name in enumerate(spec.channelnames):
        if channel_formats and idx < len(channel_formats):
            data_type = _type_desc_to_str(channel_formats[idx])
        else:
            data_type = _type_desc_to_str(spec.format)
        if "." in name:
            layer_name = name.rsplit(".", 1)[0]
            component_name = name.rsplit(".", 1)[1]
        else:
            layer_name = ""
            component_name = name
        channels.append(
            {
                "part_index": part_index,
                "name": name,
                "layer_name": layer_name,
                "component_name": component_name,
                "type": data_type,
                "x_sampling": x_samples[idx] if idx < len(x_samples) else 1,
                "y_sampling": y_samples[idx] if idx < len(y_samples) else 1,
            }
        )
    return channels


def _attributes_from_spec(spec: Any) -> List[Dict[str, Any]]:
    attributes: List[Dict[str, Any]] = []
    for attr in spec.extra_attribs:
        attributes.append(
            {
                "name": attr.name,
                "type": _type_desc_to_str(attr.type),
                "value": _serialize_value(attr.value),
            }
        )
    return attributes


def _color_from_spec(spec: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    cs = _get_attr(spec, "oiio:ColorSpace") or _get_attr(spec, "colorspace")
    if cs:
        out["color_space"] = cs
    tf = _get_attr(spec, "oiio:TransferFunction") or _get_attr(spec, "transferFunction")
    if tf:
        out["transfer_function"] = tf
    chroma = _get_attr(spec, "chromaticities")
    if chroma is not None:
        out["primaries"] = _serialize_value(chroma)
    return out


def _timecode_from_spec(spec: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    tc = _get_attr(spec, "smpte:TimeCode") or _get_attr(spec, "timeCode")
    if tc is not None:
        out["value"] = _serialize_value(tc)
    fps = _get_attr(spec, "FramesPerSecond") or _get_attr(spec, "framesPerSecond")
    if fps is not None:
        try:
            out["rate"] = float(fps)
        except (TypeError, ValueError):
            pass
    return out


def _camera_from_spec(spec: Any) -> Dict[str, Any]:
    pairs = {
        "make": "Make",
        "model": "Model",
        "lens": "LensModel",
        "exposure": "ExposureTime",
        "fnumber": "FNumber",
        "iso": "Exif:PhotographicSensitivity",
    }
    out: Dict[str, Any] = {}
    for normalized_key, attr_name in pairs.items():
        value = _get_attr(spec, attr_name)
        if value not in (None, ""):
            out[normalized_key] = _serialize_value(value)
    return out


def _production_from_spec(spec: Any) -> Dict[str, Any]:
    pairs = {
        "creator": "Artist",
        "copyright": "Copyright",
        "description": "ImageDescription",
        "software": "Software",
    }
    out: Dict[str, Any] = {}
    for normalized_key, attr_name in pairs.items():
        value = _get_attr(spec, attr_name)
        if value not in (None, ""):
            out[normalized_key] = _serialize_value(value)
    return out


# ---------------------------------------------------------------------------
# AOV (Arbitrary Output Variable) grouping
# ---------------------------------------------------------------------------
#
# Channels carry the raw pixel data; AOVs are the renderer-meaningful groups
# the user thinks in (beauty, diffuse, specular, AO, Z, motion_vec, normals,
# cryptomatte, ...). Two EXR conventions:
#   1. Single-part: channels named '<aov>.<component>' (e.g. diffuse.R).
#      Bare R/G/B/A become 'beauty'; bare Z becomes 'depth'.
#   2. Multi-part: each part's part_name is the AOV; per-channel names are
#      typically just R/G/B/A within the part.
# Stereo is captured by part.view_name and is part of the AOV grouping key —
# so left+right beauty become two rows distinguished by view, not collapsed.
# Cryptomatte ranks (uCryptoObject00, uCryptoObject01, ...) collapse to one
# AOV row with `ranks: N`.

_DEPTH_LABEL: Dict[str, str] = {
    "HALF": "16f", "FLOAT": "32f", "DOUBLE": "64f",
    "UINT8": "8u", "UINT16": "16u", "UINT32": "32u",
    "INT8": "8i", "INT16": "16i", "INT32": "32i",
}
_DEPTH_BITS: Dict[str, int] = {
    "HALF": 16, "FLOAT": 32, "DOUBLE": 64,
    "UINT8": 8, "UINT16": 16, "UINT32": 32,
    "INT8": 8, "INT16": 16, "INT32": 32,
}

_CANONICAL_GROUPS: List[Tuple[frozenset, str, List[str]]] = [
    (frozenset("RGBA"), "RGBA", ["R", "G", "B", "A"]),
    (frozenset("RGB"), "RGB", ["R", "G", "B"]),
    (frozenset("XYZ"), "XYZ", ["X", "Y", "Z"]),
    (frozenset("XY"), "XY", ["X", "Y"]),
    (frozenset("UV"), "UV", ["U", "V"]),
    (frozenset(["Z"]), "Z", ["Z"]),
]

# Standard beauty part-name aliases (Arnold/RenderMan/Karma/V-Ray emit one of
# these for the main render pass). Comparison is case-insensitive.
_BEAUTY_PART_NAMES: frozenset = frozenset({"rgba", "beauty", "main", "combined"})

# Bare AOV names that always carry non-color data — viewers must not apply
# a view transform to these.
_DATA_AOV_NAMES: frozenset = frozenset({
    "z", "depth",
    "n", "normal", "normals",
    "p", "position", "world_position",
    "motion", "motion_vec", "motionvector", "vector",
    "uv", "st",
    "id", "objectid", "object_id", "matteid", "matte_id",
})

# Common AOV bases that get split with a light group suffix
# (e.g., "diffuse_key" -> name="diffuse", light_group="key").
_LIGHT_GROUP_RE = re.compile(
    r"^(diffuse|specular|sss|transmission|emission|indirect|direct|"
    r"volume|reflection|refraction|coat)_(.+)$",
    re.IGNORECASE,
)

# Cryptomatte rank pattern — captures Arnold's uCryptoObjectNN, RenderMan's
# CryptoMaterialNN, and the lowercase crypto_object0/1 variants. Group 1 is
# the AOV-level group name; group 2 is the rank index.
_CRYPTO_RE = re.compile(r"^(.*[Cc]rypto[A-Za-z_]*?)(\d+)$")


def _channel_group_label(components: List[str]) -> str:
    """Build the UI-friendly channel-group label (RGB/RGBA/XYZ/UV/Z/...)."""
    if not components:
        return ""
    upper_set = frozenset(c.upper() for c in components)
    for canonical_set, label, _order in _CANONICAL_GROUPS:
        if upper_set == canonical_set:
            return label
    if len(components) == 1:
        return components[0].upper()
    return "+".join(sorted(c.upper() for c in components))


def _canonical_components(components: List[str]) -> List[str]:
    """Reorder components into semantic order (R,G,B,A or X,Y,Z, etc.) rather
    than EXR alphabetical storage order."""
    if not components:
        return []
    upper_set = frozenset(c.upper() for c in components)
    for canonical_set, _label, order in _CANONICAL_GROUPS:
        if upper_set == canonical_set:
            return order
    return sorted(c.upper() for c in components)


def _detect_cryptomatte(token: str) -> Tuple[Optional[str], Optional[int]]:
    """Return (group_name, rank_index) if `token` looks like a cryptomatte
    rank, else (None, None)."""
    if not token:
        return None, None
    match = _CRYPTO_RE.match(token)
    if not match:
        return None, None
    return match.group(1), int(match.group(2))


def _classify_aov(
    name: str,
    is_deep: bool,
    is_crypto: bool,
) -> Tuple[str, bool, Optional[str], str]:
    """Return (category, is_data, light_group, normalized_name).

    Categories: beauty / light_group / utility / matte / crypto / denoise /
    deep / data. The decision is based on the AOV name (which is the
    layer_name for in-part AOVs, or the part_name for multi-part AOVs) — not
    the containing part's name, because in single-part multi-AOV EXRs the
    part is universally called "rgba" yet contains many non-beauty AOVs.
    """
    if is_crypto:
        return "crypto", True, None, name

    name_lower = name.lower()

    if name_lower in _BEAUTY_PART_NAMES:
        return "beauty", False, None, name

    if is_deep:
        return "deep", True, None, name

    if name_lower in _DATA_AOV_NAMES:
        return "data", True, None, name

    light_match = _LIGHT_GROUP_RE.match(name_lower)
    if light_match:
        # Display name stays full ("diffuse_key"); light_group is sidecar
        # metadata so the UI can sort/group without collapsing distinct rows.
        return "light_group", False, light_match.group(2), name

    if "matte" in name_lower or "mask" in name_lower:
        return "matte", True, None, name

    if any(t in name_lower for t in ("denoise_albedo", "denoise_normal", "variance")):
        return "denoise", False, None, name

    return "utility", False, None, name


def _extract_aovs(
    parts: List[Dict[str, Any]],
    channels: List[Dict[str, Any]],
    file_format: str,
) -> List[Dict[str, Any]]:
    """Group channels into AOV records ready for UI display and DB persistence.

    See module-level comment for the EXR conventions handled.
    """
    if not parts:
        return []

    part_lookup = {p["part_index"]: p for p in parts}
    multipart = len(parts) > 1
    grouped: Dict[Tuple[int, str, str], Dict[str, Any]] = {}

    for ch in channels:
        part_idx = ch.get("part_index", 0)
        layer = (ch.get("layer_name") or "").strip()
        component = ch.get("component_name") or ch.get("name") or ""
        part = part_lookup.get(part_idx, {})
        part_name = (part.get("part_name") or "").strip()
        view = part.get("view_name") or ""

        # Cryptomatte ranks collapse to one AOV row.
        crypto_source = layer or part_name or component
        crypto_group, rank = _detect_cryptomatte(crypto_source)
        if crypto_group:
            aov_name = crypto_group
        elif multipart and part_name:
            aov_name = part_name
        elif layer:
            aov_name = layer
        elif component.upper() in ("R", "G", "B", "A"):
            aov_name = "beauty"
        elif component.upper() == "Z":
            aov_name = "depth"
        else:
            aov_name = component

        key = (part_idx, aov_name, view)
        rec = grouped.setdefault(
            key,
            {
                "name": aov_name,
                "part_index": part_idx,
                "view": view or None,
                "components": [],
                "types": [],
                "samplings": [],
                "ranks": set(),
                "is_crypto": bool(crypto_group),
                "part": part,
            },
        )
        rec["components"].append(component)
        rec["types"].append((ch.get("type") or "UNKNOWN").upper())
        rec["samplings"].append(
            (int(ch.get("x_sampling") or 1), int(ch.get("y_sampling") or 1))
        )
        if rank is not None:
            rec["ranks"].add(rank)

    aovs: List[Dict[str, Any]] = []
    for rec in grouped.values():
        unique_types = sorted(set(rec["types"]))
        is_mixed = len(unique_types) > 1
        primary = unique_types[0]
        depth_label = "MIXED" if is_mixed else _DEPTH_LABEL.get(primary, primary.lower())

        part = rec["part"]
        width = int(part.get("width") or 0)
        height = int(part.get("height") or 0)
        size_bytes = 0
        for type_name, (xs, ys) in zip(rec["types"], rec["samplings"]):
            pixels = (width * height) // max(1, xs * ys)
            size_bytes += pixels * (_DEPTH_BITS.get(type_name, 32) // 8)

        components_canonical = _canonical_components(rec["components"])
        channel_group = _channel_group_label(rec["components"])

        category, is_data, light_group, normalized_name = _classify_aov(
            rec["name"],
            bool(part.get("is_deep")),
            rec["is_crypto"],
        )

        # Data AOVs are always raw (non-color); beauty/lighting inherit the
        # part's color_space attribute.
        if is_data:
            color_space = "raw"
        else:
            color_space = part.get("color_space") or "unknown"

        # Synthetic = no real AOV concept in the source format. For non-EXR
        # formats with a single-channel-group beauty pass, mark it so the UI
        # can render the panel uniformly without misrepresenting the source.
        synthetic = file_format != "openexr" and category == "beauty" and not multipart

        record = {
            "name": normalized_name,
            "part_index": rec["part_index"],
            "view": rec["view"],
            "components": components_canonical,
            "channel_group": channel_group,
            "channel_count": len(rec["components"]),
            "data_type": "MIXED" if is_mixed else primary,
            "bit_depth": _DEPTH_BITS.get(primary, 32),
            "depth_label": depth_label,
            "category": category,
            "is_beauty": category == "beauty",
            "is_data": is_data,
            "color_space": color_space,
            "light_group": light_group,
            "synthetic": synthetic,
            "uncompressed_bytes": size_bytes,
        }
        if rec["is_crypto"]:
            record["ranks"] = len(rec["ranks"]) or 1
        aovs.append(record)

    # Order: beauty rows first, then light groups, then everything else
    # alphabetically; within an AOV, sort by view for stereo determinism.
    aovs.sort(
        key=lambda a: (
            not a["is_beauty"],
            a["category"] != "light_group",
            a["name"].lower(),
            a.get("view") or "",
        )
    )

    # pct_of_frame_logical: per-frame share among AOVs (sums to ~100). Pipeline
    # specifically warned against pro-rata compressed bytes, so this is the
    # honest share of the *uncompressed* footprint.
    total_bytes = sum(a["uncompressed_bytes"] for a in aovs) or 1
    for record in aovs:
        record["pct_of_frame_logical"] = round(
            100.0 * record["uncompressed_bytes"] / total_bytes, 4
        )

    return aovs


# ---------------------------------------------------------------------------
# Serialization helpers (shared with exr_inspector shape for DB compatibility)
# ---------------------------------------------------------------------------


def _get_attr(spec: Any, name: str) -> Any:
    try:
        return spec.getattribute(name)
    except Exception:
        return None


def _type_desc_to_str(type_desc: Any) -> str:
    try:
        return str(type_desc).upper()
    except Exception:
        return "UNKNOWN"


def _py_type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, str):
        return "STRING"
    return type(value).__name__.upper()


def _serialize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        return {
            "encoding": "base64",
            "data": base64.b64encode(value).decode("ascii"),
        }
    normalized = _serialize_oiio_type(value)
    if normalized is not None:
        return normalized
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_value(val) for key, val in value.items()}
    return value


def _serialize_oiio_type(value: Any) -> Optional[Any]:
    if hasattr(value, "tolist"):
        try:
            return value.tolist()
        except Exception:
            return None
    if hasattr(value, "min") and hasattr(value, "max"):
        try:
            return {
                "min": _serialize_value(value.min),
                "max": _serialize_value(value.max),
            }
        except Exception:
            return None
    vector_keys = ("x", "y", "z", "w")
    if any(hasattr(value, key) for key in vector_keys):
        vector: Dict[str, Any] = {}
        for key in vector_keys:
            if hasattr(value, key):
                vector[key] = _serialize_value(getattr(value, key))
        if vector:
            return vector
    color_keys = ("r", "g", "b", "a")
    if any(hasattr(value, key) for key in color_keys):
        color: Dict[str, Any] = {}
        for key in color_keys:
            if hasattr(value, key):
                color[key] = _serialize_value(getattr(value, key))
        if color:
            return color
    if hasattr(value, "__iter__") and not isinstance(value, (str, bytes)):
        try:
            return [_serialize_value(item) for item in value]
        except Exception:
            return None
    return None


def _isoformat(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _error_result(message: str) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "file": {},
        "parts": [],
        "channels": [],
        "aovs": [],
        "attributes": {},
        "color": {},
        "timecode": {},
        "sequence": {},
        "camera": {},
        "production": {},
        "extraction": {
            "tool": "frame-metadata-extractor",
            "tool_version": __version__,
            "timestamp": _isoformat(time.time()),
            "warnings": [],
        },
        "errors": [message],
        "timestamp": _isoformat(time.time()),
    }
