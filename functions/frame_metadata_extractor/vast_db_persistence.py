"""VAST DataBase persistence layer for frame-metadata-extractor.

Adapted from exr-inspector to support multiple VFX still-frame formats
(EXR, DPX, TIFF, PNG, TGA, HDR, JPEG2000, Cineon). The schema shape is kept
backwards-compatible with the EXR flow (files / parts / channels /
attributes) but stored under a new schema name ``frame_metadata``, and the
``files`` table gains a ``format`` column so downstream consumers can
filter by container type without re-parsing paths.

Key features:

- Deterministic vector embeddings for metadata and channel structure
- Idempotent upsert pattern using SELECT-then-INSERT (no UPDATE row IDs)
- PyArrow table conversion for efficient batch inserts
- Transaction-based consistency with rollback on error
- Stateless session management for serverless environments
- Comprehensive error handling and audit logging

Author: Claude Code
Date: 2025-02-05
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import struct
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import pyarrow as pa
except ImportError:
    pa = None

try:
    import vastdb
except ImportError:
    vastdb = None


logger = logging.getLogger(__name__)


# Configuration defaults
DEFAULT_METADATA_EMBEDDING_DIM = 384
DEFAULT_CHANNEL_FINGERPRINT_DIM = 128
DEFAULT_SCHEMA_NAME = "frame_metadata"
DEFAULT_VASTDB_ENDPOINT = os.environ.get("VAST_DB_ENDPOINT", "")
DEFAULT_VASTDB_BUCKET = os.environ.get("VAST_DB_BUCKET", "frame-data")


class VectorEmbeddingError(Exception):
    """Raised when vector embedding computation fails."""
    pass


class VASTDatabaseError(Exception):
    """Raised when VAST DataBase operations fail."""
    pass


# ============================================================================
# Vector Embedding Functions
# ============================================================================


def compute_metadata_embedding(
    payload: Dict[str, Any],
    embedding_dim: int = DEFAULT_METADATA_EMBEDDING_DIM,
) -> List[float]:
    """
    Compute a deterministic vector embedding for complete EXR metadata.

    This function creates a single normalized vector representing all metadata
    from an EXR file inspection. The embedding is deterministic: the same input
    payload will always produce the same vector. This approach avoids external
    ML dependencies while capturing structural metadata characteristics.

    The embedding is computed by:
    1. Extracting key features (channel count, compression type, etc.)
    2. Creating a normalized feature vector
    3. Hashing to fill additional dimensions
    4. Normalizing to unit vector

    Args:
        payload: Complete exr-inspector JSON output from _inspect_exr()
        embedding_dim: Output vector dimensionality (default: 384)

    Returns:
        List of float values with length equal to embedding_dim

    Raises:
        VectorEmbeddingError: If payload structure is invalid

    Example:
        >>> payload = {
        ...     "file": {"multipart_count": 2, "is_deep": False},
        ...     "channels": [...],
        ...     "parts": [...]
        ... }
        >>> vec = compute_metadata_embedding(payload)
        >>> len(vec)  # == 384
        384
        >>> # Same payload produces same vector (deterministic)
        >>> vec2 = compute_metadata_embedding(payload)
        >>> all(abs(v1 - v2) < 1e-9 for v1, v2 in zip(vec, vec2))
        True
    """
    try:
        # Extract normalized features from payload
        features = _extract_metadata_features(payload)

        # Build initial feature vector from extracted metrics
        feature_values = [
            float(features.get("channel_count", 0)) / max(1, 64),  # normalize to [0,1]
            float(features.get("part_count", 0)) / max(1, 16),
            float(features.get("is_deep", 0)),
            float(features.get("is_tiled", 0)),
            float(features.get("has_multiview", 0)),
            _compression_to_normalized(features.get("compression_type", "")),
        ]

        # Hash the complete payload JSON to fill remaining dimensions
        payload_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).digest()

        # Convert hash bytes to normalized float values
        hash_values = [
            (struct.unpack("f", payload_hash[i : i + 4])[0] % 1.0)
            if i + 4 <= len(payload_hash)
            else 0.0
            for i in range(0, len(payload_hash) - 4, 4)
        ]

        # Combine feature and hash vectors
        combined = feature_values + hash_values

        # Pad or truncate to target dimension
        if len(combined) < embedding_dim:
            # Pad with derived values from combined
            while len(combined) < embedding_dim:
                seed = len(combined)
                combined.append(
                    abs(
                        (sum(combined) * (seed + 1))
                        % (seed + 2)
                    ) / max(1, seed + 2)
                )
        else:
            combined = combined[:embedding_dim]

        # Normalize to unit vector (L2 norm)
        magnitude = (sum(v * v for v in combined) ** 0.5)
        if magnitude < 1e-9:
            # Degenerate case: uniform vector
            return [1.0 / (embedding_dim ** 0.5)] * embedding_dim

        normalized = [v / magnitude for v in combined]
        return normalized

    except Exception as exc:
        raise VectorEmbeddingError(
            f"Failed to compute metadata embedding: {exc}"
        ) from exc


def compute_channel_fingerprint(
    channels: List[Dict[str, Any]],
    embedding_dim: int = DEFAULT_CHANNEL_FINGERPRINT_DIM,
) -> List[float]:
    """
    Compute a deterministic vector embedding for EXR channel structure.

    This function creates a fingerprint of the channel layout and properties.
    Useful for finding files with similar channel configurations.

    The fingerprint captures:
    - Channel count and naming patterns
    - Data types distribution
    - Sampling patterns (x/y sampling ratios)
    - Layer/component organization

    Args:
        channels: List of channel dictionaries from exr-inspector output
        embedding_dim: Output vector dimensionality (default: 128)

    Returns:
        List of float values with length equal to embedding_dim

    Raises:
        VectorEmbeddingError: If channel structure is invalid

    Example:
        >>> channels = [
        ...     {"name": "R", "type": "float", "x_sampling": 1, "y_sampling": 1},
        ...     {"name": "G", "type": "float", "x_sampling": 1, "y_sampling": 1},
        ... ]
        >>> fp = compute_channel_fingerprint(channels)
        >>> len(fp)  # == 128
        128
    """
    try:
        if not channels:
            return [0.0] * embedding_dim

        # Extract channel features
        channel_count = len(channels)
        type_counts: Dict[str, int] = {}
        total_x_sampling = 0
        total_y_sampling = 0
        layer_set = set()

        for ch in channels:
            ch_type = ch.get("type", "unknown")
            type_counts[ch_type] = type_counts.get(ch_type, 0) + 1
            total_x_sampling += ch.get("x_sampling", 1)
            total_y_sampling += ch.get("y_sampling", 1)

            # Extract layer name (e.g., "diffuse.R" -> "diffuse")
            name = ch.get("name", "")
            if "." in name:
                layer_set.add(name.split(".")[0])

        # Build feature vector
        features = [
            float(channel_count) / 64.0,  # normalize to [0,1]
            float(len(layer_set)) / max(1, channel_count),
            float(total_x_sampling) / max(1, channel_count * 2),
            float(total_y_sampling) / max(1, channel_count * 2),
        ]

        # Add type distribution as ratios
        for data_type in ["float", "half", "uint32", "uint8"]:
            count = type_counts.get(data_type, 0)
            features.append(float(count) / max(1, channel_count))

        # Hash channel names for unique identification
        channel_names = [ch.get("name", "") for ch in channels]
        names_hash = hashlib.md5(
            "|".join(channel_names).encode()
        ).digest()

        hash_values = [
            (struct.unpack("f", names_hash[i : i + 4])[0] % 1.0)
            if i + 4 <= len(names_hash)
            else 0.0
            for i in range(0, len(names_hash) - 4, 4)
        ]

        # Combine all vectors
        combined = features + hash_values

        # Pad or truncate
        if len(combined) < embedding_dim:
            while len(combined) < embedding_dim:
                combined.append(
                    abs(
                        sum(combined[:4])
                        * (len(combined) + 1)
                    ) % 1.0
                )
        else:
            combined = combined[:embedding_dim]

        # Normalize to unit vector
        magnitude = (sum(v * v for v in combined) ** 0.5)
        if magnitude < 1e-9:
            return [1.0 / (embedding_dim ** 0.5)] * embedding_dim

        return [v / magnitude for v in combined]

    except Exception as exc:
        raise VectorEmbeddingError(
            f"Failed to compute channel fingerprint: {exc}"
        ) from exc


# ============================================================================
# Helper Functions for Vector Computation
# ============================================================================


def _extract_metadata_features(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extract key features from EXR inspection payload."""
    file_info = payload.get("file", {})
    channels = payload.get("channels", [])
    parts = payload.get("parts", [])

    # Count unique compressions
    compressions = set()
    for part in parts:
        if comp := part.get("compression"):
            compressions.add(comp)

    return {
        "channel_count": len(channels),
        "part_count": len(parts),
        "is_deep": bool(file_info.get("is_deep", False)),
        "is_tiled": any(p.get("is_tiled", False) for p in parts),
        "has_multiview": any(p.get("multi_view") for p in parts),
        "compression_type": list(compressions)[0] if compressions else "none",
    }


def _compression_to_normalized(compression_type: str) -> float:
    """Convert compression type string to normalized float [0, 1]."""
    compression_map = {
        "none": 0.0,
        "rle": 0.2,
        "zips": 0.4,
        "zip": 0.5,
        "piz": 0.6,
        "pxr24": 0.7,
        "b44": 0.8,
        "b44a": 0.85,
        "dwaa": 0.9,
        "dwab": 0.95,
    }
    return compression_map.get(compression_type.lower(), 0.5)


# ============================================================================
# PyArrow Conversion Functions
# ============================================================================


def payload_to_files_row(
    payload: Dict[str, Any],
    metadata_embedding: List[float],
    file_id: Optional[str] = None,
) -> pa.Table:
    """
    Convert inspection payload to a PyArrow table row for files table.

    Creates a single-row table with file-level metadata including embedded vector.

    Args:
        payload: exr-inspector JSON output
        metadata_embedding: Vector from compute_metadata_embedding()
        file_id: Optional UUID (generated if not provided)

    Returns:
        PyArrow Table with schema matching VAST DataBase files table

    Raises:
        ValueError: If payload structure is invalid
    """
    if pa is None:
        raise ImportError("pyarrow is required for payload conversion")

    file_info = payload.get("file", {})
    if not file_info.get("path"):
        raise ValueError("Payload missing file.path")

    # Generate file_id if not provided
    if not file_id:
        path_hash = hashlib.md5(file_info["path"].encode()).hexdigest()
        mtime = file_info.get("mtime", "")
        file_id = hashlib.sha256(
            f"{file_info['path']}{mtime}{path_hash}".encode()
        ).hexdigest()[:16]

    # Create normalized path for deduplication
    file_path_normalized = _normalize_path(file_info["path"])

    # Compute header hash from key structural elements
    header_elements = [
        str(file_info.get("multipart_count", 0)),
        str(file_info.get("is_deep", False)),
        json.dumps(payload.get("parts", []), sort_keys=True, default=str),
    ]
    header_hash = hashlib.sha256(
        "".join(header_elements).encode()
    ).hexdigest()

    now = datetime.now(timezone.utc).isoformat()

    schema = pa.schema([
        ("file_id", pa.string()),
        ("file_path", pa.string()),
        ("file_path_normalized", pa.string()),
        ("format", pa.string()),
        ("header_hash", pa.string()),
        ("size_bytes", pa.int64()),
        ("mtime", pa.string()),
        ("multipart_count", pa.int32()),
        ("is_deep", pa.bool_()),
        ("metadata_embedding", pa.list_(pa.field(name="item", type=pa.float32(), nullable=False), DEFAULT_METADATA_EMBEDDING_DIM)),
        ("frame_number", pa.int32()),
        ("inspection_timestamp", pa.string()),
        ("inspection_count", pa.int32()),
        ("last_inspected", pa.string()),
    ])

    data = {
        "file_id": [file_id],
        "file_path": [file_info.get("path", "")],
        "file_path_normalized": [file_path_normalized],
        "format": [file_info.get("format", "unknown")],
        "header_hash": [header_hash],
        "size_bytes": [file_info.get("size_bytes", 0)],
        "mtime": [file_info.get("mtime", "")],
        "multipart_count": [file_info.get("multipart_count", 1)],
        "is_deep": [file_info.get("is_deep", False)],
        "metadata_embedding": [metadata_embedding],
        "frame_number": [file_info.get("frame_number")],
        "inspection_timestamp": [now],
        "inspection_count": [1],
        "last_inspected": [now],
    }

    return pa.table(data, schema=schema)


def payload_to_parts_rows(
    payload: Dict[str, Any],
    file_id: str,
) -> pa.Table:
    """
    Convert inspection payload to PyArrow table rows for parts table.

    Creates one row per part (subimage) in the EXR file.

    Args:
        payload: exr-inspector JSON output
        file_id: Parent file_id from files table

    Returns:
        PyArrow Table with schema matching VAST DataBase parts table
    """
    if pa is None:
        raise ImportError("pyarrow is required for payload conversion")

    parts = payload.get("parts", [])
    if not parts:
        return pa.table({
            "file_id": pa.array([], type=pa.string()),
        })

    file_info = payload.get("file", {})
    file_path = file_info.get("path", "")

    schema = pa.schema([
        ("file_id", pa.string()),
        ("file_path", pa.string()),
        ("part_index", pa.int32()),
        ("width", pa.int32()),
        ("height", pa.int32()),
        ("display_width", pa.int32()),
        ("display_height", pa.int32()),
        ("data_x_offset", pa.int32()),
        ("data_y_offset", pa.int32()),
        ("part_name", pa.string()),
        ("view_name", pa.string()),
        ("multi_view", pa.bool_()),
        ("data_window", pa.string()),
        ("display_window", pa.string()),
        ("pixel_aspect_ratio", pa.float32()),
        ("line_order", pa.string()),
        ("compression", pa.string()),
        ("color_space", pa.string()),
        ("render_software", pa.string()),
        ("is_tiled", pa.bool_()),
        ("tile_width", pa.int32()),
        ("tile_height", pa.int32()),
        ("tile_depth", pa.int32()),
        ("is_deep", pa.bool_()),
    ])

    data = {
        "file_id": [],
        "file_path": [],
        "part_index": [],
        "width": [],
        "height": [],
        "display_width": [],
        "display_height": [],
        "data_x_offset": [],
        "data_y_offset": [],
        "part_name": [],
        "view_name": [],
        "multi_view": [],
        "data_window": [],
        "display_window": [],
        "pixel_aspect_ratio": [],
        "line_order": [],
        "compression": [],
        "color_space": [],
        "render_software": [],
        "is_tiled": [],
        "tile_width": [],
        "tile_height": [],
        "tile_depth": [],
        "is_deep": [],
    }

    for part in parts:
        data["file_id"].append(file_id)
        data["file_path"].append(file_path)
        data["part_index"].append(part.get("part_index", 0))
        data["width"].append(part.get("width", 0))
        data["height"].append(part.get("height", 0))
        data["display_width"].append(part.get("display_width", 0))
        data["display_height"].append(part.get("display_height", 0))
        data["data_x_offset"].append(part.get("data_x_offset", 0))
        data["data_y_offset"].append(part.get("data_y_offset", 0))
        data["part_name"].append(part.get("part_name"))
        data["view_name"].append(part.get("view_name"))
        data["multi_view"].append(bool(part.get("multi_view")))
        data["data_window"].append(json.dumps(part.get("data_window")))
        data["display_window"].append(json.dumps(part.get("display_window")))
        data["pixel_aspect_ratio"].append(
            float(part.get("pixel_aspect_ratio", 1.0))
        )
        data["line_order"].append(part.get("line_order"))
        data["compression"].append(part.get("compression"))
        data["color_space"].append(part.get("color_space"))
        data["render_software"].append(part.get("render_software"))
        data["is_tiled"].append(bool(part.get("is_tiled")))
        data["tile_width"].append(part.get("tile_width") or 0)
        data["tile_height"].append(part.get("tile_height") or 0)
        data["tile_depth"].append(part.get("tile_depth") or 0)
        data["is_deep"].append(bool(part.get("is_deep")))

    return pa.table(data, schema=schema)


def payload_to_channels_rows(
    payload: Dict[str, Any],
    file_id: str,
    channel_fingerprint: List[float],
) -> pa.Table:
    """
    Convert inspection payload to PyArrow table rows for channels table.

    Creates one row per channel across all parts.

    Args:
        payload: exr-inspector JSON output
        file_id: Parent file_id from files table
        channel_fingerprint: Vector from compute_channel_fingerprint()

    Returns:
        PyArrow Table with schema matching VAST DataBase channels table
    """
    if pa is None:
        raise ImportError("pyarrow is required for payload conversion")

    channels = payload.get("channels", [])
    if not channels:
        return pa.table({
            "file_id": pa.array([], type=pa.string()),
        })

    file_info = payload.get("file", {})
    file_path = file_info.get("path", "")

    schema = pa.schema([
        ("file_id", pa.string()),
        ("file_path", pa.string()),
        ("part_index", pa.int32()),
        ("channel_name", pa.string()),
        ("layer_name", pa.string()),
        ("component_name", pa.string()),
        ("channel_type", pa.string()),
        ("x_sampling", pa.int32()),
        ("y_sampling", pa.int32()),
        ("channel_fingerprint", pa.list_(pa.field(name="item", type=pa.float32(), nullable=False), DEFAULT_CHANNEL_FINGERPRINT_DIM)),
    ])

    data = {
        "file_id": [],
        "file_path": [],
        "part_index": [],
        "channel_name": [],
        "layer_name": [],
        "component_name": [],
        "channel_type": [],
        "x_sampling": [],
        "y_sampling": [],
        "channel_fingerprint": [],
    }

    for idx, channel in enumerate(channels):
        data["file_id"].append(file_id)
        data["file_path"].append(file_path)
        data["part_index"].append(channel.get("part_index", 0))
        data["channel_name"].append(channel.get("name", ""))
        data["layer_name"].append(channel.get("layer_name", ""))
        data["component_name"].append(channel.get("component_name", ""))
        data["channel_type"].append(channel.get("type", ""))
        data["x_sampling"].append(channel.get("x_sampling", 1))
        data["y_sampling"].append(channel.get("y_sampling", 1))
        # Include fingerprint only in first row to avoid duplication
        data["channel_fingerprint"].append(
            channel_fingerprint if idx == 0 else [0.0] * DEFAULT_CHANNEL_FINGERPRINT_DIM
        )

    return pa.table(data, schema=schema)


def payload_to_attributes_rows(
    payload: Dict[str, Any],
    file_id: str,
) -> pa.Table:
    """
    Convert inspection payload to PyArrow table rows for attributes table.

    Creates one row per attribute across all parts.

    Args:
        payload: exr-inspector JSON output
        file_id: Parent file_id from files table

    Returns:
        PyArrow Table with schema matching VAST DataBase attributes table
    """
    if pa is None:
        raise ImportError("pyarrow is required for payload conversion")

    attributes_data = payload.get("attributes", {})
    parts_attrs = attributes_data.get("parts", [])

    if not parts_attrs:
        return pa.table({
            "file_id": pa.array([], type=pa.string()),
        })

    file_info = payload.get("file", {})
    file_path = file_info.get("path", "")

    schema = pa.schema([
        ("file_id", pa.string()),
        ("file_path", pa.string()),
        ("part_index", pa.int32()),
        ("attr_name", pa.string()),
        ("attr_type", pa.string()),
        ("value_json", pa.string()),
        ("value_text", pa.string()),
        ("value_int", pa.int64()),
        ("value_float", pa.float64()),
    ])

    data = {
        "file_id": [],
        "file_path": [],
        "part_index": [],
        "attr_name": [],
        "attr_type": [],
        "value_json": [],
        "value_text": [],
        "value_int": [],
        "value_float": [],
    }

    for part_idx, part_attrs in enumerate(parts_attrs):
        if not isinstance(part_attrs, list):
            continue

        for attr in part_attrs:
            value = attr.get("value")
            data["file_id"].append(file_id)
            data["file_path"].append(file_path)
            data["part_index"].append(part_idx)
            data["attr_name"].append(attr.get("name", ""))
            data["attr_type"].append(attr.get("type", ""))
            data["value_json"].append(json.dumps(value))
            data["value_text"].append(str(value) if isinstance(value, str) else None)
            data["value_int"].append(int(value) if isinstance(value, (int,)) and not isinstance(value, bool) else None)
            data["value_float"].append(float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None)

    return pa.table(data, schema=schema)


def payload_to_aovs_rows(
    payload: Dict[str, Any],
    file_id: str,
) -> pa.Table:
    """
    Convert inspection payload to PyArrow table rows for the aovs table.

    Creates one row per AOV per frame. AOV records come from
    main._extract_aovs() which groups channels into renderer-meaningful
    layers (beauty, diffuse, specular, AO, depth, motion_vec, normals,
    cryptomatte, etc.).

    Args:
        payload: frame-metadata-extractor JSON output
        file_id: Parent file_id from files table

    Returns:
        PyArrow Table with schema matching VAST DataBase aovs table
    """
    if pa is None:
        raise ImportError("pyarrow is required for payload conversion")

    aovs = payload.get("aovs", [])
    if not aovs:
        return pa.table({"file_id": pa.array([], type=pa.string())})

    file_info = payload.get("file", {})
    file_path = file_info.get("path", "")
    frame_number = file_info.get("frame_number")

    data: Dict[str, List[Any]] = {col: [] for col in _AOVS_TABLE_SCHEMA.names}

    for idx, aov in enumerate(aovs):
        name = aov.get("name", "") or ""
        data["file_id"].append(file_id)
        data["file_path"].append(file_path)
        data["frame_number"].append(frame_number)
        data["aov_index"].append(idx)
        data["part_index"].append(int(aov.get("part_index", 0)))
        data["view"].append(aov.get("view") or "")
        data["name"].append(name)
        data["name_normalized"].append(name.lower())
        data["channel_group"].append(aov.get("channel_group", "") or "")
        data["components"].append([str(c) for c in aov.get("components", [])])
        data["channel_count"].append(int(aov.get("channel_count", 0)))
        data["data_type"].append(aov.get("data_type", "") or "")
        data["bit_depth"].append(int(aov.get("bit_depth", 0)))
        data["depth_label"].append(aov.get("depth_label", "") or "")
        data["category"].append(aov.get("category", "") or "")
        data["is_beauty"].append(bool(aov.get("is_beauty")))
        data["is_data"].append(bool(aov.get("is_data")))
        data["color_space"].append(aov.get("color_space", "") or "")
        data["light_group"].append(aov.get("light_group") or "")
        data["synthetic"].append(bool(aov.get("synthetic")))
        data["uncompressed_bytes"].append(int(aov.get("uncompressed_bytes", 0)))
        data["pct_of_frame_logical"].append(float(aov.get("pct_of_frame_logical", 0.0)))
        # ranks: 0 means "not a cryptomatte AOV"; >0 is the collapsed rank count
        data["ranks"].append(int(aov.get("ranks", 0)))

    return pa.table(data, schema=_AOVS_TABLE_SCHEMA)


# ============================================================================
# Path Normalization
# ============================================================================


def _normalize_path(path: str) -> str:
    """
    Normalize file path for consistent deduplication.

    Normalizes separators and converts to lowercase without resolving against
    the local filesystem — the path is a VAST view path, not a local path.

    Args:
        path: File path to normalize (VAST view path or S3 key)

    Returns:
        Normalized path string suitable as unique key
    """
    # Normalize separators to forward slash, strip trailing slash, lowercase
    normalized = path.replace("\\", "/").rstrip("/").lower()
    return normalized


# ============================================================================
# VAST DataBase Session Management
# ============================================================================


def _create_vastdb_session(
    ctx: Optional[Any] = None,
    event: Optional[Dict[str, Any]] = None,
) -> Optional[Any]:
    """
    Create a VAST DataBase session from ctx.secrets or environment.

    Uses ``vastdb.connect()`` from the official VAST Python SDK.

    Session creation prioritizes:
    1. ctx.secrets (DataEngine runtime - production path)
    2. Environment variables (local development / testing fallback)

    Args:
        ctx: DataEngine runtime context with secrets access
        event: DataEngine event (unused, kept for backward compat)

    Returns:
        Session object if successful, None if not configured

    Raises:
        VASTDatabaseError: If session creation fails due to invalid credentials
    """
    if vastdb is None:
        logger.warning("vastdb SDK not available; skipping persistence")
        return None

    endpoint = None
    access_key = None
    secret_key = None

    # Primary: ctx.secrets (production DataEngine path)
    secret_name = os.environ.get("VAST_DB_SECRET_NAME", "vast-db")
    if ctx is not None:
        try:
            secrets = ctx.secrets[secret_name]
            endpoint = secrets.get("endpoint")
            access_key = secrets.get("access_key")
            secret_key = secrets.get("secret_key")
            logger.debug("Credentials loaded from ctx.secrets['%s']", secret_name)
        except Exception:
            logger.debug("ctx.secrets['%s'] not available, falling back to env", secret_name)

    # Fallback: environment variables
    # VAST_DB_ENDPOINT takes priority; falls back to S3_ENDPOINT (same VIP on many clusters)
    if not endpoint:
        endpoint = (os.environ.get("VAST_DB_ENDPOINT")
                    or os.environ.get("S3_ENDPOINT")
                    or DEFAULT_VASTDB_ENDPOINT)
    if not access_key:
        access_key = os.environ.get("VAST_DB_ACCESS_KEY") or os.environ.get("S3_ACCESS_KEY")
    if not secret_key:
        secret_key = os.environ.get("VAST_DB_SECRET_KEY") or os.environ.get("S3_SECRET_KEY")

    if not endpoint:
        logger.debug("VAST_DB_ENDPOINT not configured")
        return None

    try:
        session = vastdb.connect(
            endpoint=endpoint,
            access=access_key,
            secret=secret_key,
        )
        logger.info("VAST DataBase session created: %s", endpoint)
        return session

    except Exception as exc:
        raise VASTDatabaseError(
            f"Failed to create VAST DataBase session: {exc}"
        ) from exc


# ============================================================================
# Main Persistence Function
# ============================================================================


def persist_to_vast_database(
    payload: Dict[str, Any],
    event: Optional[Dict[str, Any]] = None,
    ctx: Optional[Any] = None,
    vastdb_session: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Persist EXR inspection results to VAST DataBase with idempotent upsert.

    This is the main entry point for VAST DataBase persistence. It orchestrates
    the complete flow:

    1. Create/validate VAST DataBase session (via ctx.secrets or env vars)
    2. Compute vector embeddings
    3. Convert payload to PyArrow tables
    4. Start transaction
    5. INSERT all related records (files, parts, channels, attributes)
    6. Commit transaction with error handling

    Args:
        payload: Complete exr-inspector JSON output from handler()
        event: DataEngine event (optional, for backward compat)
        ctx: DataEngine runtime context with secrets access (production)
        vastdb_session: Optional pre-created session (for testing)

    Returns:
        dict with keys:
            - status: "success" or "error"
            - file_id: Unique identifier for persisted file (if successful)
            - inserted: bool indicating if new record was inserted
            - message: Human-readable status message
            - error: Error message (if status == "error")
    """
    result: Dict[str, Any] = {
        "status": "error",
        "file_id": None,
        "inserted": False,
        "message": "",
        "error": None,
    }

    # Validate payload structure
    file_info = payload.get("file", {})
    if not file_info.get("path"):
        result["error"] = "Payload missing file.path"
        result["message"] = "Invalid payload structure"
        logger.error(result["error"])
        return result

    file_path = file_info["path"]

    try:
        # Create or use provided session
        session = vastdb_session or _create_vastdb_session(ctx=ctx, event=event)
        if session is None:
            result["status"] = "skipped"
            result["message"] = "VAST DataBase not configured"
            logger.debug(f"VAST persistence skipped for {file_path}")
            return result

        # Only run DDL if session was not pre-initialized in init()
        if vastdb_session is None:
            ensure_database_tables(session)

        # Compute vector embeddings
        logger.debug(f"Computing embeddings for {file_path}")
        metadata_embedding = compute_metadata_embedding(payload)
        channel_fingerprint = compute_channel_fingerprint(
            payload.get("channels", [])
        )

        # Convert payload to PyArrow tables
        files_table = payload_to_files_row(payload, metadata_embedding)
        file_id = files_table.column("file_id")[0].as_py()

        parts_table = payload_to_parts_rows(payload, file_id)
        channels_table = payload_to_channels_rows(
            payload, file_id, channel_fingerprint
        )
        attributes_table = payload_to_attributes_rows(payload, file_id)
        aovs_table = payload_to_aovs_rows(payload, file_id)

        logger.debug(
            f"Tables converted for {file_id}: files, parts, channels, attributes, aovs"
        )

        # Perform transaction
        _persist_with_transaction(
            session=session,
            file_path=file_path,
            file_id=file_id,
            files_table=files_table,
            parts_table=parts_table,
            channels_table=channels_table,
            attributes_table=attributes_table,
            aovs_table=aovs_table,
            result=result,
        )

    except VectorEmbeddingError as exc:
        result["error"] = f"Embedding computation failed: {exc}"
        result["message"] = "Vector embedding error"
        logger.error(result["error"])

    except VASTDatabaseError as exc:
        result["error"] = f"VAST DataBase error: {exc}"
        result["message"] = "Database connection error"
        logger.error(result["error"])

    except Exception as exc:
        result["error"] = f"Unexpected error during persistence: {exc}"
        result["message"] = "Persistence failed"
        logger.exception(f"Unhandled exception persisting {file_path}")

    return result


# ============================================================================
# Database Auto-Provisioning (get-or-create pattern)
# ============================================================================
#
# VAST vastdb SDK: create_schema/create_table are NOT idempotent — they throw
# if the resource already exists. We use try/except to handle this safely.
# Per VAST Admin Guide p.623, the pattern is:
#   bucket.create_schema(name) -> schema
#   schema.create_table(name, pyarrow_schema) -> table
# Vector columns use: pa.list_(pa.field("item", pa.float32(), nullable=False), dim)
#
# The bucket (database) must pre-exist — it cannot be created via the SDK.
# DDL (create schema/table) runs in a separate transaction from DML (inserts).

# Table schemas for auto-creation (must match the schemas used in payload_to_*_rows)
_FILES_TABLE_SCHEMA = pa.schema([
    ("file_id", pa.string()),
    ("file_path", pa.string()),
    ("file_path_normalized", pa.string()),
    ("format", pa.string()),
    ("header_hash", pa.string()),
    ("size_bytes", pa.int64()),
    ("mtime", pa.string()),
    ("multipart_count", pa.int32()),
    ("is_deep", pa.bool_()),
    ("metadata_embedding", pa.list_(
        pa.field(name="item", type=pa.float32(), nullable=False),
        DEFAULT_METADATA_EMBEDDING_DIM,
    )),
    ("frame_number", pa.int32()),
    ("inspection_timestamp", pa.string()),
    ("inspection_count", pa.int32()),
    ("last_inspected", pa.string()),
])

_PARTS_TABLE_SCHEMA = pa.schema([
    ("file_id", pa.string()),
    ("file_path", pa.string()),
    ("part_index", pa.int32()),
    ("width", pa.int32()),
    ("height", pa.int32()),
    ("display_width", pa.int32()),
    ("display_height", pa.int32()),
    ("data_x_offset", pa.int32()),
    ("data_y_offset", pa.int32()),
    ("part_name", pa.string()),
    ("view_name", pa.string()),
    ("multi_view", pa.bool_()),
    ("data_window", pa.string()),
    ("display_window", pa.string()),
    ("pixel_aspect_ratio", pa.float32()),
    ("line_order", pa.string()),
    ("compression", pa.string()),
    ("color_space", pa.string()),
    ("render_software", pa.string()),
    ("is_tiled", pa.bool_()),
    ("tile_width", pa.int32()),
    ("tile_height", pa.int32()),
    ("tile_depth", pa.int32()),
    ("is_deep", pa.bool_()),
])

_CHANNELS_TABLE_SCHEMA = pa.schema([
    ("file_id", pa.string()),
    ("file_path", pa.string()),
    ("part_index", pa.int32()),
    ("channel_name", pa.string()),
    ("layer_name", pa.string()),
    ("component_name", pa.string()),
    ("channel_type", pa.string()),
    ("x_sampling", pa.int32()),
    ("y_sampling", pa.int32()),
    ("channel_fingerprint", pa.list_(
        pa.field(name="item", type=pa.float32(), nullable=False),
        DEFAULT_CHANNEL_FINGERPRINT_DIM,
    )),
])

_ATTRIBUTES_TABLE_SCHEMA = pa.schema([
    ("file_id", pa.string()),
    ("file_path", pa.string()),
    ("part_index", pa.int32()),
    ("attr_name", pa.string()),
    ("attr_type", pa.string()),
    ("value_json", pa.string()),
    ("value_text", pa.string()),
    ("value_int", pa.int64()),
    ("value_float", pa.float64()),
])

# AOV (Arbitrary Output Variable) table — one row per AOV per frame.
#
# Why this table exists separately from `channels`: AOVs are the renderer-
# meaningful groups (beauty, diffuse, AO, motion_vec, cryptomatte). Channels
# are the raw pixel arrays. The UI's "AOV Layer Map" panel groups by AOV.
#
# Re-render dedup pattern (no schema change to existing tables):
# `files.mtime` is ISO 8601 UTC and lexicographically sortable. The UI
# filters AOVs to the latest version per file_path_normalized via:
#
#   SELECT a.* FROM aovs a JOIN files f ON a.file_id = f.file_id
#   WHERE f.mtime = (
#     SELECT MAX(f2.mtime) FROM files f2
#     WHERE f2.file_path_normalized = f.file_path_normalized
#   )
#   AND f.file_path_normalized LIKE '<sequence_prefix>%'
#
# Sequence rollup (the panel header "N LAYERS · MULTI-CHANNEL EXR"):
#   SELECT name,
#          COUNT(DISTINCT depth_label) AS depth_variants,
#          ANY_VALUE(channel_group)    AS channels,
#          ANY_VALUE(depth_label)      AS depth,
#          SUM(uncompressed_bytes)     AS logical_bytes,
#          COUNT(DISTINCT frame_number) AS frame_count,
#          ANY_VALUE(is_beauty)        AS is_beauty
#   FROM <dedup query above>
#   GROUP BY name, view
#   ORDER BY is_beauty DESC, name;
#
# `depth_variants > 1` means this AOV's bit depth changed across the
# sequence — surface it in the UI rather than silently picking ANY_VALUE.
_AOVS_TABLE_SCHEMA = pa.schema([
    ("file_id", pa.string()),
    ("file_path", pa.string()),
    ("frame_number", pa.int32()),
    ("aov_index", pa.int32()),
    ("part_index", pa.int32()),
    ("view", pa.string()),
    ("name", pa.string()),
    ("name_normalized", pa.string()),
    ("channel_group", pa.string()),
    ("components", pa.list_(pa.string())),
    ("channel_count", pa.int32()),
    ("data_type", pa.string()),
    ("bit_depth", pa.int32()),
    ("depth_label", pa.string()),
    ("category", pa.string()),
    ("is_beauty", pa.bool_()),
    ("is_data", pa.bool_()),
    ("color_space", pa.string()),
    ("light_group", pa.string()),
    ("synthetic", pa.bool_()),
    ("uncompressed_bytes", pa.int64()),
    ("pct_of_frame_logical", pa.float64()),
    ("ranks", pa.int32()),
])

_TABLE_DEFINITIONS = {
    "files": _FILES_TABLE_SCHEMA,
    "parts": _PARTS_TABLE_SCHEMA,
    "channels": _CHANNELS_TABLE_SCHEMA,
    "attributes": _ATTRIBUTES_TABLE_SCHEMA,
    "aovs": _AOVS_TABLE_SCHEMA,
}


def _get_or_create_schema(bucket, schema_name: str):
    """Get existing schema or create it. Handles race conditions."""
    try:
        return bucket.schema(schema_name)
    except Exception:
        pass
    try:
        logger.info("Creating schema: %s", schema_name)
        return bucket.create_schema(schema_name)
    except Exception as exc:
        logger.warning("create_schema race condition (%s), retrying get", exc)
        return bucket.schema(schema_name)


def _get_or_create_table(schema, table_name: str, arrow_schema: pa.Schema):
    """Get existing table or create it. Handles race conditions."""
    try:
        return schema.table(table_name)
    except Exception:
        pass
    try:
        logger.info("Creating table: %s", table_name)
        return schema.create_table(table_name, arrow_schema)
    except Exception as exc:
        logger.warning("create_table race condition (%s), retrying get", exc)
        return schema.table(table_name)


def ensure_database_tables(session) -> None:
    """Ensure all required schema and tables exist in VAST DataBase.

    Safe to call on every invocation. Uses get-or-create pattern since
    vastdb create_schema/create_table are not idempotent.

    The bucket (database) must already exist as a Database-enabled view.
    DDL runs in its own transaction, separate from data inserts.
    """
    bucket_name = os.environ.get("VAST_DB_BUCKET", DEFAULT_VASTDB_BUCKET)
    schema_name = os.environ.get("VAST_DB_SCHEMA", DEFAULT_SCHEMA_NAME)

    with session.transaction() as tx:
        bucket = tx.bucket(bucket_name)
        schema = _get_or_create_schema(bucket, schema_name)

        for table_name, arrow_schema in _TABLE_DEFINITIONS.items():
            _get_or_create_table(schema, table_name, arrow_schema)

    logger.info("Database tables verified: %s/%s [%s]",
                bucket_name, schema_name, ", ".join(_TABLE_DEFINITIONS.keys()))


def _persist_with_transaction(
    session: Any,
    file_path: str,
    file_id: str,
    files_table: pa.Table,
    parts_table: pa.Table,
    channels_table: pa.Table,
    attributes_table: pa.Table,
    aovs_table: pa.Table,
    result: Dict[str, Any],
) -> None:
    """Execute idempotent upsert within a VAST SDK transaction.

    Checks if the file already exists by file_id. If found, updates
    audit fields (last_inspected, inspection_count). If not found,
    inserts all 4 tables.
    """
    bucket_name = os.environ.get("VAST_DB_BUCKET", DEFAULT_VASTDB_BUCKET)
    schema_name = os.environ.get("VAST_DB_SCHEMA", DEFAULT_SCHEMA_NAME)

    try:
        with session.transaction() as tx:
            schema = tx.bucket(bucket_name).schema(schema_name)
            files_tbl = schema.table("files")

            # Check if file already exists (SELECT by file_id)
            existing_count = 0
            old_inspection_count = 0
            try:
                import ibis
                reader = files_tbl.select(
                    columns=["file_id", "inspection_count"],
                    predicate=ibis.literal(file_id) == ibis._["file_id"],
                    limit_rows=1,
                )
                existing_rows = reader.read_all()
                # Validate we got a real Arrow table (not a mock)
                if hasattr(existing_rows, "num_rows") and isinstance(existing_rows.num_rows, int):
                    existing_count = existing_rows.num_rows
                    if existing_count > 0:
                        val = existing_rows.column("inspection_count")[0].as_py()
                        old_inspection_count = val if isinstance(val, int) else 0
            except Exception:
                existing_count = 0

            if existing_count > 0:
                # File already exists — update audit fields only
                now = datetime.now(timezone.utc).isoformat()
                old_count = old_inspection_count
                update_table = pa.table({
                    "file_id": [file_id],
                    "last_inspected": [now],
                    "inspection_count": [old_count + 1],
                })
                files_tbl.update(update_table)

                result["status"] = "success"
                result["file_id"] = file_id
                result["inserted"] = False
                result["message"] = f"File already exists, updated audit (count={old_count + 1}): {file_id}"
                logger.info(f"File updated (re-inspection #{old_count + 1}): {file_id}")
            else:
                # New file — insert all tables
                _insert_new_file(
                    tx,
                    file_id,
                    files_table,
                    parts_table,
                    channels_table,
                    attributes_table,
                    aovs_table,
                )

                result["status"] = "success"
                result["file_id"] = file_id
                result["inserted"] = True
                result["message"] = f"File persisted: {file_id}"
                logger.info(f"File inserted: {file_id}")

    except Exception as exc:
        raise VASTDatabaseError(f"Transaction failed: {exc}") from exc


def _insert_new_file(
    tx: Any,
    file_id: str,
    files_table: pa.Table,
    parts_table: pa.Table,
    channels_table: pa.Table,
    attributes_table: pa.Table,
    aovs_table: pa.Table,
) -> None:
    """Insert new file record and related data across all tables.

    All inserts run in the caller's transaction so AOV rows can never become
    orphans of a missing files row.
    """
    bucket_name = os.environ.get("VAST_DB_BUCKET", DEFAULT_VASTDB_BUCKET)
    schema_name = os.environ.get("VAST_DB_SCHEMA", DEFAULT_SCHEMA_NAME)

    try:
        schema = tx.bucket(bucket_name).schema(schema_name)

        schema.table("files").insert(files_table)
        logger.debug(f"Inserted files record for {file_id}")

        if parts_table.num_rows > 0:
            schema.table("parts").insert(parts_table)
            logger.debug(f"Inserted {parts_table.num_rows} part records")

        if channels_table.num_rows > 0:
            schema.table("channels").insert(channels_table)
            logger.debug(f"Inserted {channels_table.num_rows} channel records")

        if attributes_table.num_rows > 0:
            schema.table("attributes").insert(attributes_table)
            logger.debug(f"Inserted {attributes_table.num_rows} attribute records")

        if aovs_table.num_rows > 0:
            schema.table("aovs").insert(aovs_table)
            logger.debug(f"Inserted {aovs_table.num_rows} aov records")

    except Exception as exc:
        raise VASTDatabaseError(f"Insert failed for {file_id}: {exc}") from exc
