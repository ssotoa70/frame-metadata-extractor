"""Minimal DPX (SMPTE 268M) header parser.

OpenImageIO reads DPX pixel data correctly but silently drops several film /
tv header fields that matter in scan and finishing pipelines (creator,
project, film manufacturer, frame position, timecode, frame rate). This
module parses the fixed 2048-byte header with `struct.unpack` so those
fields are preserved.

Only the fields commonly used downstream are returned. The full spec is
SMPTE 268M-2014; see that document for field-by-field semantics.
"""

from __future__ import annotations

import struct
from typing import Any, Dict, Optional

# Magic values at offset 0 — determines byte order.
_MAGIC_BE = 0x53445058  # b"SDPX"
_MAGIC_LE = 0x58504453  # b"XPDS"

_UNDEF_U32 = 0xFFFFFFFF
_UNDEF_U16 = 0xFFFF
_UNDEF_U8 = 0xFF


def parse_dpx_header(raw: bytes) -> Optional[Dict[str, Any]]:
    """Parse a DPX header and return a flat dict of useful fields.

    Returns None if the buffer is not a valid DPX header.
    """
    if len(raw) < 2048:
        return None

    magic = struct.unpack(">I", raw[0:4])[0]
    if magic == _MAGIC_BE:
        endian = ">"
    elif magic == _MAGIC_LE:
        endian = "<"
    else:
        return None

    out: Dict[str, Any] = {}

    # --- Generic file information (offsets from SMPTE 268M) ---
    offset_to_image = struct.unpack(endian + "I", raw[4:8])[0]
    header_version = _c_string(raw[8:16])
    file_size = struct.unpack(endian + "I", raw[16:20])[0]
    ditto_key = struct.unpack(endian + "I", raw[20:24])[0]
    generic_header_size = struct.unpack(endian + "I", raw[24:28])[0]
    industry_header_size = struct.unpack(endian + "I", raw[28:32])[0]
    user_data_size = struct.unpack(endian + "I", raw[32:36])[0]
    file_name = _c_string(raw[36:136])
    creation_time = _c_string(raw[136:160])
    creator = _c_string(raw[160:260])
    project = _c_string(raw[260:460])
    copyright_ = _c_string(raw[460:660])
    encryption_key = struct.unpack(endian + "I", raw[660:664])[0]

    out.update(
        {
            "byte_order": "big" if endian == ">" else "little",
            "header_version": header_version,
            "file_size": _clean_u32(file_size),
            "offset_to_image": _clean_u32(offset_to_image),
            "generic_header_size": _clean_u32(generic_header_size),
            "industry_header_size": _clean_u32(industry_header_size),
            "user_data_size": _clean_u32(user_data_size),
            "file_name": file_name,
            "creation_time": creation_time,
            "creator": creator,
            "project": project,
            "copyright": copyright_,
            "encryption_key": None if encryption_key == _UNDEF_U32 else encryption_key,
            "ditto_key": _clean_u32(ditto_key),
        }
    )

    # --- Image information header (starts at offset 768) ---
    orientation = struct.unpack(endian + "H", raw[768:770])[0]
    number_of_elements = struct.unpack(endian + "H", raw[770:772])[0]
    pixels_per_line = struct.unpack(endian + "I", raw[772:776])[0]
    lines_per_element = struct.unpack(endian + "I", raw[776:780])[0]

    out.update(
        {
            "orientation": _clean_u16(orientation),
            "number_of_image_elements": _clean_u16(number_of_elements),
            "pixels_per_line": _clean_u32(pixels_per_line),
            "lines_per_element": _clean_u32(lines_per_element),
        }
    )

    # First image element descriptor (one of eight, each 72 bytes) at 780.
    element_base = 780
    data_sign = struct.unpack(endian + "I", raw[element_base + 0 : element_base + 4])[0]
    descriptor = raw[element_base + 800 - 780 : element_base + 801 - 780]
    # The per-element layout per SMPTE 268M:
    #   0..3    data_sign (u32)
    #   4..7    ref_low_data (u32)
    #   8..11   ref_low_quantity (float)
    #   12..15  ref_high_data (u32)
    #   16..19  ref_high_quantity (float)
    #   20      descriptor (u8)
    #   21      transfer (u8)
    #   22      colorimetric (u8)
    #   23      bit_size (u8)
    #   24..25  packing (u16)
    #   26..27  encoding (u16)
    #   28..31  offset_to_data (u32)
    descriptor_code = raw[element_base + 20]
    transfer_code = raw[element_base + 21]
    colorimetric_code = raw[element_base + 22]
    bit_size = raw[element_base + 23]
    packing = struct.unpack(endian + "H", raw[element_base + 24 : element_base + 26])[0]
    encoding = struct.unpack(endian + "H", raw[element_base + 26 : element_base + 28])[0]
    offset_to_data = struct.unpack(endian + "I", raw[element_base + 28 : element_base + 32])[0]

    out.update(
        {
            "data_sign": data_sign,
            "descriptor_code": _clean_u8(descriptor_code),
            "descriptor_name": _DPX_DESCRIPTORS.get(descriptor_code),
            "transfer_code": _clean_u8(transfer_code),
            "transfer_name": _DPX_TRANSFER.get(transfer_code),
            "colorimetric_code": _clean_u8(colorimetric_code),
            "colorimetric_name": _DPX_COLORIMETRIC.get(colorimetric_code),
            "bit_size": _clean_u8(bit_size),
            "packing": _clean_u16(packing),
            "encoding": _clean_u16(encoding),
            "element_offset_to_data": _clean_u32(offset_to_data),
        }
    )

    # --- Image source information header (starts at offset 1408) ---
    x_offset = struct.unpack(endian + "I", raw[1408:1412])[0]
    y_offset = struct.unpack(endian + "I", raw[1412:1416])[0]
    x_center = struct.unpack(endian + "f", raw[1416:1420])[0]
    y_center = struct.unpack(endian + "f", raw[1420:1424])[0]
    x_orig_size = struct.unpack(endian + "I", raw[1424:1428])[0]
    y_orig_size = struct.unpack(endian + "I", raw[1428:1432])[0]
    source_file_name = _c_string(raw[1432:1532])
    source_creation_time = _c_string(raw[1532:1556])
    input_device = _c_string(raw[1556:1588])
    input_device_serial = _c_string(raw[1588:1620])

    out.update(
        {
            "source_x_offset": _clean_u32(x_offset),
            "source_y_offset": _clean_u32(y_offset),
            "source_x_center": _clean_float(x_center),
            "source_y_center": _clean_float(y_center),
            "source_x_original_size": _clean_u32(x_orig_size),
            "source_y_original_size": _clean_u32(y_orig_size),
            "source_file_name": source_file_name,
            "source_creation_time": source_creation_time,
            "input_device": input_device,
            "input_device_serial": input_device_serial,
        }
    )

    # --- Motion-picture film information (starts at offset 1664) ---
    film_mfg_id = _c_string(raw[1664:1666])
    film_type = _c_string(raw[1666:1668])
    film_offset = _c_string(raw[1668:1670])
    prefix = _c_string(raw[1670:1676])
    count = _c_string(raw[1676:1680])
    film_format = _c_string(raw[1680:1712])
    frame_position = struct.unpack(endian + "I", raw[1712:1716])[0]
    sequence_length = struct.unpack(endian + "I", raw[1716:1720])[0]
    held_count = struct.unpack(endian + "I", raw[1720:1724])[0]
    frame_rate_film = struct.unpack(endian + "f", raw[1724:1728])[0]
    shutter_angle = struct.unpack(endian + "f", raw[1728:1732])[0]
    frame_id = _c_string(raw[1732:1764])
    slate_info = _c_string(raw[1764:1864])

    out.update(
        {
            "film_mfg_id": film_mfg_id,
            "film_type": film_type,
            "film_offset_in_perfs": film_offset,
            "film_prefix": prefix,
            "film_count": count,
            "film_format": film_format,
            "frame_position": _clean_u32(frame_position),
            "sequence_length": _clean_u32(sequence_length),
            "held_count": _clean_u32(held_count),
            "film_frame_rate": _clean_float(frame_rate_film),
            "shutter_angle": _clean_float(shutter_angle),
            "frame_id": frame_id,
            "slate_info": slate_info,
        }
    )

    # --- Television information header (starts at offset 1920) ---
    timecode_packed = struct.unpack(endian + "I", raw[1920:1924])[0]
    user_bits_packed = struct.unpack(endian + "I", raw[1924:1928])[0]
    interlace = raw[1928]
    field_number = raw[1929]
    video_standard = raw[1930]
    _pad = raw[1931]
    hsr = struct.unpack(endian + "f", raw[1932:1936])[0]
    tv_frame_rate = struct.unpack(endian + "f", raw[1936:1940])[0]
    time_offset = struct.unpack(endian + "f", raw[1940:1944])[0]
    gamma = struct.unpack(endian + "f", raw[1944:1948])[0]
    black_level = struct.unpack(endian + "f", raw[1948:1952])[0]
    black_gain = struct.unpack(endian + "f", raw[1952:1956])[0]
    breakpoint_ = struct.unpack(endian + "f", raw[1956:1960])[0]
    white_level = struct.unpack(endian + "f", raw[1960:1964])[0]
    integration_times = struct.unpack(endian + "f", raw[1964:1968])[0]

    out.update(
        {
            "timecode": _decode_timecode(timecode_packed),
            "user_bits": _clean_u32(user_bits_packed),
            "interlace": _clean_u8(interlace),
            "field_number": _clean_u8(field_number),
            "video_standard_code": _clean_u8(video_standard),
            "horizontal_sampling_rate": _clean_float(hsr),
            "frame_rate": _clean_float(tv_frame_rate),
            "time_offset": _clean_float(time_offset),
            "gamma": _clean_float(gamma),
            "black_level_code": _clean_float(black_level),
            "black_gain_code": _clean_float(black_gain),
            "breakpoint_code": _clean_float(breakpoint_),
            "white_level_code": _clean_float(white_level),
            "integration_time": _clean_float(integration_times),
        }
    )

    return {k: v for k, v in out.items() if v not in (None, "")}


# ---------------------------------------------------------------------------
# SMPTE 268M code tables (subset — the values pipelines actually query)
# ---------------------------------------------------------------------------

_DPX_DESCRIPTORS: Dict[int, str] = {
    0: "user-defined",
    1: "red",
    2: "green",
    3: "blue",
    4: "alpha",
    6: "luma",
    7: "color-difference",
    8: "depth",
    9: "composite-video",
    50: "rgb",
    51: "rgba",
    52: "abgr",
    100: "cbycry422",
    101: "cbycray4224",
    102: "cbycr444",
    103: "cbycra4444",
}

_DPX_TRANSFER: Dict[int, str] = {
    0: "user-defined",
    1: "printing-density",
    2: "linear",
    3: "logarithmic",
    4: "unspecified-video",
    5: "smpte-274m",
    6: "itu-r-709",
    7: "itu-r-601-625",
    8: "itu-r-601-525",
    9: "ntsc-composite-video",
    10: "pal-composite-video",
    11: "z-linear",
    12: "z-homogeneous",
}

_DPX_COLORIMETRIC: Dict[int, str] = {
    0: "user-defined",
    1: "printing-density",
    4: "unspecified-video",
    5: "smpte-274m",
    6: "itu-r-709",
    7: "itu-r-601-625",
    8: "itu-r-601-525",
    9: "ntsc",
    10: "pal",
}


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------


def _c_string(buf: bytes) -> str:
    end = buf.find(b"\x00")
    if end >= 0:
        buf = buf[:end]
    try:
        return buf.decode("ascii", errors="replace").strip()
    except Exception:
        return ""


def _clean_u32(value: int) -> Optional[int]:
    return None if value == _UNDEF_U32 else int(value)


def _clean_u16(value: int) -> Optional[int]:
    return None if value == _UNDEF_U16 else int(value)


def _clean_u8(value: int) -> Optional[int]:
    return None if value == _UNDEF_U8 else int(value)


def _clean_float(value: float) -> Optional[float]:
    import math

    if value is None or math.isnan(value) or math.isinf(value):
        return None
    # SMPTE 268M "undefined float" is all-1 bits = NaN, so the above catches it.
    return float(value)


def _decode_timecode(packed: int) -> Optional[str]:
    """Decode packed BCD timecode into HH:MM:SS:FF string."""
    if packed == _UNDEF_U32:
        return None
    try:
        # SMPTE 12M BCD layout: HHhh MMmm SSss FFff (nibble-per-digit, MSB first)
        h_tens = (packed >> 28) & 0x0F
        h_ones = (packed >> 24) & 0x0F
        m_tens = (packed >> 20) & 0x0F
        m_ones = (packed >> 16) & 0x0F
        s_tens = (packed >> 12) & 0x0F
        s_ones = (packed >> 8) & 0x0F
        f_tens = (packed >> 4) & 0x0F
        f_ones = packed & 0x0F
        hh = h_tens * 10 + h_ones
        mm = m_tens * 10 + m_ones
        ss = s_tens * 10 + s_ones
        ff = f_tens * 10 + f_ones
        if hh > 23 or mm > 59 or ss > 59 or ff > 99:
            return None
        return f"{hh:02d}:{mm:02d}:{ss:02d}:{ff:02d}"
    except Exception:
        return None
