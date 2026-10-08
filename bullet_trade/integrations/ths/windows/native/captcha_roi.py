"""复制验证码控件 2405 的局部 PNG 离线质量检查。

当前已记录的 ROI 为 72x32；本检查允许少量尺寸漂移以供诊断，
不证明窗口/控件身份，也不代替仅接受 72x32 的 OCR 或客户端验证。
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

from PIL import Image, UnidentifiedImageError


MIN_SIZE = (64, 28)
MAX_SIZE = (80, 36)
MAX_PNG_BYTES = 1_000_000
INK_THRESHOLD = 200
MIN_INK_PIXELS = 8
MIN_CONTRAST = 20
MAX_INK_FRACTION = 0.75


@dataclass(frozen=True)
class RoiCheck:
    ok: bool
    reason: str
    size: tuple[int, int] | None = None
    ink_pixels: int | None = None


def _heavy_edge_band(ink: list[bool], width: int, height: int) -> bool:
    """Reject only broad, continuous-looking edge occlusion/crop evidence.

    A thin digit stroke or an isolated speck at an edge is insufficient.
    This heuristic intentionally cannot detect every possible occlusion.
    """
    def column(x: int) -> float:
        return sum(ink[y * width + x] for y in range(height)) / height

    def row(y: int) -> float:
        return sum(ink[y * width + x] for x in range(width)) / width

    return (column(0) >= 0.8 and column(1) >= 0.65
            or column(width - 1) >= 0.8 and column(width - 2) >= 0.65
            or row(0) >= 0.8 and row(1) >= 0.65
            or row(height - 1) >= 0.8 and row(height - 2) >= 0.65)


def check_roi_png(image_bytes: bytes) -> RoiCheck:
    """Decode one PNG and report conservative geometry/content quality only."""
    if not isinstance(image_bytes, bytes) or not image_bytes or len(image_bytes) > MAX_PNG_BYTES:
        return RoiCheck(False, "invalid_png_size")
    try:
        with Image.open(BytesIO(image_bytes)) as image:
            if image.format != "PNG":
                return RoiCheck(False, "not_png")
            width, height = image.size
            size = (width, height)
            if not (MIN_SIZE[0] <= width <= MAX_SIZE[0]
                    and MIN_SIZE[1] <= height <= MAX_SIZE[1]):
                return RoiCheck(False, "roi_size_out_of_bounds", size)
            image.load()  # Force full decode; reject truncated PNG streams.
            rgba = image.convert("RGBA")
            white = Image.new("RGBA", size, (255, 255, 255, 255))
            gray = Image.alpha_composite(white, rgba).convert("L")
            pixels = list(gray.tobytes())
    except (OSError, ValueError, UnidentifiedImageError):
        return RoiCheck(False, "png_decode_failed")

    darkest, brightest = min(pixels), max(pixels)
    if brightest - darkest < MIN_CONTRAST:
        return RoiCheck(False, "single_color_or_low_contrast", size, 0)
    ink = [value < INK_THRESHOLD for value in pixels]
    ink_pixels = sum(ink)
    if ink_pixels < MIN_INK_PIXELS:
        return RoiCheck(False, "blank_or_near_blank", size, ink_pixels)
    if ink_pixels / len(ink) > MAX_INK_FRACTION:
        return RoiCheck(False, "mostly_occluded", size, ink_pixels)
    if _heavy_edge_band(ink, width, height):
        return RoiCheck(False, "severe_edge_clipping", size, ink_pixels)
    return RoiCheck(True, "ok", size, ink_pixels)
