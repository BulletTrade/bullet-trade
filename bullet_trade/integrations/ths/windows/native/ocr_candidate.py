"""Offline, conservative adapter for the reference template OCR on 72x32 copy ROIs.

The reference engine's ``_segment_digits`` drops groups narrower than four
pixels. In this ROI format, a digit can be a two-column vertical stroke. This
adapter changes only segmentation and adds per-digit score/ambiguity gates.
It neither learns from images nor writes to the template directory.

Pass an explicitly loaded ``LightweightCaptchaOCR`` instance. Reference asset
licensing and product adoption are separate decisions.
"""
from __future__ import annotations

from io import BytesIO
import re

import numpy as np
from PIL import Image, UnidentifiedImageError


class FourDigitCandidateOCR:
    """Return four ASCII digits only when geometry and template scores agree."""

    ROI_SIZE = (72, 32)
    BIN_THRESHOLD = 200
    MIN_NCC = 0.50
    # The observed two-column "1" in the 2026-09-29 ROI scores 0.540 vs
    # 0.510 for "4"; 0.025 retains a margin while avoiding that abstention.
    MIN_DIGIT_MARGIN = 0.025

    def __init__(self, engine: object):
        self.engine = engine

    @staticmethod
    def _groups(projected: np.ndarray) -> list[tuple[int, int]]:
        raw: list[tuple[int, int]] = []
        start = None
        for col, count in enumerate(projected):
            if count >= 2 and start is None:
                start = col
            elif count < 2 and start is not None:
                raw.append((start, col))
                start = None
        if start is not None:
            raw.append((start, len(projected)))
        merged: list[list[int]] = []
        for left, right in raw:
            if merged and left - merged[-1][1] <= 5:
                merged[-1][1] = right
            else:
                merged.append([left, right])
        return [(left, right) for left, right in merged]

    def _regions(self, image: Image.Image) -> list[np.ndarray]:
        gray = np.asarray(image.convert("L"), dtype=np.uint8)
        ink = gray < self.BIN_THRESHOLD
        groups = self._groups(ink.sum(axis=0))
        # Never discard extra groups or invent a missing one.
        if len(groups) != 4:
            return []
        dark_rows = np.flatnonzero(ink.sum(axis=1) >= 2)
        if len(dark_rows) == 0:
            return []
        top = max(0, int(dark_rows[0]) - 1)
        bottom = min(gray.shape[0], int(dark_rows[-1]) + 2)
        if bottom - top < 10:
            return []

        regions = []
        for left, right in groups:
            width = right - left
            # A two-column glyph is allowed only when it has enough ink to be
            # a continuous stroke; isolated specks must not become a digit.
            if not 2 <= width <= 12 or int(ink[top:bottom, left:right].sum()) < 12:
                return []
            cropped = gray[top:bottom, max(0, left - 1):min(gray.shape[1], right + 1)]
            resized = Image.fromarray(cropped).resize(self.engine.TEMPLATE_SIZE, Image.Resampling.LANCZOS)
            regions.append(np.asarray(resized, dtype=np.float32) / 255.0)
        return regions

    def _digit(self, region: np.ndarray) -> str:
        matrix = self.engine._template_matrix
        labels = self.engine._template_labels
        if matrix is None or labels is None or matrix.ndim != 2 or len(matrix) != len(labels):
            return ""
        normalized = self.engine._normalize(region).reshape(-1)
        if matrix.shape[1] != len(normalized) or not np.isfinite(normalized).all():
            return ""
        scores = (matrix @ normalized) / len(normalized)
        if not np.isfinite(scores).all():
            return ""
        # Compare distinct digits, not two near-identical templates of one digit.
        by_digit = [float(scores[labels == digit].max()) if np.any(labels == digit) else float("-inf")
                    for digit in range(10)]
        ranked = sorted(range(10), key=lambda digit: by_digit[digit], reverse=True)
        best, second = ranked[:2]
        if by_digit[best] < self.MIN_NCC or by_digit[best] - by_digit[second] < self.MIN_DIGIT_MARGIN:
            return ""
        return str(best)

    def recognize_bytes(self, image_data: bytes) -> str:
        if not isinstance(image_data, bytes):
            return ""
        try:
            with Image.open(BytesIO(image_data)) as image:
                if image.size != self.ROI_SIZE:
                    return ""
                image.load()
                if not self.engine.warmup():
                    return ""
                regions = self._regions(image)
            if len(regions) != 4:
                return ""
            candidate = "".join(self._digit(region) for region in regions)
            return candidate if re.fullmatch(r"[0-9]{4}", candidate) else ""
        except (OSError, ValueError, TypeError, AttributeError, UnidentifiedImageError):
            return ""

    def recognize(self, image_data: bytes) -> str:
        """Match the read-only solver's candidate provider contract."""
        return self.recognize_bytes(image_data)
