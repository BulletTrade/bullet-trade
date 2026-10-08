"""Small project-owned NCC loader for private digit PNG templates.

No templates are bundled. Each PNG must be named ``<digit>*.png`` or placed
inside a directory named for its digit. Ambiguous candidates abstain in the
existing FourDigitCandidateOCR gate.
"""

from __future__ import annotations

from pathlib import Path
import re


class NccTemplateEngine:
    TEMPLATE_SIZE = (20, 20)

    def __init__(self, template_dir):
        self.template_dir = Path(template_dir)
        self._template_matrix = None
        self._template_labels = None

    @staticmethod
    def _normalize(array):
        import numpy as np
        value = np.asarray(array, dtype=np.float32)
        value = value - value.mean()
        deviation = value.std()
        return value / deviation if deviation > 1e-6 else value * 0

    def warmup(self):
        import numpy as np
        from PIL import Image
        if self._template_matrix is not None:
            return True
        vectors, labels = [], []
        for path in sorted(self.template_dir.rglob("*.png")):
            label = path.parent.name if re.fullmatch(r"[0-9]", path.parent.name) else path.stem[:1]
            named = re.fullmatch(r"real_([0-9])_[0-9]+", path.stem)
            if named is not None:
                label = named[1]
            if re.fullmatch(r"[0-9]", label) is None:
                continue
            try:
                with Image.open(path) as image:
                    gray = image.convert("L").resize(self.TEMPLATE_SIZE, Image.Resampling.LANCZOS)
                    vectors.append(self._normalize(np.asarray(gray, dtype=np.float32) / 255).reshape(-1))
                    labels.append(int(label))
            except (OSError, ValueError):
                return False
        if set(labels) != set(range(10)):
            return False
        self._template_matrix = np.stack(vectors)
        self._template_labels = np.asarray(labels, dtype=np.int8)
        return True
