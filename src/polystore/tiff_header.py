"""TIFF header access without decoding pixels or interpreting acquisition XML."""

from dataclasses import dataclass
from pathlib import Path

from .formats import FileFormat


@dataclass(frozen=True, slots=True)
class TiffImageHeader:
    """Exact first-page container description and array shape."""

    description: str | None
    shape: tuple[int, ...]
    page_count: int

    @classmethod
    def read(cls, path: Path) -> "TiffImageHeader | None":
        if path.suffix.lower() not in FileFormat.TIFF.extensions:
            return None
        tifffile = FileFormat.TIFF.load_dependency()
        try:
            image = tifffile.TiffFile(path)
        except (OSError, tifffile.TiffFileError):
            return None
        with image:
            page = image.pages[0]
            return cls(
                description=page.description,
                shape=tuple(page.shape),
                page_count=len(image.pages),
            )
