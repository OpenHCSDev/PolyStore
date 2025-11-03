from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any


class ZarrChunkStrategy(Enum):
    WELL = 'well'
    FILE = 'file'


@dataclass
class CompressorConfig:
    """Minimal compressor config used by Zarr backend when real compressors aren't provided."""
    name: str = 'none'

    def create_compressor(self, level: Optional[int], shuffle: bool = True) -> Optional[Any]:
        """Return a compressor object acceptable to zarr or None to disable compression."""
        # Minimal fallback: return None (no compression)
        return None


@dataclass
class ZarrConfig:
    """Minimal Zarr configuration dataclass for polystore (OpenHCS-agnostic)."""
    compression_level: Optional[int] = None
    compressor: CompressorConfig = field(default_factory=CompressorConfig)
    chunk_strategy: ZarrChunkStrategy = ZarrChunkStrategy.WELL
