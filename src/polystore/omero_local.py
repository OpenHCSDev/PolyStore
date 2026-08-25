# polystore/omero_local.py
"""
OMERO Local Storage Backend - Zero-copy server-side OMERO access.

Reads directly from OMERO binary repository, saves results back to OMERO.
"""

import logging
import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

import numpy as np

from .array_payload import storage_numpy_array
from .atomic import file_lock
from .base import PicklableBackend, VirtualBackend
from .omero_tables import OMERO_TABLE_SERVICE, OMEROTableColumnType
from .omero_text import OMEROTextFormat

logger = logging.getLogger(__name__)

DEFAULT_OMERO_DESCRIPTION = os.getenv("POLYSTORE_OMERO_DESCRIPTION", "Processed by Polystore")
DEFAULT_OMERO_WELL_DESCRIPTION_TEMPLATE = os.getenv(
    "POLYSTORE_OMERO_WELL_DESCRIPTION_TEMPLATE",
    "Processed image for well {well_id}, site {site}",
)


@dataclass
class ImageStructure:
    """Metadata for a single OMERO image."""

    image_id: int
    sizeZ: int
    sizeC: int
    sizeT: int
    sizeY: int
    sizeX: int


@dataclass
class WellStructure:
    """Metadata for a single well."""

    sites: dict[int, ImageStructure]  # site_idx → ImageStructure


@dataclass
class PlateStructure:
    """Lightweight metadata for entire plate."""

    plate_id: int
    parser_name: str
    microscope_type: str
    wells: dict[str, WellStructure]  # well_id → WellStructure

    # Cached for quick access
    all_well_ids: set[str]
    max_sites: int
    max_z: int
    max_c: int
    max_t: int


@dataclass
class ImagePlaneBatch:
    """Own one OMERO image's normalized planes and declared ZCT dimensions."""

    planes: dict[tuple[int, int, int], np.ndarray] = field(default_factory=dict)
    size_z: int = 0
    size_c: int = 0
    size_t: int = 0

    def add(self, *, z: int, c: int, t: int, data: Any) -> None:
        """Normalize and add one plane at its zero-based OMERO coordinate."""

        plane = storage_numpy_array(data)
        if plane.ndim != 2:
            raise ValueError(f"OMERO image planes must be 2D, got shape {plane.shape}")
        self.planes[(z, c, t)] = plane
        self.size_z = max(self.size_z, z + 1)
        self.size_c = max(self.size_c, c + 1)
        self.size_t = max(self.size_t, t + 1)

    def iter_omero_planes(self):
        """Yield a rectangular, dtype-consistent plane sequence in ZCT order."""

        if not self.planes:
            raise ValueError("Cannot create an OMERO image without planes")

        height = max(plane.shape[0] for plane in self.planes.values())
        width = max(plane.shape[1] for plane in self.planes.values())
        dtype = np.result_type(*(plane.dtype for plane in self.planes.values()))

        for t in range(self.size_t):
            for c in range(self.size_c):
                for z in range(self.size_z):
                    plane = self.planes.get((z, c, t))
                    if plane is None:
                        yield np.zeros((height, width), dtype=dtype)
                        continue

                    normalized = np.asarray(plane, dtype=dtype)
                    if normalized.shape == (height, width):
                        yield normalized
                        continue

                    padded = np.zeros((height, width), dtype=dtype)
                    padded[: normalized.shape[0], : normalized.shape[1]] = normalized
                    yield padded


class OMEROLocalBackend(VirtualBackend, PicklableBackend):
    """
    Virtual backend for OMERO server-side execution.

    Generates filenames on-demand from OMERO plate structure.
    No real filesystem operations - all paths are virtual.

    Implements PicklableBackend to support multiprocessing by preserving
    connection parameters across process boundaries.
    """

    _backend_type = "omero_local"

    def resolve_listed_address(
        self,
        listed_address: str | Path,
        *,
        directory: str | Path,
    ) -> str:
        """Qualify a generated filename within its OMERO virtual directory."""

        address = PurePosixPath(str(listed_address).replace("\\", "/"))
        if address.is_absolute():
            return address.as_posix()
        base = PurePosixPath(str(directory).replace("\\", "/"))
        return (base / address).as_posix()

    # Class-level lock dictionary for thread-safe well creation
    _well_locks: dict[str, threading.Lock] = {}
    _well_locks_lock = threading.Lock()  # Lock for the lock dictionary itself

    def contextual_save_kwargs(
        self,
        *,
        images_dir: str | None,
    ) -> Mapping[str, Any]:
        """Bind save context from the source plate represented by the workspace."""

        if images_dir is None:
            raise ValueError("images_dir required for OMERO contextual save")

        _, base_id, _ = self._parse_omero_path(Path(images_dir))
        if base_id not in self._plate_metadata:
            self._load_plate_structure(base_id)

        plate = self._plate_metadata[base_id]
        return {
            "images_dir": images_dir,
            "parser_name": plate.parser_name,
            "microscope_type": plate.microscope_type,
        }

    def __init__(
        self,
        omero_data_dir: Path | None = None,
        omero_conn=None,
        namespace_prefix: str = "polystore",
        lock_dir_name: str = ".polystore",
    ):
        try:
            from omero.gateway import BlitzGateway

            self._BlitzGateway = BlitzGateway
        except ImportError as error:
            raise ImportError("omero-py required: pip install omero-py") from error

        if omero_data_dir:
            omero_data_dir = Path(omero_data_dir)
            if not omero_data_dir.exists():
                raise ValueError(f"OMERO data directory not found: {omero_data_dir}")

        self.omero_data_dir = omero_data_dir
        # DO NOT store omero_conn - it contains unpicklable IcePy.Communicator
        # Connection must be passed via kwargs or retrieved from global registry
        self._initial_conn = omero_conn  # Store temporarily for registration

        # Store connection parameters for reconnection in worker processes
        self._conn_params = None
        if omero_conn is not None:
            user = omero_conn.getUser()
            if user is None:
                raise ValueError("Connected OMERO gateway has no authenticated user")
            self._conn_params = {
                "host": omero_conn.host,
                "port": omero_conn.port,
                "username": user.getName(),
                # Password is intentionally obtained from the worker environment.
            }

        # Caches for virtual filesystem
        self._plate_metadata: dict[int, PlateStructure] = {}
        self._parser_cache: dict[int, Any] = {}  # plate_id → parser instance
        self._plate_name_cache: dict[str, int] = {}  # plate_name → plate_id

        # Namespace configuration
        self._namespace_prefix = namespace_prefix
        self._metadata_namespace = f"{namespace_prefix}.metadata"
        self._analysis_namespace = f"{namespace_prefix}.analysis.results"
        self._analysis_table_namespace = f"{namespace_prefix}.analysis.results.table"
        self._provenance_namespace = f"{namespace_prefix}.provenance"
        self._parser_key = f"{namespace_prefix}.parser"
        self._microscope_key = f"{namespace_prefix}.microscope_type"
        self._lock_dir_name = lock_dir_name

        # Parser registry - use auto-discovered FilenameParser registry from openhcs
        from openhcs.microscopes.microscope_interfaces import FilenameParser

        self._parser_registry = FilenameParser.__registry__

    def __getstate__(self):
        """Exclude unpicklable connection from pickle."""
        state = self.__dict__.copy()
        # Remove unpicklable connection
        state["_initial_conn"] = None
        return state

    def __setstate__(self, state):
        """Restore state after unpickling."""
        self.__dict__.update(state)
        # Connection will be retrieved from global registry in worker process

    def get_connection_params(self) -> dict[str, Any] | None:
        """
        Return connection parameters for worker process reconnection.

        Implements PicklableBackend protocol.

        Returns:
            Dictionary of connection parameters (host, port, username)
            or None if no connection parameters are available.
        """
        return self._conn_params

    def set_connection_params(self, params: dict[str, Any] | None) -> None:
        """
        Set connection parameters (used during unpickling).

        Implements PicklableBackend protocol.

        Args:
            params: Dictionary of connection parameters or None
        """
        self._conn_params = params

    def _get_connection(self, **kwargs):
        """
        Get an explicit OMERO connection or reconnect from serialized parameters.

        The instance connection is authoritative in its creating process. Pickled
        workers reconnect from the parameters declared by PicklableBackend.
        """
        conn = kwargs.get("omero_conn")
        if conn is None and self._initial_conn is not None:
            conn = self._initial_conn

        if conn is None and self._conn_params is not None:
            logger.info(
                "Creating new OMERO connection to %s:%s",
                self._conn_params["host"],
                self._conn_params["port"],
            )
            conn = self._BlitzGateway(
                self._conn_params["username"],
                os.getenv("OMERO_PASSWORD", "polystore"),
                host=self._conn_params["host"],
                port=self._conn_params["port"],
            )
            if not conn.connect():
                raise ConnectionError(
                    "Failed to connect to OMERO at "
                    f"{self._conn_params['host']}:{self._conn_params['port']}"
                )
            self._initial_conn = conn
            logger.info("Successfully connected to OMERO")

        if conn is None:
            raise ValueError(
                "No OMERO connection available. "
                "Pass omero_conn, construct the backend with one, or provide "
                "PicklableBackend connection parameters."
            )
        return conn

    def _ensure_connection(self, **kwargs):
        """Validate OMERO connection is available."""
        self._get_connection(**kwargs)

    def _plate_metadata_value(self, plate, key: str) -> str:
        """Return one value from the plate's declared OpenHCS map annotation."""

        from omero.gateway import MapAnnotationWrapper

        for annotation in plate.listAnnotations():
            if not isinstance(annotation, MapAnnotationWrapper):
                continue
            if annotation.getNs() != self._metadata_namespace:
                continue
            metadata = dict(annotation.getValue())
            if key in metadata:
                return str(metadata[key])

        raise ValueError(f"Plate {plate.getId()} missing {key} metadata")

    def _load_parser(self, parser_name: str):
        """Dynamically load parser class by name."""
        parser_entry = self._parser_registry.get(parser_name)
        if parser_entry is None:
            raise ValueError(
                f"Unknown parser: {parser_name}. "
                f"Available parsers: {list(self._parser_registry.keys())}"
            )

        if isinstance(parser_entry, type):
            return parser_entry()
        if callable(parser_entry):
            return parser_entry()
        return parser_entry

    def _load_plate_structure(self, plate_id: int, **kwargs) -> None:
        """
        Query OMERO once to build lightweight plate structure.

        Args:
            plate_id: OMERO Plate ID
            **kwargs: Must include omero_conn

        Raises:
            ValueError: If plate not found or missing metadata
        """
        import time

        conn = self._get_connection(**kwargs)

        # Query OMERO for plate with retry mechanism
        # Plates may need time to become available after upload
        max_retries = 30
        retry_delay = 1.0

        for attempt in range(max_retries):
            plate = conn.getObject("Plate", plate_id)
            if plate:
                break
            if attempt < max_retries - 1:
                logger.info(
                    f"Plate {plate_id} not found yet, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(retry_delay)
            else:
                raise ValueError(f"OMERO Plate not found after {max_retries} retries: {plate_id}")

        # Get parser metadata
        parser_name = self._plate_metadata_value(plate, self._parser_key)
        microscope_type = self._plate_metadata_value(plate, self._microscope_key)

        # Load parser (cache it)
        if plate_id not in self._parser_cache:
            self._parser_cache[plate_id] = self._load_parser(parser_name)

        # Build structure
        wells = {}
        all_well_ids = set()
        max_sites = 0
        max_z = 0
        max_c = 0
        max_t = 0

        for well in plate.listChildren():
            row = int(well.row)
            col = int(well.column)
            well_id = f"{chr(ord('A') + row)}{col + 1:02d}"
            all_well_ids.add(well_id)

            sites = {}
            for site_idx_0based, wellsample in enumerate(well.listChildren()):
                image = wellsample.getImage()
                # Use enumeration order as site index (0-based)
                # Convert to 1-based indexing for downstream consumers
                site_idx = site_idx_0based + 1

                image_struct = ImageStructure(
                    image_id=image.getId(),
                    sizeZ=image.getSizeZ(),
                    sizeC=image.getSizeC(),
                    sizeT=image.getSizeT(),
                    sizeY=image.getSizeY(),
                    sizeX=image.getSizeX(),
                )
                sites[site_idx] = image_struct

                # Track maximums
                max_sites = max(max_sites, site_idx)
                max_z = max(max_z, image.getSizeZ())
                max_c = max(max_c, image.getSizeC())
                max_t = max(max_t, image.getSizeT())

            wells[well_id] = WellStructure(sites=sites)

        # Store structure
        self._plate_metadata[plate_id] = PlateStructure(
            plate_id=plate_id,
            parser_name=parser_name,
            microscope_type=microscope_type,
            wells=wells,
            all_well_ids=all_well_ids,
            max_sites=max_sites,
            max_z=max_z,
            max_c=max_c,
            max_t=max_t,
        )

        logger.info(
            f"Loaded plate structure for {plate_id}: "
            f"{len(wells)} wells, {max_sites} sites, "
            f"{max_z}Z × {max_c}C × {max_t}T"
        )

    def load(self, file_path: str | Path, **kwargs) -> np.ndarray:
        """
        Load by parsing filename to extract coordinates, then lookup image.

        Flow: path → extract plate_id → filename → parse → (well, site, z, c, t) → lookup structure → image_id → load plane

        Args:
            file_path: Full path including plate_id (e.g., "/omero/plate_59/A01_s001_w1_z001_t001.tif")
            **kwargs: Additional backend-specific arguments (unused)

        Returns:
            2D numpy array (single z-plane, single channel, single timepoint)
        """
        # Extract plate_id from path using parent directory
        # Path format: /omero/plate_59/A01_s001_w1_z001_t001.tif
        path_obj = Path(file_path)
        plate_dir = path_obj.parent  # /omero/plate_59

        # Extract plate_id from plate directory name
        import re

        plate_dir_name = plate_dir.name  # "plate_59" or "plate_59_outputs"
        match = re.match(r"plate_(\d+)", plate_dir_name)
        if not match:
            raise ValueError(
                f"Could not extract plate_id from path: {file_path}. Expected /omero/plate_<id>/filename format"
            )

        plate_id = int(match.group(1))

        # Extract filename
        filename = path_obj.name

        # Ensure plate structure is loaded
        if plate_id not in self._plate_metadata:
            self._load_plate_structure(plate_id, **kwargs)

        plate_struct = self._plate_metadata[plate_id]
        parser = self._parser_cache[plate_id]

        # Parse filename to extract components
        parsed = parser.parse_filename(filename)
        if not parsed:
            raise ValueError(f"Cannot parse filename: {filename}")

        well_id = parsed["well"]
        site_idx = parsed["site"]
        z_idx = parsed["z_index"] - 1  # Convert to 0-based
        c_idx = parsed["channel"] - 1
        t_idx = parsed["timepoint"] - 1

        # Lookup image_id from structure
        if well_id not in plate_struct.wells:
            raise ValueError(f"Well {well_id} not found in plate {plate_id}")

        well_struct = plate_struct.wells[well_id]
        if site_idx not in well_struct.sites:
            raise ValueError(f"Site {site_idx} not found in well {well_id}")

        image_struct = well_struct.sites[site_idx]

        # Validate coordinates
        if z_idx >= image_struct.sizeZ:
            raise ValueError(f"Z-index {z_idx} out of range (max: {image_struct.sizeZ})")
        if c_idx >= image_struct.sizeC:
            raise ValueError(f"Channel {c_idx} out of range (max: {image_struct.sizeC})")
        if t_idx >= image_struct.sizeT:
            raise ValueError(f"Timepoint {t_idx} out of range (max: {image_struct.sizeT})")

        # Load plane from OMERO
        conn = self._get_connection(**kwargs)
        image = conn.getObject("Image", image_struct.image_id)
        if not image:
            raise ValueError(f"OMERO Image not found: {image_struct.image_id}")

        pixels = image.getPrimaryPixels()
        plane = pixels.getPlane(z_idx, c_idx, t_idx)  # Returns 2D numpy array

        logger.debug(
            f"Loaded {filename} → image {image_struct.image_id}, "
            f"z={z_idx}, c={c_idx}, t={t_idx}, shape={plane.shape}"
        )

        return plane

    def save(self, data: Any, output_path: str | Path, **kwargs) -> None:
        """
        Save data to OMERO.

        For ROI data (List[ROI]): Creates OMERO ROI objects linked to images
        For image data (numpy arrays): Creates a new image in a dataset
        For tabular data (CSV/JSON/TXT): Attempts to parse and save as OMERO.table (queryable structured data)
        For other text data: Creates a FileAnnotation attached to plate/well/image

        Args:
            data: Data to save
            output_path: Output path
            **kwargs: Additional arguments, including:
                - images_dir: Directory containing images (required for analysis results to link to correct plate)
                - dataset_id: Dataset ID for image data
        """
        output_path = Path(output_path)

        # Explicit type dispatch - fail-loud
        if self._is_roi_payload(data):
            # ROI data - save as OMERO ROI objects
            images_dir = kwargs.pop("images_dir", None)
            self._save_rois(data, output_path, images_dir=images_dir, **kwargs)
        elif self._is_text_payload(data, output_path):
            # Try to parse as tabular data and save as OMERO.table
            # Extract images_dir from kwargs if present (passed via filemanager context)
            # Remove it from kwargs to avoid duplicate keyword argument error
            images_dir = kwargs.pop("images_dir", None)
            self._save_as_table_or_annotation(data, output_path, images_dir=images_dir, **kwargs)
        else:
            # Image data - save as OMERO image
            self._save_image(data, output_path, **kwargs)

    @staticmethod
    def _is_roi_payload(data: Any) -> bool:
        """Return whether one payload uses OMERO's ROI save path."""
        from .roi import ROI

        return isinstance(data, list) and bool(data) and isinstance(data[0], ROI)

    def _is_text_payload(self, data: Any, output_path: Path) -> bool:
        """Return whether one payload uses OMERO's annotation/table save path."""
        return isinstance(data, str) and OMEROTextFormat.for_path(output_path) is not None

    def _save_as_table_or_annotation(
        self, text_content: str, output_path: Path, images_dir: str = None, **kwargs
    ) -> None:
        """
        Try to parse text content as tabular data and save as OMERO.table.
        If parsing fails, fall back to FileAnnotation.

        Args:
            text_content: Text content to save
            output_path: Output path
            images_dir: Directory containing images (required for analysis results to link to correct plate)
            **kwargs: Additional arguments
        """
        table = OMEROTextFormat.require_path(output_path).table(text_content)
        if table is None:
            self._save_text_annotation(
                text_content,
                output_path,
                images_dir=images_dir,
                **kwargs,
            )
            return

        self._save_csv_as_table(
            table.to_csv(index=False),
            output_path,
            images_dir=images_dir,
            **kwargs,
        )

    def _save_csv_as_table(
        self, csv_content: str, output_path: Path, images_dir: str = None, **kwargs
    ) -> None:
        """
        Save CSV content as an OMERO.table (queryable structured data).

        Tables are linked to the appropriate OMERO object (Plate, Well, or Image).

        Args:
            csv_content: CSV content to save
            output_path: Output path (used for table name)
            images_dir: Directory containing images (required to link table to correct plate)
            **kwargs: Additional arguments
        """
        from io import StringIO

        import pandas as pd
        from omero.model import FileAnnotationI
        from omero.rtypes import rstring

        conn = self._get_connection(**kwargs)

        # Parse CSV content into pandas DataFrame
        df = pd.read_csv(StringIO(csv_content))

        # Validate images_dir is provided
        if not images_dir:
            raise ValueError(
                f"images_dir is required for OMERO table linking. "
                f"This should be passed from the materialization context. "
                f"Output path: {output_path}"
            )

        # Parse the images directory path to get the plate name, then query OMERO for actual plate ID
        # Path format: /omero/plate_274_outputs/images/
        # The path contains the INPUT plate ID (274), but we need the OUTPUT plate ID
        # We must parse the full plate name and query OMERO to get the actual ID
        images_dir = Path(images_dir)
        plate_name, _, _ = self._parse_omero_path(images_dir)

        # Query OMERO for the actual plate ID by name
        plate_id = self._find_plate_by_name(plate_name, **kwargs)
        if plate_id is None:
            raise ValueError(f"Plate '{plate_name}' not found in OMERO (images dir: {images_dir})")

        # Determine table name from filename
        # Remove ALL extensions (e.g., "file.roi.zip.json" -> "file")
        # OMERO table names cannot contain dots except for the .h5 extension
        table_name = output_path.name.split(".")[0]

        columns = [
            OMEROTableColumnType.column_for(col_name, df[col_name]) for col_name in df.columns
        ]

        table = OMERO_TABLE_SERVICE.create_table(conn, f"{table_name}.h5")

        try:
            # Initialize table with columns
            table.initialize(columns)

            # Add all rows
            table.addData(columns)

            # Get the OriginalFile for the table
            orig_file = table.getOriginalFile()

            # Create FileAnnotation to link the table
            file_ann = FileAnnotationI()
            file_ann.setFile(orig_file)
            file_ann.setNs(rstring(self._analysis_table_namespace))
            file_ann.setDescription(rstring(f"Analysis results table: {table_name}"))
            file_ann = conn.getUpdateService().saveAndReturnObject(file_ann)

            # Link to plate
            plate = conn.getObject("Plate", plate_id)
            if not plate:
                raise ValueError(f"Plate {plate_id} not found")

            # Get the annotation ID and fetch as gateway object
            ann_id = file_ann.getId().getValue()
            file_ann_wrapped = conn.getObject("Annotation", ann_id)
            plate.linkAnnotation(file_ann_wrapped)
            logger.info(f"Created OMERO.table '{table_name}' and linked to plate {plate_id}")

        finally:
            table.close()

    def _save_text_annotation(
        self, text_content: str, output_path: Path, images_dir: str = None, **kwargs
    ) -> None:
        """Save text content as a FileAnnotation attached to OMERO object.

        Args:
            text_content: Text content to save
            output_path: Output path (used for filename)
            images_dir: Directory containing images (required to link annotation to correct plate)
            **kwargs: Additional arguments
        """
        conn = self._get_connection(**kwargs)

        # Validate images_dir is provided
        if not images_dir:
            raise ValueError(
                f"images_dir is required for OMERO annotation linking. "
                f"This should be passed from the materialization context. "
                f"Output path: {output_path}"
            )

        # Parse the images directory path to get the plate name, then query OMERO for actual plate ID
        # Path format: /omero/plate_274_outputs/images/
        # The path contains the INPUT plate ID (274), but we need the OUTPUT plate ID
        # We must parse the full plate name and query OMERO to get the actual ID
        images_dir = Path(images_dir)
        plate_name, base_id, is_derived = self._parse_omero_path(images_dir)

        # Query OMERO for the actual plate ID by name
        plate_id = self._find_plate_by_name(plate_name, **kwargs)
        if not plate_id:
            raise ValueError(f"Plate '{plate_name}' not found in OMERO (images dir: {images_dir})")

        # Create FileAnnotation
        import tempfile

        # Write content to temporary file
        with tempfile.NamedTemporaryFile(mode="w", suffix=output_path.suffix, delete=False) as tmp:
            tmp.write(text_content)
            tmp_path = tmp.name

        try:
            # Upload file to OMERO with the actual filename from output_path
            mimetype = OMEROTextFormat.require_path(output_path).mimetype

            file_ann = conn.createFileAnnfromLocalFile(
                tmp_path,
                origFilePathAndName=output_path.name,  # Use actual filename, not temp name
                mimetype=mimetype,
                ns=self._analysis_namespace,
                desc=f"Analysis results: {output_path.name}",
            )

            # Attach to plate
            plate = conn.getObject("Plate", plate_id)
            if plate is not None:
                plate.linkAnnotation(file_ann)
                logger.info(f"Attached {output_path.name} as FileAnnotation to plate {plate_id}")
            else:
                logger.warning(f"Plate {plate_id} not found, FileAnnotation created but not linked")
        finally:
            os.unlink(tmp_path)

    def _save_image(self, data: Any, output_path: Path, **kwargs) -> None:
        """Save image data to OMERO as new image."""
        conn = self._get_connection(**kwargs)

        dataset_id = kwargs.get("dataset_id")
        if not dataset_id:
            raise ValueError("dataset_id required")

        dataset = conn.getObject("Dataset", dataset_id)
        if not dataset:
            raise ValueError(f"Dataset not found: {dataset_id}")

        image_name = output_path.stem

        # Get dimensions
        if data.ndim == 3:
            sizeZ, sizeY, sizeX = data.shape
            sizeC, sizeT = 1, 1
        elif data.ndim == 4:
            sizeZ, sizeC, sizeY, sizeX = data.shape
            sizeT = 1
        else:
            raise ValueError(f"Data must be 3D or 4D, got {data.shape}")

        # Plane generator
        def planes():
            if data.ndim == 3:
                for z in range(sizeZ):
                    yield data[z]
            else:
                for z in range(sizeZ):
                    for c in range(sizeC):
                        yield data[z, c]

        # Create image
        new_image = conn.createImageFromNumpySeq(
            planes(),
            image_name,
            sizeZ=sizeZ,
            sizeC=sizeC,
            sizeT=sizeT,
            description=kwargs.get("description", DEFAULT_OMERO_DESCRIPTION),
            dataset=dataset,
        )

        logger.info(f"Created OMERO image {new_image.getId()}: {image_name}")

    def delete(self, path: str | Path, **kwargs) -> bool:
        """
        Delete a file/annotation from OMERO.

        For text files (JSON/CSV): Delete FileAnnotation
        For images: Delete image (if allowed)
        """
        path = Path(path)

        # For text files, delete FileAnnotation using registry
        if OMEROTextFormat.for_path(path) is not None:
            # For now, just log and return success
            # FileAnnotations will be overwritten on save
            logger.debug(f"Delete requested for {path} - will be overwritten on save")
            return True

        # For images, deletion not supported
        logger.warning(f"Delete not supported for OMERO images: {path}")
        return False

    def _parse_omero_path(self, path: Path) -> tuple[str, int, bool]:
        """Extract (plate_name, base_id, is_derived) from path.

        This method extracts the OMERO plate name from a path by combining the base plate directory
        with any subdirectories (but NOT the filename).

        Examples:
            /omero/plate_289 -> ("plate_289", 289, False)
            /omero/plate_289_outputs -> ("plate_289_outputs", 289, True)
            /omero/plate_289_outputs/images -> ("plate_289_outputs_images", 289, True)
            /omero/plate_289_outputs/images/A01.tif -> ("plate_289_outputs_images", 289, True)
            /omero/plate_289_outputs/images_results -> ("plate_289_outputs_images_results", 289, True)
            /omero/plate_289_outputs/checkpoints_step0/A01.tif -> ("plate_294_outputs_checkpoints_step0", 294, True)
        """
        path = PurePosixPath(str(path).replace("\\", "/"))
        parts = path.parts
        if len(parts) < 2 or parts[0] != "/" or parts[1] != "omero":
            raise ValueError(f"Not an OMERO path: {path}")

        base_name = parts[2]  # "plate_289_outputs"
        # Extract subdirectories (everything between base_name and filename)
        # For /omero/plate_289_outputs/images/A01.tif, subdirs should be ["images"]
        # parts[3:-1] excludes both the base_name (parts[2]) and the filename (parts[-1])
        subdirs = (
            list(parts[3:-1]) if len(parts) > 4 else (list(parts[3:]) if len(parts) == 4 else [])
        )

        if not base_name.startswith("plate_"):
            raise ValueError(f"OMERO path must use 'plate_{{id}}' format: {base_name}")

        name_parts = base_name.split("_")
        if len(name_parts) < 2 or not name_parts[1].isdigit():
            raise ValueError(f"Cannot extract plate ID from: {base_name}")

        base_id = int(name_parts[1])
        plate_name = "_".join([base_name] + subdirs) if subdirs else base_name
        is_derived = len(subdirs) > 0 or len(name_parts) > 2

        return plate_name, base_id, is_derived

    def _get_image_id(self, plate_id: int, well_id: str, site: int, **kwargs) -> int:
        """Get OMERO image ID for well and site."""
        if plate_id not in self._plate_metadata:
            self._load_plate_structure(plate_id, **kwargs)

        plate_struct = self._plate_metadata[plate_id]
        if well_id not in plate_struct.wells:
            raise ValueError(f"Well {well_id} not found in plate {plate_id}")
        if site not in plate_struct.wells[well_id].sites:
            raise ValueError(f"Site {site} not found in well {well_id}")

        return plate_struct.wells[well_id].sites[site].image_id

    def _find_plate_by_name(self, plate_name: str, **kwargs) -> int | None:
        """Query OMERO for plate by name."""
        conn = self._get_connection(**kwargs)
        plates = conn.getObjects("Plate", attributes={"name": plate_name})
        for plate in plates:
            return plate.getId()
        return None

    def save_batch(self, data_list: list[Any], identifiers: list[str | Path], **kwargs) -> None:
        """Save images in one plate batch and route other artifacts individually."""
        if len(data_list) != len(identifiers):
            raise ValueError(f"Length mismatch: {len(data_list)} vs {len(identifiers)}")
        if not identifiers:
            return

        image_data = []
        image_identifiers = []
        individual_items = []
        for data, identifier in zip(data_list, identifiers, strict=True):
            output_path = Path(identifier)
            if self._is_roi_payload(data) or self._is_text_payload(data, output_path):
                individual_items.append((data, identifier))
            else:
                image_data.append(data)
                image_identifiers.append(identifier)

        if image_identifiers:
            self._save_image_batch(image_data, image_identifiers, **kwargs)
        for data, identifier in individual_items:
            self.save(data, identifier, **kwargs)

    def _save_image_batch(
        self,
        data_list: list[Any],
        identifiers: list[str | Path],
        **kwargs,
    ) -> None:
        """Save one batch of image planes through OMERO plate creation."""

        parser_name = kwargs.get("parser_name")
        if not parser_name:
            raise ValueError("parser_name required for OMERO save_batch")

        microscope_type = kwargs.get("microscope_type")
        if not microscope_type:
            raise ValueError("microscope_type required for OMERO save_batch")

        # Validate all paths are in same plate
        plate_names = set()
        for path in identifiers:
            plate_name, _, _ = self._parse_omero_path(Path(path))
            plate_names.add(plate_name)

        if len(plate_names) > 1:
            raise ValueError(f"Cannot save batch across multiple plates: {plate_names}")

        parser = self._load_parser(parser_name)
        plate_name, base_id, _ = self._parse_omero_path(Path(identifiers[0]))

        # Group data by image (well + site)
        images: dict[tuple[str, int], ImagePlaneBatch] = {}
        for data, path in zip(data_list, identifiers, strict=True):
            parsed = parser.parse_filename(Path(path).name)
            well_id, site = parsed["well"], parsed["site"]
            z, c, t = (
                parsed.get("z_index", 1) - 1,
                parsed.get("channel", 1) - 1,
                parsed.get("timepoint", 1) - 1,
            )

            image_key = (well_id, site)
            images.setdefault(image_key, ImagePlaneBatch()).add(z=z, c=c, t=t, data=data)

        # Get or create plate with locking
        plate_id = self._plate_name_cache.get(plate_name)
        if plate_id is None:
            # Remove parser_name and microscope_type from kwargs to avoid duplicate argument error
            # (they're already passed as positional arguments)
            filtered_kwargs = {
                k: v for k, v in kwargs.items() if k not in ("parser_name", "microscope_type")
            }
            plate_id = self._get_or_create_plate_with_lock(
                plate_name, base_id, parser_name, microscope_type, images, **filtered_kwargs
            )
            self._plate_name_cache[plate_name] = plate_id

        # Write planes
        self._write_planes_to_plate(plate_id, images, **kwargs)

    def _get_or_create_plate_with_lock(
        self, plate_name, base_id, parser_name, microscope_type, images, **kwargs
    ):
        """Create plate with file locking (like zarr metadata)."""
        lock_dir = Path.home() / self._lock_dir_name / "omero_locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = lock_dir / f"{plate_name}.lock"

        with file_lock(lock_path):
            existing_id = self._find_plate_by_name(plate_name, **kwargs)
            if existing_id:
                self._load_plate_structure(existing_id, **kwargs)
                return existing_id

            return self._create_derived_plate(
                plate_name, base_id, parser_name, microscope_type, images, **kwargs
            )

    def _create_derived_plate(
        self, plate_name, base_id, parser_name, microscope_type, images, **kwargs
    ):
        """Create plate from grouped image data."""
        conn = self._get_connection(**kwargs)

        # Import OMERO model classes
        import omero.model
        from omero.model import NamedValue
        from omero.rtypes import rint, rstring

        update_service = conn.getUpdateService()

        # Create plate
        plate = omero.model.PlateI()
        plate.setName(rstring(plate_name))
        plate.setColumnNamingConvention(rstring("number"))
        plate.setRowNamingConvention(rstring("letter"))
        plate = update_service.saveAndReturnObject(plate)
        plate_id = plate.getId().getValue()

        # Attach metadata
        metadata_ann = omero.model.MapAnnotationI()
        metadata_ann.setNs(rstring(self._metadata_namespace))
        metadata_ann.setMapValue(
            [
                NamedValue(self._parser_key, parser_name),
                NamedValue(self._microscope_key, microscope_type),
            ]
        )
        plate.linkAnnotation(metadata_ann)

        prov_ann = omero.model.MapAnnotationI()
        prov_ann.setNs(rstring(self._provenance_namespace))
        prov_ann.setMapValue(
            [
                NamedValue("source_plate_id", str(base_id)),
                NamedValue("created_by", self._namespace_prefix),
                NamedValue("timestamp", datetime.now().isoformat()),
            ]
        )
        plate.linkAnnotation(prov_ann)
        update_service.saveObject(plate)

        # Create wells WITHOUT images
        # Images will be created with actual data in _write_planes_to_plate
        # This fixes the bug where placeholder zero images caused first well to be black
        well_ids = {well_id for well_id, _ in images}
        for well_id in well_ids:
            row, col = ord(well_id[0]) - ord("A"), int(well_id[1:]) - 1
            well = omero.model.WellI()
            well.setPlate(plate)
            well.setRow(rint(row))
            well.setColumn(rint(col))
            update_service.saveAndReturnObject(well)

        # Don't load plate structure yet - it will be loaded after images are written
        # Store parser for later use
        self._parser_cache[plate_id] = self._load_parser(parser_name)
        return plate_id

    def _write_planes_to_plate(self, plate_id, images, **kwargs):
        """Write planes by creating complete images with all data at once."""
        conn = self._get_connection(**kwargs)
        import omero.model
        from omero.rtypes import rint

        for (well_id, site), img_data in images.items():
            # Check if well/site already exists
            try:
                self._get_image_id(plate_id, well_id, site)
                # Image exists - skip it (already written)
                logger.info(
                    f"Image for {well_id} site {site} already exists in plate {plate_id}, skipping"
                )
                continue
            except ValueError:
                # Image doesn't exist - create it with all planes
                pass

            # Create complete image with all planes at once
            image = conn.createImageFromNumpySeq(
                zctPlanes=img_data.iter_omero_planes(),
                imageName=f"{well_id}_s{site:03d}",
                sizeZ=img_data.size_z,
                sizeC=img_data.size_c,
                sizeT=img_data.size_t,
                description=DEFAULT_OMERO_WELL_DESCRIPTION_TEMPLATE.format(
                    well_id=well_id,
                    site=site,
                ),
            )

            # Link image to well
            row, col = ord(well_id[0]) - ord("A"), int(well_id[1:]) - 1

            # Check if well exists, create if not
            query_service = conn.getQueryService()
            params = omero.sys.ParametersI()
            params.addLong("pid", plate_id)
            params.add("row", rint(row))
            params.add("col", rint(col))

            query = "select w from Well as w where w.plate.id = :pid and w.row = :row and w.column = :col"

            # Get or create lock for this specific well
            lock_key = f"plate_{plate_id}_well_{row}_{col}"
            with self._well_locks_lock:
                if lock_key not in self._well_locks:
                    self._well_locks[lock_key] = threading.Lock()
                well_lock = self._well_locks[lock_key]

            # Use threading lock for thread safety + file lock for process safety
            with well_lock:
                lock_dir = Path.home() / self._lock_dir_name / "omero_locks"
                lock_dir.mkdir(parents=True, exist_ok=True)
                lock_path = lock_dir / f"{lock_key}.lock"

                with file_lock(lock_path):
                    # Re-check if well exists after acquiring both locks
                    # Use findAllByQuery since findByQuery throws exception on null
                    wells = query_service.findAllByQuery(query, params)
                    well_obj = wells[0] if wells else None

                    if not well_obj:
                        # Create new well
                        update_service = conn.getUpdateService()
                        well = omero.model.WellI()
                        well.setPlate(omero.model.PlateI(plate_id, False))
                        well.setRow(rint(row))
                        well.setColumn(rint(col))
                        well_obj = update_service.saveAndReturnObject(well)

            # Link image to well
            # Reload well with wellSamples collection loaded
            well_obj_loaded = conn.getObject("Well", well_obj.getId().getValue())
            # Force load the wellSamples collection by accessing it
            _ = list(well_obj_loaded.listChildren())  # This loads the collection

            ws = omero.model.WellSampleI()
            ws.setImage(omero.model.ImageI(image.getId(), False))
            ws.setWell(well_obj_loaded._obj)
            well_obj_loaded._obj.addWellSample(ws)
            conn.getUpdateService().saveObject(well_obj_loaded._obj)

        # Reload plate structure to include new wells/images
        self._load_plate_structure(plate_id)

    def list_files(
        self,
        directory: str | Path,
        pattern: str = "*",
        extensions: set[str] = None,
        recursive: bool = False,
        **kwargs,
    ) -> list[str]:
        """
        Generate filenames on-demand from plate structure.

        Args:
            directory: Path containing plate ID (e.g., "/17/Images" or "17")
            pattern: File pattern (currently ignored)
            extensions: File extensions (currently ignored)
            recursive: Recursion flag (currently ignored)
            **kwargs: Additional backend-specific arguments (unused)

        Returns:
            List of filenames: ["A01_s001_w1_z001_t001.tif", ...]
        """
        # Extract plate_id from path
        # Path could be: "/omero/plate_55/Images" or "/17/Images" or "17/Images" or just "17"
        path_parts = Path(directory).parts

        # Find the numeric plate_id in the path
        plate_id = None
        for part in path_parts:
            # Handle both "55" and "plate_55" formats
            if part.isdigit():
                plate_id = int(part)
                break
            elif part.startswith("plate_"):
                try:
                    plate_id = int(part.split("_")[1])
                    break
                except (IndexError, ValueError):
                    continue

        if plate_id is None:
            raise ValueError(f"Could not extract numeric plate_id from path: {directory}")

        # Load plate structure if not cached
        if plate_id not in self._plate_metadata:
            self._load_plate_structure(plate_id)

        plate_struct = self._plate_metadata[plate_id]
        parser = self._parser_cache[plate_id]

        # Generate filenames on-the-fly
        filenames = []
        for well_id, well_struct in plate_struct.wells.items():
            for site_idx, image_struct in well_struct.sites.items():
                # Generate filename for each (z, c, t) combination
                for t in range(image_struct.sizeT):
                    for z in range(image_struct.sizeZ):
                        for c in range(image_struct.sizeC):
                            filename = parser.construct_filename(
                                well=well_id,
                                site=site_idx,
                                channel=c + 1,
                                z_index=z + 1,
                                timepoint=t + 1,
                                extension=".tif",
                            )
                            filenames.append(filename)

        logger.debug(f"Generated {len(filenames)} filenames on-demand for plate {plate_id}")
        return filenames

    def exists(self, path: str | Path) -> bool:
        """
        Check if a virtual OMERO path exists.

        For OMERO virtual backend, paths always "exist" if they're valid OMERO paths.
        This is because OMERO generates filenames on-demand from plate structure.

        Args:
            path: Virtual OMERO path to check

        Returns:
            True if path is a valid OMERO path format, False otherwise
        """
        try:
            # Check if path is valid OMERO format
            path_str = str(path)
            if not path_str.startswith("/omero/"):
                return False

            # Try to parse the path - if it parses, it's valid
            self._parse_omero_path(Path(path))
            return True
        except (ValueError, IndexError):
            return False

    def ensure_directory(self, directory: str | Path) -> None:
        """
        Ensure directory exists (no-op for OMERO virtual backend).

        OMERO is a virtual filesystem - directories don't exist as real entities.
        Plates are created on-demand during save_batch operations.
        This method exists to satisfy the backend interface but does nothing.

        Args:
            directory: Virtual directory path (ignored)
        """
        # No-op for virtual backend - directories are implicit in OMERO
        pass

    def load_batch(self, file_paths: list[str | Path], **kwargs) -> list[Any]:
        """Load multiple images from OMERO."""
        return [self.load(fp, **kwargs) for fp in file_paths]

    def _save_rois(self, rois: list, output_path: Path, images_dir: str = None, **kwargs) -> str:
        """Save ROIs to OMERO by linking to images in the materialized plate.

        Args:
            rois: List of ROI objects
            output_path: Output path (e.g., /omero/plate_32_outputs/images_results/A01_rois_step7.json)
            images_dir: Images directory path (required for OMERO to link ROIs to correct plate)

        Returns:
            String describing where ROIs were saved
        """
        import omero.model
        from omero.rtypes import rdouble, rstring

        from .roi import EllipseShape, PointShape, PolygonShape, PolylineShape

        conn = self._get_connection(**kwargs)

        # Validate images_dir is provided
        if not images_dir:
            raise ValueError(
                f"images_dir is required for OMERO ROI linking. "
                f"This should be passed from the materialization context. "
                f"Output path: {output_path}"
            )

        images_dir = Path(images_dir)

        # Parse the images directory path to get the plate name
        plate_name, base_id, is_derived = self._parse_omero_path(images_dir)

        # Query OMERO for the actual plate ID by name
        plate_id = self._find_plate_by_name(plate_name, **kwargs)
        if not plate_id:
            raise ValueError(f"Plate '{plate_name}' not found in OMERO (images dir: {images_dir})")

        # Extract well ID from filename (first component before underscore)
        filename = output_path.name
        well_id_from_filename = filename.split("_")[0]  # "A01" or "A1"

        # Query OMERO for images in this well of the materialized plate
        plate = conn.getObject("Plate", plate_id)
        if not plate:
            raise ValueError(f"Plate {plate_id} not found in OMERO")

        # Find well by label
        # Note: getWellPos() returns format like "A1" (no zero-padding)
        # but filenames might use "A01" (zero-padded), so we need to normalize
        well = None
        for w in plate.listChildren():
            well_pos = w.getWellPos()  # e.g., "A1"
            # Normalize both to compare: remove leading zeros from column number
            # "A01" -> "A1", "A1" -> "A1"
            normalized_filename_well = well_id_from_filename[0] + str(
                int(well_id_from_filename[1:])
            )
            if well_pos == normalized_filename_well:
                well = w
                break

        if not well:
            raise ValueError(f"Well {well_id_from_filename} not found in plate {plate_id}")

        # Get all images in this well
        images = []
        for well_sample in well.listChildren():
            image = well_sample.getImage()
            if image:
                images.append(image)

        if not images:
            raise ValueError(f"No images found in well {well_id_from_filename} of plate {plate_id}")

        # Link ROIs to ALL images in the well
        # (ROIs were created from the full image stack at this step)
        update_service = conn.getUpdateService()
        roi_count = 0

        for image in images:
            for roi in rois:
                # Create OMERO ROI object
                omero_roi = omero.model.RoiI()
                omero_roi.setImage(image._obj)

                # Add shapes to ROI
                for shape in roi.shapes:
                    if isinstance(shape, PolygonShape):
                        # Create OMERO polygon
                        polygon = omero.model.PolygonI()

                        # Convert coordinates to OMERO format (comma-separated string)
                        # OMERO expects "x1,y1 x2,y2 x3,y3 ..."
                        points_str = " ".join([f"{x},{y}" for y, x in shape.coordinates])
                        polygon.setPoints(rstring(points_str))

                        # Set metadata
                        if "label" in roi.metadata:
                            polygon.setTextValue(rstring(str(roi.metadata["label"])))

                        omero_roi.addShape(polygon)

                    elif isinstance(shape, PolylineShape):
                        # Create OMERO polyline
                        polyline = omero.model.PolylineI()

                        # Convert coordinates to OMERO format (comma-separated string)
                        # OMERO expects "x1,y1 x2,y2 x3,y3 ..."
                        points_str = " ".join([f"{x},{y}" for y, x in shape.coordinates])
                        polyline.setPoints(rstring(points_str))

                        # Set metadata
                        if "label" in roi.metadata:
                            polyline.setTextValue(rstring(str(roi.metadata["label"])))

                        omero_roi.addShape(polyline)

                    elif isinstance(shape, EllipseShape):
                        # Create OMERO ellipse
                        ellipse = omero.model.EllipseI()
                        ellipse.setX(rdouble(shape.center_x))
                        ellipse.setY(rdouble(shape.center_y))
                        ellipse.setRadiusX(rdouble(shape.radius_x))
                        ellipse.setRadiusY(rdouble(shape.radius_y))

                        if "label" in roi.metadata:
                            ellipse.setTextValue(rstring(str(roi.metadata["label"])))

                        omero_roi.addShape(ellipse)

                    elif isinstance(shape, PointShape):
                        # Create OMERO point
                        point = omero.model.PointI()
                        point.setX(rdouble(shape.x))
                        point.setY(rdouble(shape.y))

                        if "label" in roi.metadata:
                            point.setTextValue(rstring(str(roi.metadata["label"])))

                        omero_roi.addShape(point)

                # Save ROI to OMERO
                if omero_roi.sizeOfShapes() > 0:
                    update_service.saveAndReturnObject(omero_roi)
                    roi_count += 1

        result_msg = f"Linked {len(rois)} ROIs to {len(images)} images in well {well_id_from_filename} (plate: {plate_name}, ID: {plate_id})"
        logger.info(result_msg)
        return result_msg
