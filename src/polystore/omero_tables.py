"""OMERO.tables capability and table-creation authority."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


class OMEROTableServiceUnavailableError(RuntimeError):
    """Raised when OMERO.tables cannot satisfy a table operation."""


@dataclass(frozen=True, slots=True)
class OMEROTableService:
    """Create tables only after OMERO reports its table service as available."""

    readiness_retry_delays_seconds: tuple[float, ...] = ()

    @staticmethod
    def is_available(connection: Any) -> bool:
        """Return OMERO's declared table-service readiness state."""

        return bool(connection.c.sf.sharedResources().areTablesEnabled())

    def create_table(self, connection: Any, path: str) -> Any:
        """Create one table through a ready service and repository declaration."""

        resources = connection.c.sf.sharedResources()
        retry_delays = iter(self.readiness_retry_delays_seconds)
        while not resources.areTablesEnabled():
            retry_delay = next(retry_delays, None)
            if retry_delay is None:
                raise OMEROTableServiceUnavailableError(
                    "OMERO.tables did not become available for table creation."
                )
            logger.info(
                "Waiting %.1f seconds for OMERO.tables to become available.",
                retry_delay,
            )
            time.sleep(retry_delay)

        repository_map = resources.repositories()
        descriptions = repository_map.descriptions
        if not descriptions:
            raise OMEROTableServiceUnavailableError(
                "OMERO.tables is available but declares no repository."
            )

        repository_id_value = descriptions[0].getId()
        if repository_id_value is None:
            raise OMEROTableServiceUnavailableError(
                "The OMERO table repository has no managed identifier."
            )
        repository_id = repository_id_value.getValue()
        if repository_id is None:
            raise OMEROTableServiceUnavailableError(
                "The OMERO table repository identifier has no value."
            )

        table = resources.newTable(repository_id, path)
        if table is None:
            raise OMEROTableServiceUnavailableError(
                "OMERO.tables returned no table after reporting availability."
            )
        return table


OMERO_TABLE_SERVICE = OMEROTableService(
    readiness_retry_delays_seconds=(1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 30.0, 30.0),
)
