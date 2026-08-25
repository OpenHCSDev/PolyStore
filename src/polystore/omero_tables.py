"""OMERO.tables capability and table-creation authority."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

OMEROColumnMatcher = Callable[[Any], bool]
OMEROColumnBuilder = Callable[[str, Any], Any]


def _integer_column_matches(series: Any) -> bool:
    import pandas as pd

    return bool(pd.api.types.is_integer_dtype(series))


def _float_column_matches(series: Any) -> bool:
    import pandas as pd

    return bool(pd.api.types.is_float_dtype(series))


def _bool_column_matches(series: Any) -> bool:
    import pandas as pd

    return bool(pd.api.types.is_bool_dtype(series))


def _fallback_column_matches(series: Any) -> bool:
    del series
    return True


def _integer_column(name: str, series: Any) -> Any:
    from omero.grid import LongColumn

    column = LongColumn(name, "", [])
    column.values = series.astype(int).tolist()
    return column


def _float_column(name: str, series: Any) -> Any:
    from omero.grid import DoubleColumn

    column = DoubleColumn(name, "", [])
    column.values = series.astype(float).tolist()
    return column


def _bool_column(name: str, series: Any) -> Any:
    from omero.grid import BoolColumn

    column = BoolColumn(name, "", [])
    column.values = series.astype(bool).tolist()
    return column


def _string_column(name: str, series: Any) -> Any:
    from omero.grid import StringColumn

    values = [str(value) for value in series.tolist()]
    column = StringColumn(
        name,
        "",
        max((len(value) for value in values), default=1),
        [],
    )
    column.values = values
    return column


class OMEROTableColumnType(Enum):
    """One pandas dtype family's complete OMERO column projection."""

    def __new__(
        cls,
        identity: str,
        matches: OMEROColumnMatcher,
        build: OMEROColumnBuilder,
    ):
        member = object.__new__(cls)
        member._value_ = identity
        member._matches = matches
        member._build = build
        return member

    INTEGER = ("integer", _integer_column_matches, _integer_column)
    FLOAT = ("float", _float_column_matches, _float_column)
    BOOLEAN = ("boolean", _bool_column_matches, _bool_column)
    STRING = ("string", _fallback_column_matches, _string_column)

    @classmethod
    def column_for(cls, name: str, series: Any) -> Any:
        """Build the first declared OMERO column matching one pandas series."""

        return next(member for member in cls if member._matches(series))._build(
            name,
            series,
        )


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

    def wait_until_available(self, connection: Any) -> Any:
        """Return shared resources after the declared table capability is ready."""

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

        return resources

    def create_table(self, connection: Any, path: str) -> Any:
        """Create one table through a ready service and repository declaration."""

        resources = self.wait_until_available(connection)

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
