"""Tests for the declaration-owned OMERO.tables capability boundary."""

import sys
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest

from polystore.omero_tables import (
    OMEROTableColumnType,
    OMEROTableService,
    OMEROTableServiceUnavailableError,
)

_DEFAULT_TABLE = object()


class _Column:
    def __init__(self, *declaration) -> None:
        self.declaration = declaration
        self.values = []


class _LongColumn(_Column):
    pass


class _DoubleColumn(_Column):
    pass


class _BoolColumn(_Column):
    pass


class _StringColumn(_Column):
    pass


@pytest.fixture
def omero_grid_columns(monkeypatch):
    grid = ModuleType("omero.grid")
    grid.LongColumn = _LongColumn
    grid.DoubleColumn = _DoubleColumn
    grid.BoolColumn = _BoolColumn
    grid.StringColumn = _StringColumn
    omero = ModuleType("omero")
    omero.__path__ = []
    monkeypatch.setitem(sys.modules, "omero", omero)
    monkeypatch.setitem(sys.modules, "omero.grid", grid)


@pytest.mark.parametrize(
    ("series", "column_type", "values"),
    (
        (pd.Series([1, 2]), _LongColumn, [1, 2]),
        (pd.Series([1.5, 2.5]), _DoubleColumn, [1.5, 2.5]),
        (pd.Series([True, False]), _BoolColumn, [True, False]),
        (pd.Series(["A01", float("nan")], dtype=object), _StringColumn, ["A01", "nan"]),
    ),
)
def test_table_column_type_owns_dtype_projection(
    omero_grid_columns,
    series,
    column_type,
    values,
) -> None:
    column = OMEROTableColumnType.column_for("value", series)

    assert isinstance(column, column_type)
    assert column.values == values
    if isinstance(column, _StringColumn):
        assert column.declaration == ("value", "", 3, [])


class _Resources:
    def __init__(
        self,
        readiness: list[bool],
        *,
        repository_id: int | None = 7,
        has_managed_identifier: bool = True,
        has_repository: bool = True,
        table: object | None = _DEFAULT_TABLE,
    ) -> None:
        self._readiness = iter(readiness)
        self._last_readiness = readiness[-1]
        self._table = table
        self.created_tables: list[tuple[int, str]] = []
        identifier = (
            SimpleNamespace(getValue=lambda: repository_id) if has_managed_identifier else None
        )
        self._repository_map = SimpleNamespace(
            descriptions=[SimpleNamespace(getId=lambda: identifier)] if has_repository else []
        )

    def areTablesEnabled(self) -> bool:
        self._last_readiness = next(self._readiness, self._last_readiness)
        return self._last_readiness

    def repositories(self):
        return self._repository_map

    def newTable(self, repository_id: int, path: str):
        self.created_tables.append((repository_id, path))
        return self._table


def _connection(resources: _Resources):
    return SimpleNamespace(
        c=SimpleNamespace(
            sf=SimpleNamespace(sharedResources=lambda: resources),
        )
    )


def test_table_service_uses_declared_readiness_and_repository() -> None:
    table = object()
    resources = _Resources([True], repository_id=13, table=table)

    created = OMEROTableService().create_table(
        _connection(resources),
        "measurements.h5",
    )

    assert created is table
    assert resources.created_tables == [(13, "measurements.h5")]


def test_table_service_waits_on_capability_instead_of_exception_text(
    monkeypatch,
) -> None:
    observed_delays: list[float] = []
    monkeypatch.setattr("polystore.omero_tables.time.sleep", observed_delays.append)
    resources = _Resources([False, False, True])

    OMEROTableService(
        readiness_retry_delays_seconds=(0.1, 0.2),
    ).create_table(_connection(resources), "measurements.h5")

    assert observed_delays == [0.1, 0.2]
    assert resources.created_tables == [(7, "measurements.h5")]


def test_table_service_fails_before_create_when_capability_stays_unavailable() -> None:
    resources = _Resources([False])

    with pytest.raises(
        OMEROTableServiceUnavailableError,
        match="did not become available",
    ):
        OMEROTableService().create_table(
            _connection(resources),
            "measurements.h5",
        )

    assert resources.created_tables == []


def test_table_service_requires_managed_repository_identifier() -> None:
    resources = _Resources([True], has_managed_identifier=False)

    with pytest.raises(
        OMEROTableServiceUnavailableError,
        match="no managed identifier",
    ):
        OMEROTableService().create_table(
            _connection(resources),
            "measurements.h5",
        )

    assert resources.created_tables == []


@pytest.mark.parametrize(
    ("resources", "message"),
    [
        (_Resources([True], has_repository=False), "declares no repository"),
        (_Resources([True], repository_id=None), "identifier has no value"),
        (_Resources([True], table=None), "returned no table"),
    ],
)
def test_table_service_rejects_incomplete_capability_declarations(
    resources: _Resources,
    message: str,
) -> None:
    with pytest.raises(OMEROTableServiceUnavailableError, match=message):
        OMEROTableService().create_table(
            _connection(resources),
            "measurements.h5",
        )
