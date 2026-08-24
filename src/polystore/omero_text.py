"""Declaration-owned text storage semantics for OMERO backends."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from enum import Enum
from io import StringIO
from pathlib import Path

import pandas as pd

from .formats import FileFormat

TableParser = Callable[[str], pd.DataFrame | None]


def _csv_table(content: str) -> pd.DataFrame | None:
    try:
        return pd.read_csv(StringIO(content))
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return None


def _json_table(content: str) -> pd.DataFrame | None:
    try:
        value = json.loads(content)
    except json.JSONDecodeError:
        return None

    if isinstance(value, list):
        if not value or not all(isinstance(item, Mapping) for item in value):
            return None
        return pd.DataFrame(value)
    if isinstance(value, Mapping):
        try:
            return pd.DataFrame(value)
        except ValueError:
            return None
    return None


def _text_table(content: str) -> pd.DataFrame | None:
    for separator in ("\t", ","):
        try:
            candidate = pd.read_csv(StringIO(content), sep=separator)
        except (pd.errors.EmptyDataError, pd.errors.ParserError):
            continue
        if len(candidate.columns) > 1:
            return candidate

    key_value_data = {
        key.strip(): [value.strip()]
        for line in content.strip().splitlines()
        if ":" in line
        for key, value in (line.split(":", 1),)
    }
    return pd.DataFrame(key_value_data) if key_value_data else None


class OMEROTextFormat(Enum):
    """One text format's generic identity and OMERO-specific leaf behavior."""

    def __new__(
        cls,
        file_format: FileFormat,
        mimetype: str,
        table_parser: TableParser,
    ):
        member = object.__new__(cls)
        member._value_ = file_format.value
        member.file_format = file_format
        member.mimetype = mimetype
        member._table_parser = table_parser
        return member

    JSON = (FileFormat.JSON, "application/json", _json_table)
    CSV = (FileFormat.CSV, "text/csv", _csv_table)
    TEXT = (FileFormat.TEXT, "text/plain", _text_table)

    @classmethod
    def for_path(cls, path: str | Path) -> OMEROTextFormat | None:
        """Return the member declaring one path's text-storage semantics."""

        suffix = Path(path).suffix.lower()
        return next(
            (text_format for text_format in cls if suffix in text_format.file_format.extensions),
            None,
        )

    @classmethod
    def require_path(cls, path: str | Path) -> OMEROTextFormat:
        """Return one declared member or fail at the text-storage boundary."""

        text_format = cls.for_path(path)
        if text_format is None:
            raise ValueError(f"No OMERO text format is declared for {str(path)!r}.")
        return text_format

    def table(self, content: str) -> pd.DataFrame | None:
        """Return tabular content when this format declares a valid table."""

        table = self._table_parser(content)
        if table is None or table.empty or not len(table.columns):
            return None
        return table
