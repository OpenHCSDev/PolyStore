"""Tests for declaration-owned OMERO text format behavior."""

import pandas as pd
import pytest

from polystore.omero_text import OMEROTextFormat


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("measurements.csv", OMEROTextFormat.CSV),
        ("records.json", OMEROTextFormat.JSON),
        ("summary.txt", OMEROTextFormat.TEXT),
        ("image.tif", None),
    ],
)
def test_text_format_resolves_from_generic_file_format(path, expected) -> None:
    assert OMEROTextFormat.for_path(path) is expected


@pytest.mark.parametrize(
    ("text_format", "content", "records"),
    [
        (OMEROTextFormat.CSV, "label,count\ncell,7\n", [{"label": "cell", "count": 7}]),
        (OMEROTextFormat.JSON, '[{"label":"cell","count":7}]', [{"label": "cell", "count": 7}]),
        (OMEROTextFormat.TEXT, "label\tcount\ncell\t7\n", [{"label": "cell", "count": 7}]),
        (OMEROTextFormat.TEXT, "label: cell\ncount: 7\n", [{"label": "cell", "count": "7"}]),
    ],
)
def test_text_format_member_owns_table_parsing(
    text_format: OMEROTextFormat,
    content: str,
    records: list[dict],
) -> None:
    table = text_format.table(content)

    assert isinstance(table, pd.DataFrame)
    assert table.to_dict(orient="records") == records


@pytest.mark.parametrize(
    ("text_format", "content"),
    [
        (OMEROTextFormat.CSV, ""),
        (OMEROTextFormat.JSON, "not json"),
        (OMEROTextFormat.JSON, "[]"),
        (OMEROTextFormat.TEXT, "plain prose"),
    ],
)
def test_text_format_returns_no_table_for_annotation_content(
    text_format: OMEROTextFormat,
    content: str,
) -> None:
    assert text_format.table(content) is None


def test_text_format_fails_loud_for_undeclared_path() -> None:
    with pytest.raises(ValueError, match="No OMERO text format"):
        OMEROTextFormat.require_path("image.tif")
