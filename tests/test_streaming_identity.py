"""Tests for nominal viewer-stream producer identities."""

from __future__ import annotations

import pytest

from polystore.streaming.identity import StreamProducerIdentity


def _declared_identity(**overrides) -> StreamProducerIdentity:
    values = {
        "origin": "pipeline",
        "output_kind": "artifact",
        "output_key": "neurite_morphology",
        "projection_key": "neurite_morphology",
        "step_name": "Analyze neurites",
        "artifact_kind": "spatial_graph",
    }
    values.update(overrides)
    return StreamProducerIdentity(**values)


def test_runtime_identity_matches_partially_bound_declaration() -> None:
    declaration = _declared_identity()
    observed = _declared_identity(
        pipeline_position=4,
        step_scope_id="plate/step-4",
        invocation_key="A01/channel-1",
    )

    assert observed.matches_declaration(declaration)


@pytest.mark.parametrize(
    ("field_name", "different_value"),
    (
        ("origin", "manual"),
        ("output_kind", "main"),
        ("output_key", "other_output"),
        ("projection_key", "other_projection"),
        ("step_name", "Other step"),
        ("artifact_kind", "image"),
    ),
)
def test_runtime_identity_rejects_strict_declaration_mismatch(
    field_name: str,
    different_value: str,
) -> None:
    observed = _declared_identity(
        pipeline_position=4,
        step_scope_id="plate/step-4",
        invocation_key="A01/channel-1",
    )
    declaration = _declared_identity(**{field_name: different_value})

    assert not observed.matches_declaration(declaration)


@pytest.mark.parametrize(
    ("field_name", "expected_value", "observed_value"),
    (
        ("pipeline_position", 4, 5),
        ("step_scope_id", "plate/step-4", "plate/step-5"),
        ("invocation_key", "A01/channel-1", "A01/channel-2"),
    ),
)
def test_runtime_identity_honors_bound_runtime_declaration_field(
    field_name: str,
    expected_value: str | int,
    observed_value: str | int,
) -> None:
    declaration = _declared_identity(**{field_name: expected_value})
    matching = _declared_identity(**{field_name: expected_value})
    different = _declared_identity(**{field_name: observed_value})

    assert matching.matches_declaration(declaration)
    assert not different.matches_declaration(declaration)


def test_runtime_identity_requires_nominal_declaration() -> None:
    observed = _declared_identity()

    with pytest.raises(TypeError, match="StreamProducerIdentity"):
        observed.matches_declaration(observed.to_payload())  # type: ignore[arg-type]
