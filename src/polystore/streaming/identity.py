"""Nominal stream identity records shared by viewer streaming backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Mapping


@dataclass(frozen=True, slots=True)
class StreamProducerIdentity:
    """Producer/output identity for one streamed viewer item."""

    origin: str
    output_kind: str
    output_key: str
    step_name: str | None = None
    pipeline_position: int | None = None
    step_scope_id: str | None = None
    invocation_key: str | None = None
    artifact_kind: str | None = None
    PAYLOAD_FIELDS: ClassVar[tuple[str, ...]] = (
        "origin",
        "output_kind",
        "output_key",
        "step_name",
        "pipeline_position",
        "step_scope_id",
        "invocation_key",
        "artifact_kind",
    )

    @classmethod
    def pipeline_output(
        cls,
        *,
        output_kind: str,
        output_key: str,
        step_name: str,
        pipeline_position: int | None,
        step_scope_id: str | None = None,
        artifact_kind: str | None = None,
    ) -> "StreamProducerIdentity":
        """Build identity for one pipeline-produced stream output."""
        return cls(
            origin="pipeline",
            output_kind=output_kind,
            output_key=output_key,
            step_name=step_name,
            pipeline_position=pipeline_position,
            step_scope_id=step_scope_id,
            artifact_kind=artifact_kind,
        )

    @classmethod
    def manual(cls, output_key: str) -> "StreamProducerIdentity":
        """Build identity for one manual viewer action."""
        return cls._fixed_origin_output(
            origin="manual",
            output_kind="manual",
            output_key=output_key,
        )

    @classmethod
    def direct(cls, output_key: str) -> "StreamProducerIdentity":
        """Build identity for direct in-process display calls."""
        return cls._fixed_origin_output(
            origin="direct",
            output_kind="direct",
            output_key=output_key,
        )

    @classmethod
    def _fixed_origin_output(
        cls,
        *,
        origin: str,
        output_kind: str,
        output_key: str,
    ) -> "StreamProducerIdentity":
        """Build identity variants whose origin and output kind match."""
        return cls(
            origin=origin,
            output_kind=output_kind,
            output_key=output_key,
        )

    @classmethod
    def from_payload(cls, payload: "StreamProducerIdentity | Mapping[str, Any]") -> "StreamProducerIdentity":
        if isinstance(payload, cls):
            return payload
        if not isinstance(payload, Mapping):
            raise TypeError(
                "Stream producer identity must be a mapping or StreamProducerIdentity, "
                f"got {type(payload).__name__}."
            )
        missing = [
            field_name
            for field_name in ("origin", "output_kind", "output_key")
            if payload.get(field_name) in (None, "")
        ]
        if missing:
            raise ValueError(f"Stream producer identity missing required fields: {missing}")
        pipeline_position = payload.get("pipeline_position")
        return cls(
            origin=str(payload["origin"]),
            output_kind=str(payload["output_kind"]),
            output_key=str(payload["output_key"]),
            step_name=_optional_str(payload.get("step_name")),
            pipeline_position=(
                None if pipeline_position is None else int(pipeline_position)
            ),
            step_scope_id=_optional_str(payload.get("step_scope_id")),
            invocation_key=_optional_str(payload.get("invocation_key")),
            artifact_kind=_optional_str(payload.get("artifact_kind")),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            field_name: getattr(self, field_name)
            for field_name in self.PAYLOAD_FIELDS
        }

    def route_parts(self) -> tuple[str, ...]:
        parts = [
            f"origin_{self.origin}",
            f"kind_{self.output_kind}",
            f"out_{self.output_key}",
        ]
        if self.pipeline_position is not None:
            parts.append(f"step_{self.pipeline_position}")
        if self.step_scope_id:
            parts.append(f"scope_{self.step_scope_id}")
        if self.step_name:
            parts.append(f"name_{self.step_name}")
        if self.invocation_key:
            parts.append(f"invocation_{self.invocation_key}")
        if self.artifact_kind:
            parts.append(f"artifact_{self.artifact_kind}")
        return tuple(parts)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


class StreamProducerDisplayNameAuthority:
    """Own user-facing labels derived from stream producer identity."""

    PIPELINE_DISPLAY_INDEX_BASE: ClassVar[int] = 1
    OUTPUT_KEY_OMITTING_KINDS: ClassVar[frozenset[str]] = frozenset(
        {"main", "manual", "direct"}
    )

    @staticmethod
    def producer_base(producer: StreamProducerIdentity) -> str:
        if producer.step_name:
            return producer.step_name
        return producer.output_key

    @classmethod
    def producer_label(cls, producer: StreamProducerIdentity) -> str:
        base = cls.producer_base(producer)
        if producer.pipeline_position is None:
            return base
        return f"{producer.pipeline_position + cls.PIPELINE_DISPLAY_INDEX_BASE}. {base}"

    @classmethod
    def output_label(cls, producer: StreamProducerIdentity) -> str:
        parts = [cls.producer_label(producer)]
        if cls.includes_output_key(producer):
            parts.append(producer.output_key)
        return " ".join(part for part in parts if part)

    @classmethod
    def disambiguation_label(cls, producer: StreamProducerIdentity) -> str:
        if producer.pipeline_position is not None:
            return f"step {producer.pipeline_position + cls.PIPELINE_DISPLAY_INDEX_BASE}"
        return producer.output_key or producer.origin

    @classmethod
    def includes_output_key(cls, producer: StreamProducerIdentity) -> bool:
        if not producer.output_key:
            return False
        if producer.output_kind in cls.OUTPUT_KEY_OMITTING_KINDS:
            return False
        return producer.output_key != cls.producer_base(producer)


class StreamRouteKeyAuthority:
    """Own stable key-token projection for viewer route keys."""

    @staticmethod
    def token(value: object) -> str:
        return str(value).replace("/", "_").replace("\\", "_").replace(" ", "_")

    @classmethod
    def join(cls, parts: tuple[object, ...] | list[object]) -> str:
        if not parts:
            raise ValueError("Cannot build a stream route key with no parts.")
        return "_".join(cls.token(part) for part in parts)
