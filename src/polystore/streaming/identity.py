"""Nominal stream identity records shared by viewer streaming backends."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import ClassVar, Mapping, Sequence, TypeAlias


StreamProducerPayloadValue: TypeAlias = str | int | None
StreamProducerPayloadMapping: TypeAlias = Mapping[str, StreamProducerPayloadValue]
RouteKeyPart: TypeAlias = str | int | float | bool | None


class StreamProducerOrigin(str, Enum):
    """Nominal stream producer origin values."""

    PIPELINE = "pipeline"
    MANUAL = "manual"
    DIRECT = "direct"


class FixedStreamProducerIdentityKind(str, Enum):
    """Producer identities whose origin and output kind are intentionally equal."""

    MANUAL = StreamProducerOrigin.MANUAL.value
    DIRECT = StreamProducerOrigin.DIRECT.value


class StreamProducerIdentityPayload(dict[str, StreamProducerPayloadValue]):
    """Wire payload for one stream producer identity."""

    @classmethod
    def from_identity(
        cls,
        identity: "StreamProducerIdentity",
    ) -> "StreamProducerIdentityPayload":
        return cls(
            origin=identity.origin,
            output_kind=identity.output_kind,
            output_key=identity.output_key,
            projection_key=identity.projection_key,
            step_name=identity.step_name,
            pipeline_position=identity.pipeline_position,
            step_scope_id=identity.step_scope_id,
            invocation_key=identity.invocation_key,
            artifact_kind=identity.artifact_kind,
        )


@dataclass(frozen=True, slots=True)
class StreamProducerIdentity:
    """Producer/output identity for one streamed viewer item."""

    origin: str
    output_kind: str
    output_key: str
    projection_key: str
    step_name: str | None = None
    pipeline_position: int | None = None
    step_scope_id: str | None = None
    invocation_key: str | None = None
    artifact_kind: str | None = None

    @classmethod
    def pipeline_output(
        cls,
        *,
        output_kind: str,
        output_key: str,
        projection_key: str,
        step_name: str,
        pipeline_position: int | None,
        step_scope_id: str | None = None,
        artifact_kind: str | None = None,
    ) -> "StreamProducerIdentity":
        """Build identity for one pipeline-produced stream output."""
        return cls(
            origin=StreamProducerOrigin.PIPELINE.value,
            output_kind=output_kind,
            output_key=output_key,
            projection_key=projection_key,
            step_name=step_name,
            pipeline_position=pipeline_position,
            step_scope_id=step_scope_id,
            artifact_kind=artifact_kind,
        )

    @classmethod
    def fixed_output(
        cls,
        kind: FixedStreamProducerIdentityKind,
        output_key: str,
    ) -> "StreamProducerIdentity":
        """Build identity for producer kinds whose origin owns the output kind."""
        return cls(
            origin=kind.value,
            output_kind=kind.value,
            output_key=output_key,
            projection_key=output_key,
        )

    @classmethod
    def from_payload(
        cls,
        payload: "StreamProducerIdentity | StreamProducerPayloadMapping",
    ) -> "StreamProducerIdentity":
        if isinstance(payload, cls):
            return payload
        if not isinstance(payload, Mapping):
            raise TypeError(
                "Stream producer identity must be a mapping or StreamProducerIdentity, "
                f"got {type(payload).__name__}."
            )
        return cls(
            origin=_required_payload_str(payload, "origin"),
            output_kind=_required_payload_str(payload, "output_kind"),
            output_key=_required_payload_str(payload, "output_key"),
            projection_key=_required_payload_str(payload, "projection_key"),
            step_name=_optional_payload_str(payload, "step_name"),
            pipeline_position=_optional_payload_int(payload, "pipeline_position"),
            step_scope_id=_optional_payload_str(payload, "step_scope_id"),
            invocation_key=_optional_payload_str(payload, "invocation_key"),
            artifact_kind=_optional_payload_str(payload, "artifact_kind"),
        )

    def to_payload(self) -> StreamProducerIdentityPayload:
        return StreamProducerIdentityPayload.from_identity(self)

    def matches_declaration(self, declaration: StreamProducerIdentity) -> bool:
        """Return whether this observed identity satisfies a declaration.

        Pipeline position and runtime scope fields constrain a match when the
        declaration supplies them. An unbound declaration leaves those fields
        as ``None`` while every other identity field remains an exact
        constraint. The distinction lives on this nominal owner so consumers
        do not maintain partial identity comparisons.
        """

        if not isinstance(declaration, StreamProducerIdentity):
            raise TypeError(
                "Stream producer declaration must be a StreamProducerIdentity, "
                f"got {type(declaration).__name__}."
            )
        expected = replace(
            declaration,
            pipeline_position=(
                self.pipeline_position
                if declaration.pipeline_position is None
                else declaration.pipeline_position
            ),
            step_scope_id=(
                self.step_scope_id
                if declaration.step_scope_id is None
                else declaration.step_scope_id
            ),
            invocation_key=(
                self.invocation_key
                if declaration.invocation_key is None
                else declaration.invocation_key
            ),
        )
        return self == expected

    def route_parts(self) -> tuple[str, ...]:
        parts = [
            f"origin_{self.origin}",
            f"kind_{self.output_kind}",
            f"projection_{self.projection_key}",
        ]
        if self.pipeline_position is not None:
            parts.append(f"step_{self.pipeline_position}")
        if self.step_scope_id:
            parts.append(f"scope_{self.step_scope_id}")
        if self.step_name:
            parts.append(f"name_{self.step_name}")
        return tuple(parts)

def _required_payload_str(
    payload: StreamProducerPayloadMapping,
    field_name: str,
) -> str:
    if field_name not in payload:
        raise ValueError(
            f"Stream producer identity missing required field: {field_name}"
        )
    value = payload[field_name]
    if value in (None, ""):
        raise ValueError(
            f"Stream producer identity missing required field: {field_name}"
        )
    return str(value)


def _optional_payload_str(
    payload: StreamProducerPayloadMapping,
    field_name: str,
) -> str | None:
    if field_name not in payload:
        return None
    return _optional_str(payload[field_name])


def _optional_payload_int(
    payload: StreamProducerPayloadMapping,
    field_name: str,
) -> int | None:
    if field_name not in payload:
        return None
    value = payload[field_name]
    if value is None:
        return None
    return int(value)


def _optional_str(value: StreamProducerPayloadValue) -> str | None:
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
    def token(value: RouteKeyPart) -> str:
        return str(value).replace("/", "_").replace("\\", "_").replace(" ", "_")

    @classmethod
    def join(cls, parts: Sequence[RouteKeyPart]) -> str:
        if not parts:
            raise ValueError("Cannot build a stream route key with no parts.")
        return "_".join(cls.token(part) for part in parts)
