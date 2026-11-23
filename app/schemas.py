"""Pydantic request/response models for the port allocation API."""

from __future__ import annotations

import base64
import binascii
import ipaddress
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

PORT_MIN = 1
PORT_MAX = 65535
# A single UDP datagram carries at most 65507 bytes of payload over IPv4.
MAX_DATAGRAM_BYTES = 65507


class AllocateRequest(BaseModel):
    probe_ipv4: str = Field(..., description="Probe IPv4 address submitting the lease")
    probe_port: int = Field(..., ge=PORT_MIN, le=PORT_MAX)
    peer_ipv4: str = Field(..., description="Polled peer's IPv4 address")
    peer_port: int = Field(..., ge=PORT_MIN, le=PORT_MAX)

    @field_validator("probe_ipv4", "peer_ipv4")
    @classmethod
    def _must_be_ipv4(cls, value: str) -> str:
        try:
            ipaddress.IPv4Address(value)
        except (ipaddress.AddressValueError, ValueError) as exc:
            raise ValueError("must be a valid IPv4 address") from exc
        return value


class LeaseResponse(BaseModel):
    lease_id: str
    public_ipv4: str
    public_port: int
    probe_ipv4: str
    probe_port: int
    peer_ipv4: str
    peer_port: int
    ttl_seconds: float
    expires_at: datetime
    reused: bool = False


class LookupResponse(BaseModel):
    public_port: int
    peer_ipv4: str
    peer_port: int
    mapped: bool
    probe_ipv4: str | None = None
    probe_port: int | None = None
    reason: str | None = None


class MessageResponse(BaseModel):
    detail: str


class SendRequest(BaseModel):
    lease_id: str = Field(..., description="Valid lease id returned by POST /leases")
    payload_b64: str = Field(
        ...,
        description=(
            "Base64 encoded UDP payload (standard or URL-safe alphabet). "
            "Decoded size must not exceed 65507 bytes."
        ),
    )

    @field_validator("payload_b64")
    @classmethod
    def _must_be_base64(cls, value: str) -> str:
        try:
            decoded = _decode_base64(value)
        except ValueError as exc:
            raise ValueError("payload_b64 must be valid Base64") from exc
        if len(decoded) > MAX_DATAGRAM_BYTES:
            raise ValueError(
                f"payload_b64 decodes to {len(decoded)} bytes; "
                f"a UDP datagram holds at most {MAX_DATAGRAM_BYTES}"
            )
        return value


class SendResponse(BaseModel):
    lease_id: str
    public_ipv4: str
    public_port: int
    peer_ipv4: str
    peer_port: int
    bytes_sent: int


def _decode_base64(value: str) -> bytes:
    """Decode standard/URL-safe Base64; tolerate stripped '=' padding."""
    text = "".join(value.split())
    text += "=" * (-len(text) % 4)
    try:
        return base64.b64decode(text, altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("invalid base64") from exc


def decode_payload(value: str) -> bytes:
    """Public helper used by the route (validators call it too)."""
    return _decode_base64(value)


def _expiry_datetime(wall_deadline: float) -> datetime:
    return datetime.fromtimestamp(wall_deadline, tz=timezone.utc)
