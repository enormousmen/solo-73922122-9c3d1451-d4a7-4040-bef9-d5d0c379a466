"""Pydantic request/response models for the port allocation API."""

from __future__ import annotations

import ipaddress
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

PORT_MIN = 1
PORT_MAX = 65535


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


def _expiry_datetime(wall_deadline: float) -> datetime:
    return datetime.fromtimestamp(wall_deadline, tz=timezone.utc)
