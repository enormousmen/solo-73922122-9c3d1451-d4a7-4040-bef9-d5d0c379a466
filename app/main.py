"""FastAPI application: public UDP port lease allocation for the gateway."""

from __future__ import annotations

from datetime import timezone

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from . import __version__
from .allocator import Lease, LeaseNotFound, PoolExhausted, PortAllocator
from .config import ConfigError, Settings
from .schemas import (
    AllocateRequest,
    LeaseResponse,
    LookupResponse,
    MessageResponse,
    _expiry_datetime,
)


def build_lease_response(
    lease: Lease, public_ipv4: str, ttl_seconds: float, *, reused: bool
) -> LeaseResponse:
    return LeaseResponse(
        lease_id=lease.lease_id,
        public_ipv4=public_ipv4,
        public_port=lease.public_port,
        probe_ipv4=lease.probe_ipv4,
        probe_port=lease.probe_port,
        peer_ipv4=lease.peer_ipv4,
        peer_port=lease.peer_port,
        ttl_seconds=ttl_seconds,
        expires_at=_expiry_datetime(lease.expires_at_wall).astimezone(timezone.utc),
        reused=reused,
    )


def create_app(settings: Settings | None = None, allocator: PortAllocator | None = None) -> FastAPI:
    """Application factory.

    ``allocator`` injection exists so the test suite can run with a fake
    clock; production wiring builds one straight from the environment.
    """
    if settings is None:
        settings = Settings.from_env()
    if allocator is None:
        allocator = PortAllocator(
            pool_start=settings.pool_start,
            pool_end=settings.pool_end,
            lease_ttl_seconds=settings.lease_ttl_seconds,
            isolation_ttl_seconds=settings.isolation_ttl_seconds,
        )

    app = FastAPI(
        title="Campus Gateway Port Allocation API",
        version=__version__,
        description=(
            "Allocates public UDP ports from a contiguous pool to probe/peer "
            "pairs. Leases are in-memory, last 30 s, and returned/expired "
            "(port, peer) pairs are quarantined for 5 s."
        ),
    )
    app.state.settings = settings
    app.state.allocator = allocator

    @app.exception_handler(ConfigError)
    async def _config_error_handler(_request: Request, exc: ConfigError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": f"configuration error: {exc}"})

    @app.get("/healthz", response_model=MessageResponse, tags=["ops"])
    async def healthz() -> MessageResponse:
        return MessageResponse(detail="ok")

    @app.get("/", tags=["ops"])
    async def root() -> dict:
        return {
            "service": "campus-gateway-port-allocator",
            "version": __version__,
            "public_ipv4": settings.public_ipv4,
            "pool": {"start": settings.pool_start, "end": settings.pool_end},
            "lease_ttl_seconds": settings.lease_ttl_seconds,
            "isolation_ttl_seconds": settings.isolation_ttl_seconds,
            "docs": "/docs",
        }

    @app.post(
        "/leases",
        response_model=LeaseResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["leases"],
        responses={503: {"model": MessageResponse}},
    )
    async def allocate(body: AllocateRequest) -> LeaseResponse:
        try:
            lease, reused = allocator.allocate(
                probe_ipv4=body.probe_ipv4,
                probe_port=body.probe_port,
                peer_ipv4=body.peer_ipv4,
                peer_port=body.peer_port,
            )
        except PoolExhausted as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    f"pool exhausted for peer {body.peer_ipv4}:{body.peer_port}: {exc}"
                ),
            ) from exc
        return build_lease_response(
            lease, settings.public_ipv4, settings.lease_ttl_seconds, reused=reused
        )

    @app.post(
        "/leases/{lease_id}/renew",
        response_model=LeaseResponse,
        tags=["leases"],
        responses={404: {"model": MessageResponse}},
    )
    async def renew(lease_id: str) -> LeaseResponse:
        try:
            lease = allocator.renew(lease_id)
        except LeaseNotFound as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown or expired lease id: {lease_id}",
            ) from exc
        return build_lease_response(
            lease, settings.public_ipv4, settings.lease_ttl_seconds, reused=True
        )

    @app.post(
        "/leases/{lease_id}/release",
        response_model=LeaseResponse,
        tags=["leases"],
        responses={404: {"model": MessageResponse}},
    )
    async def release(lease_id: str) -> LeaseResponse:
        try:
            lease = allocator.release(lease_id)
        except LeaseNotFound as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown or expired lease id: {lease_id}",
            ) from exc
        return build_lease_response(
            lease, settings.public_ipv4, settings.lease_ttl_seconds, reused=False
        )

    @app.get(
        "/mappings",
        response_model=LookupResponse,
        tags=["mappings"],
        responses={404: {"model": LookupResponse}},
    )
    async def lookup(public_port: int, peer_ipv4: str, peer_port: int):
        lease = allocator.lookup(public_port, peer_ipv4, peer_port)
        if lease is None:
            # 404 carries the structured "no mapping" body so clients get one
            # schema whether the pair was never seen or is quarantined.
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content=LookupResponse(
                    public_port=public_port,
                    peer_ipv4=peer_ipv4,
                    peer_port=peer_port,
                    mapped=False,
                    reason="no mapping (unknown pair or port is quarantined)",
                ).model_dump(),
            )
        return LookupResponse(
            public_port=public_port,
            peer_ipv4=peer_ipv4,
            peer_port=peer_port,
            mapped=True,
            probe_ipv4=lease.probe_ipv4,
            probe_port=lease.probe_port,
        )

    @app.get("/debug/state", tags=["ops"])
    async def debug_state() -> dict:
        return allocator.snapshot()

    return app


app = create_app()
