"""FastAPI application: public UDP port lease allocation for the gateway."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import timezone
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from . import __version__
from .allocator import (
    Lease,
    LeaseNotFound,
    PoolExhausted,
    PortAllocator,
    PublicPortUnavailable,
)
from .config import ConfigError, Settings
from .forwarder import UdpForwarder, UdpNotBound, UdpSendError
from .schemas import (
    AllocateRequest,
    LeaseResponse,
    LookupResponse,
    MessageResponse,
    SendRequest,
    SendResponse,
    _expiry_datetime,
    decode_payload,
)

logger = logging.getLogger(__name__)


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
    forwarder: UdpForwarder | None = None
    if allocator is None:
        if settings.udp_forwarding_enabled:
            # The forwarder needs the allocator for reply lookups, while the
            # allocator needs the forwarder to bind before a lease is issued.
            holder: dict[str, PortAllocator] = {}
            forwarder = UdpForwarder(
                bind_host=settings.udp_bind_host,
                lookup=lambda port, ip, peer_port: holder["allocator"].lookup(
                    port, ip, peer_port
                ),
            )
        allocator = PortAllocator(
            pool_start=settings.pool_start,
            pool_end=settings.pool_end,
            lease_ttl_seconds=settings.lease_ttl_seconds,
            isolation_ttl_seconds=settings.isolation_ttl_seconds,
            forwarder=forwarder,
        )
        if forwarder is not None:
            holder["allocator"] = allocator
    else:
        forwarder = allocator.forwarder

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        """Own the background expiry sweep and the UDP sockets.

        The lease store is still lazy per request; this timer only enforces the
        forwarding requirement that the last socket on a public port is
        released on time even when no further request arrives.
        """
        sweeper: asyncio.Task | None = None
        if allocator.forwarder is not None:

            async def _loop() -> None:
                while True:
                    await asyncio.sleep(settings.sweep_interval_seconds)
                    try:
                        expired = await asyncio.to_thread(allocator.sweep_expired)
                    except Exception:  # pragma: no cover - keep the loop alive
                        logger.exception("expiry sweep failed")
                        continue
                    for port, lease_id in expired:
                        logger.info(
                            "lease %s expired on public port %s", lease_id, port
                        )

            sweeper = asyncio.create_task(_loop())
        try:
            yield
        finally:
            if sweeper is not None:
                sweeper.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await sweeper
            if forwarder is not None:
                await asyncio.to_thread(forwarder.close_all)

    app = FastAPI(
        title="Campus Gateway Port Allocation API",
        version=__version__,
        description=(
            "Allocates public UDP ports from a contiguous pool to probe/peer "
            "pairs and forwards their UDP polling. Leases are in-memory, last "
            "30 s, and returned/expired (port, peer) pairs are quarantined "
            "for 5 s."
        ),
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.allocator = allocator
    app.state.forwarder = forwarder

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
            "udp_forwarding_enabled": settings.udp_forwarding_enabled,
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
        except PublicPortUnavailable as exc:
            # Binding failed, so the allocator already rolled back the pool
            # slot: no lease id, no held port, no quarantine was recorded.
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"no lease issued: {exc}",
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

    @app.post(
        "/send",
        response_model=SendResponse,
        tags=["forwarding"],
        responses={
            404: {"model": MessageResponse},
            409: {"model": MessageResponse},
            502: {"model": MessageResponse},
        },
    )
    async def send(body: SendRequest) -> SendResponse:
        """Send a probe's UDP payload to its peer from the leased public port.

        Body carries the lease id and the Base64 payload. The datagram leaves
        from the lease's public port (source port = ``public_port``) towards the
        lease's peer, so the peer sees it as the probe's own polling packet.
        """
        lease = allocator.get_valid_lease(body.lease_id)
        if lease is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"unknown or expired lease id: {body.lease_id}",
            )
        if forwarder is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="UDP forwarding is disabled (set UDP_FORWARDING_ENABLED=true)",
            )
        payload = decode_payload(body.payload_b64)
        try:
            sent = await asyncio.to_thread(forwarder.send, lease, payload)
        except UdpNotBound as exc:
            # The lease may have been returned/expired between validation and
            # send; distinguish that from a lost socket.
            if allocator.get_valid_lease(body.lease_id) is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"unknown or expired lease id: {body.lease_id}",
                ) from exc
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=str(exc)
            ) from exc
        except UdpSendError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=str(exc),
            ) from exc
        return SendResponse(
            lease_id=lease.lease_id,
            public_ipv4=settings.public_ipv4,
            public_port=lease.public_port,
            peer_ipv4=lease.peer_ipv4,
            peer_port=lease.peer_port,
            bytes_sent=sent,
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
