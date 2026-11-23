"""In-memory UDP port lease allocator.

Design constraints (from the gateway requirements)
---------------------------------------------------
1. The public port pool is one contiguous range. A public port may be shared
   by different peers, but one peer may never hold the same port twice.
2. Allocation must not scan the pool port by port, and we must not pre-build a
   full per-peer table covering the whole pool (the pool may be large while any
   single peer only uses a few ports).
3. Leases live 30 seconds; renewing or returning requires a valid lease id.
   Old/unknown lease ids must never touch a new lease on the same port.
4. After a lease is returned or expires, only the (public_port, peer) pair is
   quarantined for 5 seconds. The same port stays usable by other peers and
   lookups for the quarantined pair must report "no mapping".

Data model per peer (lazy, created on first use)
------------------------------------------------
* ``free``  : a :class:`FreeIntervalSet` describing ports the peer may still
              take. It starts as ONE interval ``[pool_start, pool_end]`` and
              only splits as ports are taken, so memory is proportional to the
              ports a peer actually touches, never to the pool size.
* ``leases``: key -> Lease for the peer's currently valid leases.
* ``held``  : set of public ports held by valid leases (also the ports this
              peer currently occupies).
* ``quarantine_until``: per-port monotonic deadline; the port cannot be
              re-allocated to this peer, and lookups return no mapping, until
              the deadline passes.
* two heaps give O(log n) expiry processing instead of periodic sweeps.

UDP forwarding hook
-------------------
When a :class:`~app.forwarder.UdpForwarder` is attached, a public port must be
bound before its lease is committed (``prepare_port``) and the lease is
attached to the port's socket (``add_lease``). A bind failure rolls the pool
slot back and raises :class:`PublicPortUnavailable`, leaving no lease, no held
port and no quarantine entry. Return and expiry both detach the lease; the
forwarder closes the socket when the last lease on that port ends. Because the
business state is otherwise lazy, ``sweep_expired`` is invoked by a small timer
in the app so expiry alone still releases sockets.

All operations are O(log n) in the number of entries a peer actually uses;
there is no O(pool size) allocation step anywhere.
"""

from __future__ import annotations

import contextlib
import heapq
import itertools
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Iterator, Optional

from .forwarder import UdpForwardingError, UdpForwarder

logger = logging.getLogger(__name__)


class LeaseNotFound(KeyError):
    """Raised when a lease id is unknown or no longer valid for this peer."""


class PoolExhausted(RuntimeError):
    """Raised when the requesting peer already holds/quarantines every port."""


class PublicPortUnavailable(RuntimeError):
    """The public UDP port could not be bound, so no lease was issued.

    The allocator has already rolled back the pool slot; raising this leaves
    no lease, no held port and no quarantine entry behind.
    """


class FreeIntervalSet:
    """Disjoint, inclusive integer intervals with split / merge semantics.

    Storage is a dict ``start -> end`` plus a sorted list of starts. No object
    or entry is ever created per individual port: a pool of 60k ports is a
    single one-element interval until something is actually taken.

    Complexity: allocation and release are O(log k + s), where *k* is the
    number of live intervals (bounded by the ports the peer actually holds)
    and *s* is the number of intervals merged on release (normally two).
    """

    __slots__ = ("_starts", "_ends")

    def __init__(self) -> None:
        self._starts: list[int] = []
        self._ends: dict[int, int] = {}

    def add(self, start: int, end: int) -> None:
        """Add the inclusive interval [start, end], merging any neighbours."""
        if start > end:
            raise ValueError("interval start must not be greater than end")
        new_start, new_end = start, end

        # Every interval whose start is <= new_end + 1 might overlap or touch.
        idx = 0
        # bisect_right, written inline to keep the class self-contained.
        lo, hi = 0, len(self._starts)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._starts[mid] <= new_end + 1:
                lo = mid + 1
            else:
                hi = mid
        idx = lo

        while idx > 0:
            candidate_start = self._starts[idx - 1]
            candidate_end = self._ends[candidate_start]
            if candidate_end + 1 < new_start:
                break  # gap before this interval: nothing earlier can touch.
            new_start = min(new_start, candidate_start)
            new_end = max(new_end, candidate_end)
            idx -= 1

        # Remove every swallowed interval from idx up to the split point.
        while idx < len(self._starts) and self._starts[idx] <= new_end:
            swallowed_start = self._starts.pop(idx)
            new_end = max(new_end, self._ends.pop(swallowed_start))

        self._starts.insert(idx, new_start)
        self._ends[new_start] = new_end

    def pop_any(self) -> int:
        """Remove and return one port, splitting the containing interval.

        The last interval is consumed first, so the common case (pool still
        contiguous) removes the list's tail in O(1).
        """
        if not self._starts:
            raise IndexError("no free ports")
        start = self._starts[-1]
        end = self._ends[start]
        port = end
        if start == end:
            self._starts.pop()
            del self._ends[start]
        else:
            self._ends[start] = end - 1
        return port

    def is_empty(self) -> bool:
        return not self._starts

    def intervals(self) -> list[tuple[int, int]]:
        return [(start, self._ends[start]) for start in self._starts]


@dataclass
class Lease:
    lease_id: str
    public_port: int
    probe_ipv4: str
    probe_port: int
    peer_ipv4: str
    peer_port: int
    created_at: float          # monotonic clock
    expires_at: float          # monotonic clock
    expires_at_wall: float     # wall clock, for client-facing timestamps
    renewed: bool = False


@dataclass
class PeerState:
    peer_key: tuple[str, int]
    free: FreeIntervalSet = field(default_factory=FreeIntervalSet)
    leases: dict[str, Lease] = field(default_factory=dict)
    held: set[int] = field(default_factory=set)
    quarantine_until: dict[int, float] = field(default_factory=dict)
    # (expiry monotonic deadline, lease id) -- stale ids are skipped lazily.
    lease_expiry_heap: list[tuple[float, str]] = field(default_factory=list)
    # (quarantine monotonic deadline, port) -- stale entries skipped lazily.
    quarantine_heap: list[tuple[float, int]] = field(default_factory=list)


class PortAllocator:
    """Thread-safe, in-memory lease store for a single API instance."""

    def __init__(
        self,
        pool_start: int,
        pool_end: int,
        lease_ttl_seconds: float,
        isolation_ttl_seconds: float,
        *,
        clock=time.monotonic,
        wall_clock=time.time,
        forwarder: Optional[UdpForwarder] = None,
    ) -> None:
        if pool_start > pool_end:
            raise ValueError("pool_start must not be greater than pool_end")
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        if isolation_ttl_seconds < 0:
            raise ValueError("isolation_ttl_seconds must not be negative")
        self.pool_start = pool_start
        self.pool_end = pool_end
        self.lease_ttl = float(lease_ttl_seconds)
        self.isolation_ttl = float(isolation_ttl_seconds)
        self._clock = clock
        self._wall_clock = wall_clock
        # When set, a public port is only leased after its UDP socket bound.
        self.forwarder = forwarder
        self._peers: dict[tuple[str, int], PeerState] = {}
        # public_port -> set of peer keys currently holding a valid lease on
        # it. Drives the "same port to different peers" sharing rule.
        self._port_users: dict[int, set[tuple[str, int]]] = {}
        # lease_id -> owning peer key, so renew/release never scan all peers
        # and an old id can never resolve to a newer lease on the same port.
        self._lease_owner: dict[str, tuple[str, int]] = {}
        self._lock = threading.RLock()
        # Ports whose last lease ended while the lock was held. The forwarder
        # close is executed *after* the lock is released: closing waits for the
        # selector thread, which itself calls back into lookup() and needs this
        # lock -- waiting while holding it would deadlock.
        self._pending_release: list[tuple[int, str]] = []
        # Monotonic sequence makes lease ids unique even if uuid4 collided.
        self._id_seq = itertools.count(1)

    # ------------------------------------------------------------------ utils

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        """Hold the state lock, then hand finished sockets to the forwarder."""
        try:
            with self._lock:
                yield
        finally:
            # Always drain, even when the body raised: a pool-exhausted or
            # not-found path may already have expired leases.
            self._flush_pending_releases()

    def _flush_pending_releases(self) -> None:
        if self.forwarder is None:
            return
        while True:
            with self._lock:
                if not self._pending_release:
                    return
                port, lease_id = self._pending_release.pop()
            try:
                self.forwarder.remove_lease(port, lease_id)
            except Exception:  # pragma: no cover - keep the lease store sane
                logger.exception(
                    "failed to detach lease %s from public port %s", lease_id, port
                )

    def _queue_release(self, port: int, lease_id: str) -> None:
        """Detach a lease from its socket (deferred until the lock is free)."""
        if self.forwarder is not None:
            self._pending_release.append((port, lease_id))

    def _now(self) -> float:
        return self._clock()

    def _new_lease_id(self) -> str:
        return f"lease-{next(self._id_seq):08d}-{uuid.uuid4().hex[:12]}"

    def _peer(self, peer_ipv4: str, peer_port: int) -> PeerState:
        key = (peer_ipv4, peer_port)
        state = self._peers.get(key)
        if state is None:
            # Lazy initialisation: the pool appears as a single free interval,
            # so first allocation never touches pool_size objects.
            state = PeerState(peer_key=key)
            state.free.add(self.pool_start, self.pool_end)
            self._peers[key] = state
        return state

    def _process_expirations(self, state: PeerState, now: float) -> None:
        """Move due leases and due quarantines into/back out of quarantine.

        Runs at the start of every operation for this peer; nothing runs on a
        background timer, so no lease can disappear mid-request.
        """
        # 1. Expired leases -> quarantine the (port, peer) pair.
        self._expire_due(state, now)

        # 2. Finished quarantine -> port becomes allocatable to this peer
        #    again. Only this (port, peer) pair is affected; other peers keep
        #    whatever leases they hold on the same port.
        while state.quarantine_heap and state.quarantine_heap[0][0] <= now:
            until, port = heapq.heappop(state.quarantine_heap)
            current = state.quarantine_until.get(port)
            if current is None or current > now:
                continue  # superseded by a newer quarantine, or already freed.
            del state.quarantine_until[port]
            state.free.add(port, port)

    # --------------------------------------------------------------- public API

    def allocate(
        self,
        *,
        probe_ipv4: str,
        probe_port: int,
        peer_ipv4: str,
        peer_port: int,
    ) -> tuple[Lease, bool]:
        """Allocate (or return the existing valid lease) for probe+peer.

        Returns ``(lease, reused)``. Repeated calls while a lease is still
        valid return the original lease unchanged (its 30 s window is NOT
        reset here -- that is what renew is for).
        """
        with self._locked():
            now = self._now()
            state = self._peer(peer_ipv4, peer_port)
            self._process_expirations(state, now)

            matching = next(
                (
                    lease
                    for lease in state.leases.values()
                    if (
                        lease.probe_ipv4 == probe_ipv4
                        and lease.probe_port == probe_port
                        and lease.peer_ipv4 == peer_ipv4
                        and lease.peer_port == peer_port
                    )
                ),
                None,
            )

            # A peer that holds or has quarantined every port is full. The
            # only request still allowed in that state is the idempotent
            # repeat of its own (probe, peer) pair; a different probe asking
            # for another port gets 503 rather than an allocation from an
            # empty free set.
            if state.free.is_empty():
                if matching is not None:
                    return matching, True
                raise PoolExhausted(
                    f"peer {peer_ipv4}:{peer_port} already holds or has "
                    f"quarantined every port in pool "
                    f"{self.pool_start}-{self.pool_end}"
                )

            if matching is not None:
                return matching, True

            public_port = state.free.pop_any()
            lease_id = self._new_lease_id()
            return self._commit_lease(
                state=state,
                now=now,
                public_port=public_port,
                lease_id=lease_id,
                probe_ipv4=probe_ipv4,
                probe_port=probe_port,
                peer_ipv4=peer_ipv4,
                peer_port=peer_port,
            ), False

    def _commit_lease(
        self,
        *,
        state: PeerState,
        now: float,
        public_port: int,
        lease_id: str,
        probe_ipv4: str,
        probe_port: int,
        peer_ipv4: str,
        peer_port: int,
    ) -> Lease:
        """Bind the public port, then commit the lease; roll back on failure.

        With UDP forwarding enabled, a lease is only handed out after the
        socket bound. If binding raises, the free-set split is undone and
        nothing else was written yet -- no lease, no held slot, no quarantine.
        """
        if self.forwarder is not None:
            try:
                self.forwarder.prepare_port(public_port)
            except UdpForwardingError as exc:
                state.free.add(public_port, public_port)
                raise PublicPortUnavailable(
                    f"public port {public_port} is unavailable: {exc}"
                ) from exc

        lease = Lease(
            lease_id=lease_id,
            public_port=public_port,
            probe_ipv4=probe_ipv4,
            probe_port=probe_port,
            peer_ipv4=peer_ipv4,
            peer_port=peer_port,
            created_at=now,
            expires_at=now + self.lease_ttl,
            expires_at_wall=self._wall_clock() + self.lease_ttl,
        )
        state.leases[lease_id] = lease
        state.held.add(public_port)
        heapq.heappush(state.lease_expiry_heap, (lease.expires_at, lease_id))
        self._lease_owner[lease_id] = state.peer_key
        self._port_users.setdefault(public_port, set()).add(state.peer_key)
        if self.forwarder is not None:
            try:
                self.forwarder.add_lease(public_port, lease_id)
            except UdpForwardingError as exc:
                # Extremely defensive: undo the in-memory commit as well.
                self._uncommit_lease(state, lease)
                raise PublicPortUnavailable(
                    f"public port {public_port} is unavailable: {exc}"
                ) from exc
        return lease

    def _uncommit_lease(self, state: PeerState, lease: Lease) -> None:
        state.leases.pop(lease.lease_id, None)
        state.held.discard(lease.public_port)
        self._lease_owner.pop(lease.lease_id, None)
        users = self._port_users.get(lease.public_port)
        if users is not None:
            users.discard(state.peer_key)
            if not users:
                del self._port_users[lease.public_port]
        state.free.add(lease.public_port, lease.public_port)
        if self.forwarder is not None:
            self.forwarder.release_port_if_unused(lease.public_port)

    def renew(self, lease_id: str) -> Lease:
        """Reset a valid lease's lifetime to a full 30 s.

        Unknown / returned / expired ids raise :class:`LeaseNotFound`; an old
        id therefore can never touch a new lease that reused the same port.
        """
        with self._locked():
            now = self._now()
            owner = self._lease_owner.get(lease_id)
            if owner is None:
                raise LeaseNotFound(lease_id)
            state = self._peers[owner]
            self._process_expirations(state, now)
            lease = state.leases.get(lease_id)
            if lease is None:
                raise LeaseNotFound(lease_id)
            lease.expires_at = now + self.lease_ttl
            lease.expires_at_wall = self._wall_clock() + self.lease_ttl
            lease.renewed = True
            heapq.heappush(state.lease_expiry_heap, (lease.expires_at, lease_id))
            return lease

    def _expire_lease(self, state: PeerState, lease: Lease, now: float) -> None:
        """Remove an expired lease, drop its socket, quarantine (port, peer)."""
        state.leases.pop(lease.lease_id, None)
        self._lease_owner.pop(lease.lease_id, None)
        port = lease.public_port
        state.held.discard(port)
        users = self._port_users.get(port)
        if users is not None:
            users.discard(state.peer_key)
            if not users:
                del self._port_users[port]
        # Last lease on this public port -> the socket is closed and its
        # receive thread stops without needing another request.
        self._queue_release(port, lease.lease_id)
        until = now + self.isolation_ttl
        old = state.quarantine_until.get(port)
        if old is None or until > old:
            state.quarantine_until[port] = until
            heapq.heappush(state.quarantine_heap, (until, port))

    def release(self, lease_id: str) -> Lease:
        """Return a valid lease; only its (port, peer) pair is quarantined."""
        with self._locked():
            now = self._now()
            owner = self._lease_owner.get(lease_id)
            if owner is None:
                raise LeaseNotFound(lease_id)
            state = self._peers[owner]
            self._process_expirations(state, now)
            lease = state.leases.pop(lease_id, None)
            if lease is None:
                # It expired inside _process_expirations, which already moved
                # the (port, peer) pair into quarantine.
                self._lease_owner.pop(lease_id, None)
                raise LeaseNotFound(lease_id)
            self._lease_owner.pop(lease_id, None)
            port = lease.public_port
            state.held.discard(port)
            users = self._port_users.get(port)
            if users is not None:
                users.discard(state.peer_key)
                if not users:
                    del self._port_users[port]
            # If another peer still holds this port its lease stays attached
            # and the socket is kept; only the last lease closes it.
            self._queue_release(port, lease_id)
            until = now + self.isolation_ttl
            state.quarantine_until[port] = until
            heapq.heappush(state.quarantine_heap, (until, port))
            return lease

    def get_valid_lease(self, lease_id: str) -> Optional[Lease]:
        """Return a lease if it is still valid, otherwise ``None``.

        Used by the send API: an unknown, released or expired id must not be
        able to push a datagram out of a public port.
        """
        with self._locked():
            owner = self._lease_owner.get(lease_id)
            if owner is None:
                return None
            state = self._peers[owner]
            self._process_expirations(state, self._now())
            return state.leases.get(lease_id)

    def lookup(
        self, public_port: int, peer_ipv4: str, peer_port: int
    ) -> Optional[Lease]:
        """Where should a reply arriving on public_port be forwarded?

        Returns the valid lease, or ``None`` when the (port, peer) pair has no
        mapping -- including while it is inside the 5 s quarantine window, so
        late replies during isolation are reported as unmapped.
        """
        with self._locked():
            state = self._peers.get((peer_ipv4, peer_port))
            if state is None:
                return None
            now = self._now()
            self._process_expirations(state, now)
            if public_port in state.quarantine_until:
                return None
            for lease in state.leases.values():
                if lease.public_port == public_port:
                    return lease
            return None

    def sweep_expired(self) -> list[tuple[int, str]]:
        """Process expirations for every peer without an inbound request.

        Called periodically by the app so that when the last lease on a public
        port expires the UDP socket is closed and its receive thread stops even
        if nothing else ever touches the service. Returns the (port, lease id)
        pairs that actually expired, for logging.
        """
        expired: list[tuple[int, str]] = []
        with self._locked():
            now = self._now()
            for state in self._peers.values():
                expired.extend(self._expire_due(state, now))
        return expired

    def _expire_due(self, state: PeerState, now: float) -> list[tuple[int, str]]:
        """Pop due lease heap entries, forwarding each real expiry."""
        expired: list[tuple[int, str]] = []
        while state.lease_expiry_heap and state.lease_expiry_heap[0][0] <= now:
            deadline, lease_id = heapq.heappop(state.lease_expiry_heap)
            lease = state.leases.get(lease_id)
            if lease is None:
                continue  # already returned.
            if lease.expires_at != deadline:
                continue  # stale entry left behind by a renewal.
            self._expire_lease(state, lease, now)
            expired.append((lease.public_port, lease_id))
        return expired

    # ------------------------------------------------------------- diagnostics

    def snapshot(self) -> dict:
        """Debug view of the whole store (used by GET /debug/state)."""
        with self._locked():
            now = self._now()
            peers_out: dict[str, dict] = {}
            for (peer_ip, peer_port), state in self._peers.items():
                self._process_expirations(state, now)
                peers_out[f"{peer_ip}:{peer_port}"] = {
                    "held_ports": sorted(state.held),
                    "free_intervals": state.free.intervals(),
                    "quarantine": {
                        str(port): round(until - now, 3)
                        for port, until in sorted(state.quarantine_until.items())
                    },
                    "active_leases": sorted(state.leases.keys()),
                }
            return {
                "pool": {"start": self.pool_start, "end": self.pool_end},
                "lease_ttl_seconds": self.lease_ttl,
                "isolation_ttl_seconds": self.isolation_ttl,
                "udp_forwarding": (
                    self.forwarder.snapshot() if self.forwarder is not None else None
                ),
                "port_users": {
                    str(port): sorted(f"{ip}:{p}" for ip, p in users)
                    for port, users in sorted(self._port_users.items())
                },
                "peers": peers_out,
            }
