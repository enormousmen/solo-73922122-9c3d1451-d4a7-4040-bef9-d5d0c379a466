"""UDP socket forwarding for leased public ports.

One UDP socket is bound per public port that has at least one valid lease.
Different peers may share the same public port, so several leases can point at
the same socket; the socket is only closed when the last of them ends.

Two directions are handled here:

* ``send``       probe -> peer.  The API gives us a lease id and a payload; the
                 datagram leaves from the lease's public port towards the
                 peer, exactly as if the probe had sent it itself.
* receive loop   peer -> probe.  Every datagram arriving on a public port is
                 looked up by (public port, source IPv4, source port).  A hit
                 is forwarded byte-for-byte to the probe that owns the lease;
                 a miss (including late replies during quarantine) is dropped.

Threading model: request threads bind sockets themselves (so a failed bind is
reported synchronously and the lease is refused), then hand selector
registration / unregistration to ONE selector thread through a command queue.
Only that thread ever touches the selector, and it never holds the state lock
while blocking in ``select()`` -- holding a lock across ``select`` starves
request threads for the whole timeout. Closing the last lease waits until the
selector thread has really closed the descriptor, so an immediate re-lease of
the same port binds cleanly.
"""

from __future__ import annotations

import logging
import selectors
import socket
import threading
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger(__name__)

MAX_UDP_PAYLOAD = 65507
SELECT_TIMEOUT_SECONDS = 0.1


class UdpForwardingError(RuntimeError):
    """Base class for forwarding failures."""


class UdpBindError(UdpForwardingError):
    """The public UDP port could not be bound on this host."""

    def __init__(self, host: str, port: int, cause: OSError) -> None:
        self.host = host
        self.port = port
        self.cause = cause
        super().__init__(
            f"cannot bind UDP {host}:{port}: {cause.strerror or cause}"
        )


class UdpNotBound(UdpForwardingError):
    """A send was attempted for a lease whose public port has no socket."""


class UdpSendError(UdpForwardingError):
    """The operating system rejected the outbound datagram."""


@dataclass
class _BoundPort:
    port: int
    sock: socket.socket
    lease_ids: set[str] = field(default_factory=set)
    registered: bool = False
    closed: bool = False
    close_done: threading.Event = field(default_factory=threading.Event)


class UdpForwarder:
    """Owns the public-port UDP sockets and performs both forward directions.

    ``lookup`` is called as ``lookup(public_port, source_ipv4, source_port)``
    and must return a lease-like object with ``probe_ipv4``/``probe_port``, or
    ``None`` when no valid mapping exists.
    """

    def __init__(
        self,
        *,
        bind_host: str,
        lookup: Callable[[int, str, int], object | None],
    ) -> None:
        self.bind_host = bind_host
        self._lookup = lookup
        # Guards _ports, _commands and _closed. Requests hold it only for
        # in-memory bookkeeping, never across a blocking call.
        self._lock = threading.RLock()
        self._ports: dict[int, _BoundPort] = {}
        self._commands: list[tuple[str, _BoundPort]] = []
        self._closed = False

        # Only the selector thread touches _selector after __init__.
        self._selector = selectors.DefaultSelector()
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._selector.register(self._wake_r, selectors.EVENT_READ, None)

        self._counter_lock = threading.Lock()
        self._counters = {
            "replies_forwarded": 0,
            "replies_dropped_no_mapping": 0,
            "reply_forward_errors": 0,
            "send_errors": 0,
            "lookup_errors": 0,
        }
        self._thread = threading.Thread(
            target=self._serve, name="udp-forwarder", daemon=True
        )
        self._thread.start()

    # ------------------------------------------------------------- allocator API

    def prepare_port(self, public_port: int) -> None:
        """Bind the port before a lease is committed.

        Raises :class:`UdpBindError` if the address is already in use or cannot
        be bound.  The allocator relies on the exception to roll back the port
        it just took from the free set, so nothing is left behind.
        """
        with self._lock:
            if public_port in self._ports:
                return
            self._bind_locked(public_port)

    def add_lease(self, public_port: int, lease_id: str) -> None:
        """Attach a lease to a public port's socket (binding if necessary)."""
        with self._lock:
            entry = self._ports.get(public_port)
            if entry is None:
                entry = self._bind_locked(public_port)
            entry.lease_ids.add(lease_id)

    def remove_lease(self, public_port: int, lease_id: str) -> None:
        """Detach a lease; close the socket when the last one is gone.

        Returns only after the descriptor is closed, so an immediate
        re-allocation of the same port binds cleanly.
        """
        with self._lock:
            entry = self._ports.get(public_port)
            if entry is None:
                return
            entry.lease_ids.discard(lease_id)
            if entry.lease_ids:
                return
            del self._ports[public_port]
        self._close(entry)

    def release_port_if_unused(self, public_port: int) -> None:
        """Rollback hook for a port bound during a failed allocation."""
        with self._lock:
            entry = self._ports.get(public_port)
            if entry is None or entry.lease_ids:
                return
            del self._ports[public_port]
        self._close(entry)

    # --------------------------------------------------------------- outbound

    def send(self, lease: object, payload: bytes) -> int:
        """Send one datagram from the lease's public port to its peer."""
        public_port = lease.public_port  # type: ignore[attr-defined]
        lease_id = lease.lease_id  # type: ignore[attr-defined]
        with self._lock:
            entry = self._ports.get(public_port)
            if entry is None or lease_id not in entry.lease_ids:
                raise UdpNotBound(
                    f"no UDP socket for lease {lease_id} on public port {public_port}"
                )
            try:
                return entry.sock.sendto(
                    payload,
                    (lease.peer_ipv4, lease.peer_port),  # type: ignore[attr-defined]
                )
            except OSError as exc:
                self._incr("send_errors")
                raise UdpSendError(
                    f"failed to send UDP to peer "
                    f"{lease.peer_ipv4}:{lease.peer_port}: {exc}"  # type: ignore[attr-defined]
                ) from exc

    # --------------------------------------------------------------- lifecycle

    def _bind_locked(self, public_port: int) -> _BoundPort:
        if self._closed:
            raise UdpForwardingError("UDP forwarder is closed")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # Deliberately no SO_REUSEADDR: if another process already owns the
            # port, lease issuance must fail instead of silently splitting
            # datagrams between two sockets.
            sock.bind((self.bind_host, public_port))
            sock.setblocking(False)
        except OSError as exc:
            sock.close()
            raise UdpBindError(self.bind_host, public_port, exc) from exc

        entry = _BoundPort(port=public_port, sock=sock)
        self._ports[public_port] = entry
        self._commands.append(("register", entry))
        self._wake()
        logger.info("bound public UDP port %s:%s", self.bind_host, public_port)
        return entry

    def _close(self, entry: _BoundPort) -> None:
        if threading.current_thread() is self._thread:
            # Called from inside the reply path (lookup triggered an expiry).
            # We own the selector here, so close synchronously.
            self._apply_close(entry)
            return
        with self._lock:
            self._commands.append(("close", entry))
        self._wake()
        entry.close_done.wait(2.0)
        if not entry.close_done.is_set():  # pragma: no cover - defensive
            logger.warning("timed out closing UDP port %s", entry.port)

    def _wake(self) -> None:
        try:
            self._wake_w.send(b"\x00")
        except OSError:
            pass  # forwarder already shut down

    def _apply_close(self, entry: _BoundPort) -> None:
        if entry.closed:
            return
        if entry.registered:
            try:
                self._selector.unregister(entry.sock)
            except (KeyError, ValueError, OSError):
                pass
            entry.registered = False
        try:
            entry.sock.close()
        except OSError:
            pass
        entry.closed = True
        entry.close_done.set()
        logger.info("released public UDP port %s:%s", self.bind_host, entry.port)

    def _apply_commands(self) -> bool:
        """Apply queued registrations/closes; return True if any were queued."""
        with self._lock:
            commands, self._commands = self._commands, []
        for action, entry in commands:
            if action == "register":
                if entry.closed:
                    continue
                try:
                    self._selector.register(entry.sock, selectors.EVENT_READ, entry)
                    entry.registered = True
                except (KeyError, ValueError, OSError):  # pragma: no cover
                    pass
            else:
                self._apply_close(entry)
        return bool(commands)

    def _serve(self) -> None:
        while True:
            had_commands = self._apply_commands()
            with self._lock:
                stopped = self._closed
                pending = bool(self._commands)
            if stopped and not pending and not had_commands:
                return
            try:
                events = self._selector.select(SELECT_TIMEOUT_SECONDS)
            except (OSError, ValueError):  # pragma: no cover - selector torn down
                return
            for key, _mask in events:
                if key.data is None:
                    self._drain_wakeup()
                else:
                    self._receive_ready(key.data)

    def _drain_wakeup(self) -> None:
        while True:
            try:
                if not self._wake_r.recv(4096):
                    return
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                return

    def _receive_ready(self, entry: _BoundPort) -> None:
        while not entry.closed:
            try:
                data, addr = entry.sock.recvfrom(MAX_UDP_PAYLOAD)
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                return
            self._handle_reply(entry, data, addr)

    # --------------------------------------------------------------- inbound

    def _handle_reply(
        self, entry: _BoundPort, data: bytes, addr: tuple[str, int]
    ) -> None:
        source_ipv4, source_port = addr[0], addr[1]
        try:
            lease = self._lookup(entry.port, source_ipv4, source_port)
        except Exception:  # pragma: no cover - defensive: never kill the thread
            logger.exception("mapping lookup failed for reply on port %s", entry.port)
            self._incr("lookup_errors")
            return
        if lease is None:
            # Unknown pair or quarantined pair: drop the datagram, no ICMP,
            # nothing forwarded to any probe.
            self._incr("replies_dropped_no_mapping")
            return
        if entry.closed:  # the lease may have expired inside lookup()
            return
        try:
            entry.sock.sendto(
                data,
                (lease.probe_ipv4, lease.probe_port),  # type: ignore[attr-defined]
            )
            self._incr("replies_forwarded")
        except OSError:
            self._incr("reply_forward_errors")

    # ------------------------------------------------------------ diagnostics

    def _incr(self, name: str) -> None:
        with self._counter_lock:
            self._counters[name] += 1

    def is_bound(self, public_port: int) -> bool:
        with self._lock:
            return public_port in self._ports

    def bound_ports(self) -> list[int]:
        with self._lock:
            return sorted(self._ports)

    def snapshot(self) -> dict:
        with self._lock:
            ports = {
                str(port): {"active_leases": len(entry.lease_ids)}
                for port, entry in sorted(self._ports.items())
            }
        with self._counter_lock:
            counters = dict(self._counters)
        return {
            "enabled": True,
            "bind_host": self.bind_host,
            "bound_ports": ports,
            "counters": counters,
        }

    def close_all(self) -> None:
        """Close every socket and stop the selector thread."""
        with self._lock:
            entries = list(self._ports.values())
            self._ports.clear()
            for entry in entries:
                self._commands.append(("close", entry))
            self._closed = True
        self._wake()
        self._thread.join(timeout=2.0)
        for entry in entries:
            entry.close_done.wait(0.5)
        try:
            self._selector.close()
        except OSError:  # pragma: no cover
            pass
        for sock in (self._wake_r, self._wake_w):
            try:
                sock.close()
            except OSError:
                pass
