"""UDP forwarding tests: real sockets on 127.0.0.1, fake clock for expiry."""

from __future__ import annotations

import base64
import socket
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient  # noqa: E402

from app.allocator import (  # noqa: E402
    PortAllocator,
    PublicPortUnavailable,
)
from app.config import Settings  # noqa: E402
from app.forwarder import UdpBindError, UdpForwarder  # noqa: E402
from app.main import create_app  # noqa: E402

BIND_HOST = "127.0.0.1"
PROBE = {"probe_ipv4": "127.0.0.1", "probe_port": 5061}
PEER_1 = {"peer_ipv4": "127.0.0.1", "peer_port": 7061}
PEER_2 = {"peer_ipv4": "127.0.0.1", "peer_port": 7062}


class FakeClock:
    def __init__(self) -> None:
        self.t = 10_000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _free_udp_port() -> int:
    """Pick a free high UDP port (best effort; tests bind it immediately)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((BIND_HOST, 0))
        return sock.getsockname()[1]


def _recv(sock: socket.socket, timeout: float = 3.0):
    sock.settimeout(timeout)
    return sock.recvfrom(65535)


def _drain(sock: socket.socket, timeout: float = 0.4) -> list[bytes]:
    sock.settimeout(timeout)
    out = []
    while True:
        try:
            out.append(sock.recvfrom(65535)[0])
        except socket.timeout:
            return out


class EchoPeer:
    """Local UDP echo endpoint that records the source address of requests."""

    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((BIND_HOST, 0))
        self.port = self.sock.getsockname()[1]
        self.sources: list[tuple[str, int]] = []
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        self.sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                data, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            self.sources.append(addr)
            try:
                self.sock.sendto(b"echo:" + data, addr)
            except OSError:
                return

    def close(self) -> None:
        self._stop.set()
        self.sock.close()
        self.thread.join(timeout=1.0)


class ForwardingApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.public_port = _free_udp_port()
        self.settings = Settings(
            public_ipv4=BIND_HOST,
            pool_start=self.public_port,
            pool_end=self.public_port,
            lease_ttl_seconds=30.0,
            isolation_ttl_seconds=5.0,
            udp_forwarding_enabled=True,
            udp_bind_host=BIND_HOST,
            sweep_interval_seconds=0.05,
        )
        self.forwarder = UdpForwarder(
            bind_host=BIND_HOST,
            lookup=lambda port, ip, peer_port: self.allocator.lookup(
                port, ip, peer_port
            ),
        )
        self.allocator = PortAllocator(
            self.public_port,
            self.public_port,
            30.0,
            5.0,
            clock=self.clock,
            wall_clock=self.clock,
            forwarder=self.forwarder,
        )
        self.app = create_app(self.settings, self.allocator)
        self.client = TestClient(self.app)
        self.client.__enter__()
        self.probe_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.probe_sock.bind((BIND_HOST, 0))
        self.probe = {
            "probe_ipv4": BIND_HOST,
            "probe_port": self.probe_sock.getsockname()[1],
        }
        self.peer1 = EchoPeer()
        self.peer2 = EchoPeer()

    def tearDown(self) -> None:
        self.client.__exit__(None, None, None)
        self.probe_sock.close()
        self.peer1.close()
        self.peer2.close()

    def _allocate(self, probe=None, peer_port=None):
        body = {
            **(probe or self.probe),
            "peer_ipv4": BIND_HOST,
            "peer_port": peer_port or self.peer1.port,
        }
        return self.client.post("/leases", json=body)

    def _send(self, lease_id: str, payload: bytes):
        return self.client.post(
            "/send",
            json={
                "lease_id": lease_id,
                "payload_b64": base64.b64encode(payload).decode(),
            },
        )

    # ------------------------------------------------------------- single pair

    def test_send_uses_public_port_and_reply_reaches_probe(self) -> None:
        lease = self._allocate().json()
        self.assertTrue(self.forwarder.is_bound(self.public_port))
        self.assertEqual(lease["public_port"], self.public_port)

        resp = self._send(lease["lease_id"], b"ping-1")
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["bytes_sent"], len(b"ping-1"))

        # Peer saw the probe's payload coming FROM the public port.
        deadline = time.monotonic() + 3
        while not self.peer1.sources and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(self.peer1.sources[0], (BIND_HOST, self.public_port))

        # Reply is mapped by (public port, source IP, source port) and arrives
        # at the probe unchanged (the echo peer prefixes "echo:").
        data, addr = _recv(self.probe_sock)
        self.assertEqual(data, b"echo:ping-1")
        self.assertEqual(addr, (BIND_HOST, self.public_port))

    def test_unknown_lease_or_disabled_forwarding(self) -> None:
        lease = self._allocate().json()
        bad = self._send("lease-does-not-exist", b"x")
        self.assertEqual(bad.status_code, 404)

        self.client.__exit__(None, None, None)
        disabled_settings = Settings(
            public_ipv4=BIND_HOST,
            pool_start=self.public_port,
            pool_end=self.public_port,
            lease_ttl_seconds=30.0,
            isolation_ttl_seconds=5.0,
            udp_forwarding_enabled=False,
        )
        disabled_alloc = PortAllocator(
            self.public_port, self.public_port, 30.0, 5.0,
            clock=self.clock, wall_clock=self.clock,
        )
        self.client = TestClient(create_app(disabled_settings, disabled_alloc))
        self.client.__enter__()
        l2 = self._allocate().json()
        resp = self._send(l2["lease_id"], b"x")
        self.assertEqual(resp.status_code, 409)
        self.assertIn("disabled", resp.json()["detail"])
        self.assertEqual(lease["public_port"], l2["public_port"])

    def test_bad_base64_and_oversized_payload(self) -> None:
        lease = self._allocate().json()
        resp = self.client.post(
            "/send", json={"lease_id": lease["lease_id"], "payload_b64": "!!!"}
        )
        self.assertEqual(resp.status_code, 422)
        oversized = base64.b64encode(b"a" * 65508).decode()
        resp = self.client.post(
            "/send", json={"lease_id": lease["lease_id"], "payload_b64": oversized}
        )
        self.assertEqual(resp.status_code, 422)

    def test_unmapped_source_is_dropped(self) -> None:
        lease = self._allocate().json()
        self.assertTrue(lease)
        stranger = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        stranger.bind((BIND_HOST, 0))
        try:
            stranger.sendto(b"spoofed", (BIND_HOST, self.public_port))
            data = _drain(self.probe_sock, timeout=0.5)
        finally:
            stranger.close()
        self.assertEqual(data, [], "unmapped packet must not reach the probe")

    # ------------------------------------------------------- two peers, one port

    def test_two_peers_share_port_independent_lifecycle(self) -> None:
        l1 = self._allocate(peer_port=self.peer1.port).json()
        l2 = self._allocate(peer_port=self.peer2.port).json()
        self.assertEqual(l1["public_port"], l2["public_port"])
        self.assertNotEqual(l1["lease_id"], l2["lease_id"])

        # Both directions work while both leases are valid.
        self.assertEqual(self._send(l1["lease_id"], b"one").status_code, 200)
        self.assertEqual(self._send(l2["lease_id"], b"two").status_code, 200)
        self.assertEqual(_recv(self.probe_sock)[0], b"echo:one")
        self.assertEqual(_recv(self.probe_sock)[0], b"echo:two")

        # Returning peer 1 must not disturb peer 2 on the same public port.
        rel = self.client.post(f"/leases/{l1['lease_id']}/release")
        self.assertEqual(rel.status_code, 200)
        self.assertTrue(self.forwarder.is_bound(self.public_port))

        self.assertEqual(self._send(l2["lease_id"], b"three").status_code, 200)
        self.assertEqual(_recv(self.probe_sock)[0], b"echo:three")

        # Peer 1's old lease can no longer send, and its late reply is dropped.
        self.assertEqual(self._send(l1["lease_id"], b"late").status_code, 404)
        self.peer1.sock.sendto(b"late-reply", (BIND_HOST, self.public_port))
        self.assertEqual(_drain(self.probe_sock, timeout=0.5), [])

        # Peer 2 still reaps its replies; its lease can also be renewed.
        self.peer2.sock.sendto(b"still-here", (BIND_HOST, self.public_port))
        self.assertEqual(_recv(self.probe_sock)[0], b"still-here")
        self.assertEqual(
            self.client.post(f"/leases/{l2['lease_id']}/renew").status_code, 200
        )

    def test_last_lease_ends_socket_closed_without_new_requests(self) -> None:
        l1 = self._allocate(peer_port=self.peer1.port).json()
        l2 = self._allocate(peer_port=self.peer2.port).json()
        self.assertTrue(self.forwarder.is_bound(self.public_port))

        self.client.post(f"/leases/{l1['lease_id']}/release")
        self.assertTrue(self.forwarder.is_bound(self.public_port))

        self.client.post(f"/leases/{l2['lease_id']}/release")
        self.assertFalse(
            self.forwarder.is_bound(self.public_port),
            "last lease ended -> socket released immediately",
        )
        # The OS-level descriptor must be gone too, not just our bookkeeping.
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.bind((BIND_HOST, self.public_port))
        finally:
            probe.close()

    def test_reply_arriving_during_own_expiry_is_not_forwarded(self) -> None:
        """A reply that races its lease's expiry must be dropped, not sent.

        Deterministic version: the lookup callback itself runs the expiry
        sweep, so the receive path closes the socket *from inside the selector
        thread* while handling that very datagram. The code must notice and
        not forward to the probe, and the forwarder must stay healthy.
        """
        clock = FakeClock()
        port = _free_udp_port()
        holder: dict[str, PortAllocator] = {}

        def lookup(public_port, peer_ip, peer_port):
            # Expire the lease first, then resolve the (now dead) mapping.
            holder["alloc"].sweep_expired()
            return holder["alloc"].lookup(public_port, peer_ip, peer_port)

        forwarder = UdpForwarder(bind_host=BIND_HOST, lookup=lookup)
        alloc = PortAllocator(
            port, port, 30.0, 5.0, clock=clock, wall_clock=clock, forwarder=forwarder
        )
        holder["alloc"] = alloc
        settings = Settings(
            public_ipv4=BIND_HOST,
            pool_start=port,
            pool_end=port,
            lease_ttl_seconds=30.0,
            isolation_ttl_seconds=5.0,
            udp_forwarding_enabled=True,
            udp_bind_host=BIND_HOST,
        )
        probe_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe_sock.bind((BIND_HOST, 0))
        probe_port = probe_sock.getsockname()[1]
        try:
            with TestClient(create_app(settings, alloc)) as client:
                lease = client.post(
                    "/leases",
                    json={
                        "probe_ipv4": BIND_HOST, "probe_port": probe_port,
                        "peer_ipv4": BIND_HOST, "peer_port": self.peer1.port,
                    },
                ).json()
                self.assertTrue(forwarder.is_bound(port))
                clock.advance(30.0)
                # Reply arrives; lookup expires the lease and releases the port
                # from the selector thread itself.
                self.peer1.sock.sendto(b"expiry-race", (BIND_HOST, port))
                time.sleep(0.4)
                self.assertEqual(_drain(probe_sock, timeout=0.3), [])
                self.assertFalse(forwarder.is_bound(port))

                # Forwarder still healthy: a fresh lease after isolation works.
                clock.advance(5.0)
                again = client.post(
                    "/leases",
                    json={
                        "probe_ipv4": BIND_HOST, "probe_port": probe_port,
                        "peer_ipv4": BIND_HOST, "peer_port": self.peer1.port,
                    },
                ).json()
                self.assertEqual(again["lease_id"], again["lease_id"])
                self.assertNotEqual(again["lease_id"], lease["lease_id"])
                resp = client.post(
                    "/send",
                    json={
                        "lease_id": again["lease_id"],
                        "payload_b64": base64.b64encode(b"after").decode(),
                    },
                )
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(_recv(probe_sock)[0], b"echo:after")
        finally:
            forwarder.close_all()
            probe_sock.close()

    def test_expiry_closes_last_socket_via_sweep(self) -> None:
        lease = self._allocate().json()
        self.assertTrue(self.forwarder.is_bound(self.public_port))
        self.clock.advance(30.0)
        # Business state is still lazy, but the sweep must close the socket
        # even though nothing else calls the API.
        self.allocator.sweep_expired()
        self.assertFalse(self.forwarder.is_bound(self.public_port))
        # The lease is gone; its pair is quarantined (no mapping, no resend).
        self.assertEqual(self._send(lease["lease_id"], b"x").status_code, 404)
        self.assertEqual(self._allocate().status_code, 503)
        self.clock.advance(5.0)
        self.assertEqual(self._allocate().status_code, 201)

    def test_reallocate_after_release_binds_again(self) -> None:
        lease = self._allocate().json()
        self.client.post(f"/leases/{lease['lease_id']}/release")
        self.assertFalse(self.forwarder.is_bound(self.public_port))
        self.clock.advance(5.0)
        again = self._allocate().json()
        self.assertTrue(self.forwarder.is_bound(self.public_port))
        self.assertEqual(self._send(again["lease_id"], b"again").status_code, 200)
        self.assertEqual(_recv(self.probe_sock)[0], b"echo:again")


class BindFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.blocker = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.blocker.bind((BIND_HOST, 0))
        self.public_port = self.blocker.getsockname()[1]
        self.settings = Settings(
            public_ipv4=BIND_HOST,
            pool_start=self.public_port,
            pool_end=self.public_port,
            lease_ttl_seconds=30.0,
            isolation_ttl_seconds=5.0,
            udp_forwarding_enabled=True,
            udp_bind_host=BIND_HOST,
        )

    def tearDown(self) -> None:
        self.blocker.close()

    def test_port_in_use_rejects_lease_and_leaves_no_state(self) -> None:
        app = create_app(self.settings)
        client = TestClient(app)  # no lifespan: forwarder stays open for retry
        try:
            resp = client.post(
                "/leases",
                json={
                    "probe_ipv4": BIND_HOST, "probe_port": 5099,
                    "peer_ipv4": BIND_HOST, "peer_port": 7099,
                },
            )
            self.assertEqual(resp.status_code, 503, resp.text)
            self.assertIn("no lease issued", resp.json()["detail"])
            self.assertIn("cannot bind", resp.json()["detail"])

            # No lease, no pool slot consumption and no quarantine record.
            state = client.get("/debug/state").json()
            peer = state["peers"][f"{BIND_HOST}:7099"]
            self.assertEqual(peer["active_leases"], [])
            self.assertEqual(peer["held_ports"], [])
            self.assertEqual(peer["quarantine"], {})
            self.assertEqual(peer["free_intervals"], [[self.public_port, self.public_port]])
            self.assertEqual(state["udp_forwarding"]["bound_ports"], {})

            # Once the port is free, the very same pool slot is usable.
            self.blocker.close()
            ok = client.post(
                "/leases",
                json={
                    "probe_ipv4": BIND_HOST, "probe_port": 5099,
                    "peer_ipv4": BIND_HOST, "peer_port": 7099,
                },
            )
            self.assertEqual(ok.status_code, 201, ok.text)
            self.assertEqual(ok.json()["public_port"], self.public_port)
            self.assertEqual(client.get("/debug/state").json()["udp_forwarding"]
                             ["bound_ports"], {str(self.public_port): {"active_leases": 1}})
        finally:
            app.state.forwarder.close_all()

    def test_pool_slot_unbindable_fails_cleanly_then_recovers(self) -> None:
        """A busy port inside the pool must fail the request, not leak state.

        The allocator serves the highest free port first, so the blocked port
        is made the higher of the two. Once it is released, the same request
        succeeds on that exact port.
        """
        first = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        first.bind((BIND_HOST, 0))
        second = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        second.bind((BIND_HOST, 0))
        port_a = first.getsockname()[1]
        port_b = second.getsockname()[1]
        # Higher port stays blocked; lower port becomes free.
        if port_a > port_b:
            blocked, blocked_port, free_port = first, port_a, port_b
            second.close()
        else:
            blocked, blocked_port, free_port = second, port_b, port_a
            first.close()
        self.assertGreater(blocked_port, free_port)

        settings = Settings(
            public_ipv4=BIND_HOST,
            pool_start=free_port,
            pool_end=blocked_port,
            lease_ttl_seconds=30.0,
            isolation_ttl_seconds=5.0,
            udp_forwarding_enabled=True,
            udp_bind_host=BIND_HOST,
        )
        app = create_app(settings)
        client = TestClient(app)
        body = {
            "probe_ipv4": BIND_HOST, "probe_port": 5100,
            "peer_ipv4": BIND_HOST, "peer_port": 7100,
        }
        try:
            resp = client.post("/leases", json=body)
            self.assertEqual(resp.status_code, 503, resp.text)
            state = client.get("/debug/state").json()
            peer = state["peers"][f"{BIND_HOST}:7100"]
            self.assertEqual(peer["active_leases"], [])
            self.assertEqual(peer["held_ports"], [])
            self.assertEqual(peer["quarantine"], {})
            # The rolled-back port is back in the free set; nothing is bound.
            self.assertEqual(
                peer["free_intervals"], [[free_port, blocked_port]]
            )
            self.assertEqual(state["udp_forwarding"]["bound_ports"], {})

            blocked.close()
            ok = client.post("/leases", json=body)
            self.assertEqual(ok.status_code, 201, ok.text)
            self.assertEqual(ok.json()["public_port"], blocked_port)
        finally:
            app.state.forwarder.close_all()
            blocked.close()


class StubForwarder:
    """Duck-typed forwarder whose bind always fails, for unit isolation."""

    def prepare_port(self, public_port: int) -> None:
        raise UdpBindError(
            BIND_HOST, public_port, OSError(98, "Address already in use")
        )

    def add_lease(self, public_port: int, lease_id: str) -> None:  # pragma: no cover
        raise AssertionError("add_lease must not be reached after bind failure")

    def remove_lease(self, public_port: int, lease_id: str) -> None:
        pass

    def release_port_if_unused(self, public_port: int) -> None:
        pass

    def snapshot(self) -> dict:
        return {"enabled": True, "bound_ports": {}, "counters": {}}


class RollbackUnitTests(unittest.TestCase):
    def test_allocator_rolls_back_on_bind_failure(self) -> None:
        clock = FakeClock()
        alloc = PortAllocator(
            40000, 40000, 30.0, 5.0,
            clock=clock, wall_clock=clock, forwarder=StubForwarder(),
        )
        with self.assertRaises(PublicPortUnavailable):
            alloc.allocate(
                probe_ipv4="10.0.0.1", probe_port=5000,
                peer_ipv4="10.0.0.2", peer_port=7000,
            )
        state = alloc._peers[("10.0.0.2", 7000)]  # noqa: SLF001
        self.assertTrue(state.free.intervals() == [(40000, 40000)])
        self.assertEqual(state.leases, {})
        self.assertEqual(state.held, set())
        self.assertEqual(state.quarantine_until, {})
        self.assertEqual(alloc.snapshot()["port_users"], {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
