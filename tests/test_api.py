"""End-to-end API tests on an in-process ASGI app with a fake clock."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient  # noqa: E402

from app.allocator import PortAllocator  # noqa: E402
from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402


class FakeClock:
    def __init__(self) -> None:
        self.t = 5000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.settings = Settings(
            public_ipv4="203.0.113.10",
            pool_start=40000,
            pool_end=40000,  # single-port pool, like the compose demo
            lease_ttl_seconds=30.0,
            isolation_ttl_seconds=5.0,
        )
        self.allocator = PortAllocator(
            40000, 40000, 30.0, 5.0, clock=self.clock, wall_clock=self.clock
        )
        self.app = create_app(self.settings, self.allocator)
        self.client = TestClient(self.app)
        self.probe_a = {"probe_ipv4": "10.0.0.1", "probe_port": 5000}
        self.peer_1 = {"peer_ipv4": "198.51.100.1", "peer_port": 7000}
        self.peer_2 = {"peer_ipv4": "198.51.100.2", "peer_port": 7001}

    def _allocate(self, peer, probe=None):
        body = {**(probe or self.probe_a), **peer}
        return self.client.post("/leases", json=body)

    def test_health_and_root(self) -> None:
        self.assertEqual(self.client.get("/healthz").json(), {"detail": "ok"})
        root = self.client.get("/").json()
        self.assertEqual(root["pool"], {"start": 40000, "end": 40000})

    def test_single_port_shared_two_peers_idempotent_and_full(self) -> None:
        r1 = self._allocate(self.peer_1)
        self.assertEqual(r1.status_code, 201)
        l1 = r1.json()
        self.assertEqual(l1["public_port"], 40000)
        self.assertEqual(l1["public_ipv4"], "203.0.113.10")
        self.assertFalse(l1["reused"])

        # Idempotent repeat: same lease id.
        r1b = self._allocate(self.peer_1)
        self.assertEqual(r1b.status_code, 201)
        self.assertTrue(r1b.json()["reused"])
        self.assertEqual(r1b.json()["lease_id"], l1["lease_id"])

        # Second peer gets the same public port.
        r2 = self._allocate(self.peer_2)
        self.assertEqual(r2.status_code, 201)
        l2 = r2.json()
        self.assertEqual(l2["public_port"], 40000)
        self.assertNotEqual(l2["lease_id"], l1["lease_id"])

        # Peer 1 holds the only port; a different probe asking for another
        # port for that same peer gets 503 (per-peer pool exhaustion).
        full = self._allocate(
            self.peer_1,
            probe={"probe_ipv4": "10.0.0.9", "probe_port": 5009},
        )
        self.assertEqual(full.status_code, 503)

    def test_release_quarantine_lookup_then_reallocate(self) -> None:
        l1 = self._allocate(self.peer_1).json()
        l2 = self._allocate(self.peer_2).json()
        port = l1["public_port"]

        # Lookup resolves to the probe while mapped.
        got = self.client.get(
            "/mappings",
            params={"public_port": port, **self.peer_1},
        )
        self.assertEqual(got.status_code, 200)
        self.assertEqual(got.json()["probe_ipv4"], "10.0.0.1")
        self.assertEqual(got.json()["probe_port"], 5000)

        rel = self.client.post(f"/leases/{l1['lease_id']}/release")
        self.assertEqual(rel.status_code, 200)

        # During quarantine the pair has no mapping; late reply is unmapped.
        missing = self.client.get(
            "/mappings", params={"public_port": port, **self.peer_1}
        )
        self.assertEqual(missing.status_code, 404)
        self.assertFalse(missing.json()["mapped"])

        # Other peer's mapping on the same port survives.
        other = self.client.get(
            "/mappings", params={"public_port": port, **self.peer_2}
        )
        self.assertEqual(other.status_code, 200)
        self.assertTrue(other.json()["mapped"])

        # Allocation refused while quarantined.
        self.assertEqual(self._allocate(self.peer_1).status_code, 503)

        # Old id is dead; cannot renew it.
        self.assertEqual(
            self.client.post(f"/leases/{l1['lease_id']}/renew").status_code, 404
        )

        # After the 5 s isolation window the port can be re-leased.
        self.clock.advance(5.0)
        again = self._allocate(self.peer_1)
        self.assertEqual(again.status_code, 201)
        self.assertEqual(again.json()["public_port"], port)
        self.assertNotEqual(again.json()["lease_id"], l1["lease_id"])

        back = self.client.get(
            "/mappings", params={"public_port": port, **self.peer_1}
        )
        self.assertEqual(back.status_code, 200)
        self.assertTrue(back.json()["mapped"])
        # l2 untouched throughout.
        self.assertEqual(
            self.client.post(f"/leases/{l2['lease_id']}/renew").status_code, 200
        )

    def test_expiry_via_api(self) -> None:
        l1 = self._allocate(self.peer_1).json()
        self.clock.advance(30.0)
        # Any call for the peer triggers expiry -> quarantine.
        missing = self.client.get(
            "/mappings",
            params={"public_port": l1["public_port"], **self.peer_1},
        )
        self.assertEqual(missing.status_code, 404)
        self.clock.advance(5.0)
        fresh = self._allocate(self.peer_1)
        self.assertEqual(fresh.status_code, 201)
        self.assertNotEqual(fresh.json()["lease_id"], l1["lease_id"])

    def test_validation_errors(self) -> None:
        bad_ip = self.client.post(
            "/leases",
            json={**self.probe_a, "peer_ipv4": "not-an-ip", "peer_port": 7000},
        )
        self.assertEqual(bad_ip.status_code, 422)
        bad_port = self.client.post(
            "/leases",
            json={**self.probe_a, "peer_ipv4": "198.51.100.1", "peer_port": 0},
        )
        self.assertEqual(bad_port.status_code, 422)
        self.assertEqual(
            self.client.post("/leases/does-not-exist/renew").status_code, 404
        )

    def test_debug_state_exposes_structure(self) -> None:
        self._allocate(self.peer_1)
        state = self.client.get("/debug/state").json()
        self.assertIn("198.51.100.1:7000", state["peers"])
        self.assertEqual(state["port_users"]["40000"], ["198.51.100.1:7000"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
