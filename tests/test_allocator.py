"""Unit tests for PortAllocator using a manually advanced fake clock."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.allocator import (  # noqa: E402
    FreeIntervalSet,
    LeaseNotFound,
    PoolExhausted,
    PortAllocator,
)

PROBE_A = ("10.0.0.1", 5000)
PROBE_B = ("10.0.0.2", 5001)
PROBE_C = ("10.0.0.3", 5002)
PEER_1 = ("198.51.100.1", 7000)
PEER_2 = ("198.51.100.2", 7001)


class FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class FreeIntervalSetTests(unittest.TestCase):
    def test_full_pool_is_one_interval(self) -> None:
        fis = FreeIntervalSet()
        fis.add(40000, 40005)
        self.assertEqual(fis.intervals(), [(40000, 40005)])
        ports = [fis.pop_any() for _ in range(6)]
        self.assertEqual(ports, [40005, 40004, 40003, 40002, 40001, 40000])
        self.assertTrue(fis.is_empty())
        with self.assertRaises(IndexError):
            fis.pop_any()

    def test_split_and_merge_back(self) -> None:
        fis = FreeIntervalSet()
        fis.add(10, 14)
        self.assertEqual(fis.pop_any(), 14)
        self.assertEqual(fis.intervals(), [(10, 13)])
        self.assertEqual(fis.pop_any(), 13)
        self.assertEqual(fis.intervals(), [(10, 12)])
        # Return 13 and 14 separately; touching intervals must merge, and the
        # merged block must fuse with the neighbouring [10, 12] interval.
        fis.add(13, 13)
        fis.add(14, 14)
        self.assertEqual(fis.intervals(), [(10, 14)])

    def test_fragmentation_stays_proportional_to_usage(self) -> None:
        fis = FreeIntervalSet()
        fis.add(1, 10_000)  # a huge pool is still one entry
        self.assertEqual(len(fis.intervals()), 1)
        fis.pop_any()
        self.assertEqual(fis.intervals(), [(1, 9999)])  # still one entry


class AllocatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.alloc = PortAllocator(
            pool_start=40000,
            pool_end=40001,
            lease_ttl_seconds=30.0,
            isolation_ttl_seconds=5.0,
            clock=self.clock,
            wall_clock=self.clock,
        )

    def _alloc(self, probe=PROBE_A, peer=PEER_1):
        return self.alloc.allocate(
            probe_ipv4=probe[0],
            probe_port=probe[1],
            peer_ipv4=peer[0],
            peer_port=peer[1],
        )

    def test_repeat_allocation_returns_same_lease_without_reset(self) -> None:
        lease, reused = self._alloc()
        self.assertFalse(reused)
        self.clock.advance(10)
        again, reused = self._alloc()
        self.assertTrue(reused)
        self.assertEqual(again.lease_id, lease.lease_id)
        # Allocate must not reset the original 30 s window.
        self.assertAlmostEqual(again.expires_at, lease.created_at + 30.0)

    def test_renew_resets_window_and_old_heap_entry_is_ignored(self) -> None:
        lease, _ = self._alloc()  # t=1000, deadline t+30=1030
        self.clock.advance(20)   # t=1020
        renewed = self.alloc.renew(lease.lease_id)
        self.assertAlmostEqual(renewed.expires_at, self.clock.t + 30.0)  # 1050
        # t=1035: the old heap entry (deadline 1030) is processed here and
        # must be ignored because the lease was renewed to 1050.
        self.clock.advance(15)
        still = self.alloc.renew(lease.lease_id)
        self.assertEqual(still.lease_id, lease.lease_id)
        # That second renew pushed the deadline to 1065. Expire at 1066.
        self.clock.advance(31)
        with self.assertRaises(LeaseNotFound):
            self.alloc.renew(lease.lease_id)

    def test_same_port_shared_between_peers_then_per_peer_exhaustion(self) -> None:
        l1, _ = self._alloc(peer=PEER_1)
        l2, _ = self._alloc(peer=PEER_2)
        self.assertEqual(l1.public_port, l2.public_port, "peers share the public port")

        # Second lease for peer 1 comes from a different probe (same probe +
        # peer would idempotently return the existing lease).
        l3, _ = self._alloc(probe=PROBE_B, peer=PEER_1)
        l4, _ = self._alloc(probe=PROBE_B, peer=PEER_2)
        self.assertNotEqual(l3.public_port, l1.public_port)
        self.assertEqual(l4.public_port, l3.public_port, "second port also shared")

        with self.assertRaises(PoolExhausted):
            self._alloc(probe=PROBE_C, peer=PEER_1)
        # Peer 2 is full too, but the error is per peer.
        with self.assertRaises(PoolExhausted):
            self._alloc(probe=PROBE_C, peer=PEER_2)

    def test_full_peer_rejects_new_probe_but_keeps_idempotent_repeat(self) -> None:
        # Single-port pool (like the compose demo): one lease fills the peer.
        alloc = PortAllocator(40000, 40000, 30.0, 5.0,
                              clock=self.clock, wall_clock=self.clock)
        lease, _ = alloc.allocate(
            probe_ipv4=PROBE_A[0], probe_port=PROBE_A[1],
            peer_ipv4=PEER_1[0], peer_port=PEER_1[1],
        )
        with self.assertRaises(PoolExhausted):
            alloc.allocate(
                probe_ipv4=PROBE_C[0], probe_port=PROBE_C[1],
                peer_ipv4=PEER_1[0], peer_port=PEER_1[1],
            )
        # ... yet the original (probe, peer) pair still gets its own lease.
        same, reused = alloc.allocate(
            probe_ipv4=PROBE_A[0], probe_port=PROBE_A[1],
            peer_ipv4=PEER_1[0], peer_port=PEER_1[1],
        )
        self.assertTrue(reused)
        self.assertEqual(same.lease_id, lease.lease_id)

    def test_release_quarantines_only_this_pair(self) -> None:
        # Peer 1 fills the whole pool (two probes); peer 2 shares port 40001.
        l1, _ = self._alloc(probe=PROBE_A, peer=PEER_1)
        l_extra, _ = self._alloc(probe=PROBE_B, peer=PEER_1)
        l2, _ = self._alloc(peer=PEER_2)  # same port as l1, other peer
        port = l1.public_port
        self.assertEqual(l2.public_port, port)

        self.alloc.release(l1.lease_id)
        # Peer 2's lease on the same port is untouched.
        self.assertIsNotNone(
            self.alloc.lookup(port, *PEER_2),
            "quarantine of one pair must not affect another peer",
        )
        # The released pair maps to nothing during isolation.
        self.assertIsNone(self.alloc.lookup(port, *PEER_1))
        with self.assertRaises(PoolExhausted):
            # one port still held, one quarantined -> nothing available
            self._alloc(probe=PROBE_A, peer=PEER_1)

        self.clock.advance(5.0)
        # Quarantine over: the same peer gets the port back with a new id.
        l1_new, reused = self._alloc(peer=PEER_1)
        self.assertFalse(reused)
        self.assertEqual(l1_new.public_port, port)
        self.assertNotEqual(l1_new.lease_id, l1.lease_id)

    def test_expiry_flow_includes_quarantine_then_reuse(self) -> None:
        lease, _ = self._alloc()
        port = lease.public_port
        self.clock.advance(30.0)
        # First touch after expiry processes it; lookup sees quarantine.
        self.assertIsNone(self.alloc.lookup(port, *PEER_1))
        with self.assertRaises(LeaseNotFound):
            self.alloc.release(lease.lease_id)
        self.clock.advance(5.0)
        fresh, reused = self._alloc()
        self.assertFalse(reused)
        self.assertEqual(fresh.public_port, port)
        self.assertNotEqual(fresh.lease_id, lease.lease_id)

    def test_old_lease_id_cannot_affect_new_lease(self) -> None:
        old, _ = self._alloc()
        self.alloc.release(old.lease_id)
        self.clock.advance(5.0)
        new, _ = self._alloc()
        self.assertEqual(new.public_port, old.public_port)
        with self.assertRaises(LeaseNotFound):
            self.alloc.renew(old.lease_id)
        with self.assertRaises(LeaseNotFound):
            self.alloc.release(old.lease_id)
        # New lease still fully functional.
        renewed = self.alloc.renew(new.lease_id)
        self.assertEqual(renewed.lease_id, new.lease_id)

    def test_lookup_returns_probe_destination(self) -> None:
        lease, _ = self._alloc(probe=PROBE_A, peer=PEER_1)
        found = self.alloc.lookup(lease.public_port, *PEER_1)
        self.assertIsNotNone(found)
        self.assertEqual((found.probe_ipv4, found.probe_port), PROBE_A)
        self.assertIsNone(self.alloc.lookup(lease.public_port, *PEER_2))

    def test_allocation_is_operationally_o_log_not_port_scan(self) -> None:
        # A 20k-port pool with two used ports keeps two free intervals;
        # allocation never iterates the pool. This is a structural guard:
        # FreeIntervalSet never holds per-port entries.
        clock = FakeClock()
        alloc = PortAllocator(40000, 59999, 30.0, 5.0, clock=clock, wall_clock=clock)
        lease, _ = alloc.allocate(
            probe_ipv4=PROBE_A[0], probe_port=PROBE_A[1],
            peer_ipv4=PEER_1[0], peer_port=PEER_1[1],
        )
        state = alloc._peers[PEER_1]  # noqa: SLF001 - white-box structural check
        self.assertEqual(len(state.free.intervals()), 1)
        self.assertNotIn(lease.public_port, state.free.intervals()[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
