#!/usr/bin/env python3
"""End-to-end UDP forwarding demo against a live gateway (standard library only).

Scenario (single-port pool, like docker-compose.yml):

  1. Two local echo peers, two probes (each probe has its own UDP socket).
  2. Both peers lease the SAME public port.
  3. POST /send for each lease -> the echo peer sees the source port equal to
     the public port; the echo reply arrives unchanged at the owning probe.
  4. Release peer 1's lease: peer 2 keeps sending/receiving on that port,
     peer 1 can no longer send and its late reply is dropped.
  5. Release the last lease -> the public socket is closed right away (no
     further API call needed), proven by binding the port locally.
  6. A lease left to expire is released by the gateway's own sweep, again
     without any follow-up API call; then the port is re-leasable.
  7. An externally occupied port makes POST /leases fail with 503 before any
     lease exists (checked in step 0).

Usage:
    python3 scripts/demo_udp.py [--base-url http://127.0.0.1:8080]
"""

from __future__ import annotations

import argparse
import base64
import json
import socket
import sys
import threading
import time
import urllib.error
import urllib.request

BIND = "127.0.0.1"


class EchoPeer(threading.Thread):
    """UDP echo endpoint that records where each request came from."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((BIND, 0))
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]
        self.sources: list[tuple[str, int]] = []
        self._halt = threading.Event()
        self.start()

    def run(self) -> None:
        while not self._halt.is_set():
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

    def stop(self) -> None:
        self._halt.set()
        self.sock.close()
        self.join(timeout=1)


class Probe:
    """A probe with its own UDP socket; receives forwarded replies."""

    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((BIND, 0))
        self.port = self.sock.getsockname()[1]

    def recv(self, timeout: float = 3.0) -> tuple[bytes, tuple[str, int]] | None:
        self.sock.settimeout(timeout)
        try:
            return self.sock.recvfrom(65535)
        except socket.timeout:
            return None

    def assert_silent(self, timeout: float = 1.0) -> None:
        got = self.recv(timeout)
        assert got is None, f"probe unexpectedly received {got!r}"


def request(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def send(base: str, lease_id: str, payload: bytes) -> tuple[int, dict]:
    return request(
        "POST",
        f"{base}/send",
        {"lease_id": lease_id, "payload_b64": base64.b64encode(payload).decode()},
    )


def step(num: int, title: str) -> None:
    print(f"\n[{num}] {title}")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((BIND, 0))
        return sock.getsockname()[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    _, info = request("GET", f"{base}/")
    print("service info:", json.dumps(info, ensure_ascii=False))
    if not info.get("udp_forwarding_enabled"):
        print("ERROR: gateway has UDP forwarding disabled (UDP_FORWARDING_ENABLED=true)")
        return 2
    isolation = info["isolation_ttl_seconds"]
    pool = info["pool"]
    if pool["start"] != pool["end"]:
        print(
            f"NOTE: pool is {pool['start']}..{pool['end']}; this demo expects the "
            "single-port pool from docker-compose.yml."
        )
    public_port = pool["start"]

    peer1, peer2 = EchoPeer(), EchoPeer()
    probe_a, probe_b = Probe(), Probe()
    try:
        # 0. Port-busy refusal is demonstrated by blocking the pool port with
        #    a plain socket before any lease exists. If the port is already in
        #    use by something else we cannot run that sub-check, so report it.
        step(0, f"occupy UDP {BIND}:{public_port} from outside the gateway")
        blocker = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            blocker.bind((BIND, public_port))
            s, err = request(
                "POST",
                f"{base}/leases",
                {
                    "probe_ipv4": BIND, "probe_port": probe_a.port,
                    "peer_ipv4": BIND, "peer_port": peer1.port,
                },
            )
            print("allocate while occupied:", s, json.dumps(err, ensure_ascii=False))
            assert s == 503 and "no lease issued" in err["detail"], (s, err)
            state = request("GET", f"{base}/debug/state")[1]
            peer_state = state["peers"].get(f"{BIND}:{peer1.port}", {})
            assert peer_state.get("active_leases") == [], peer_state
            assert peer_state.get("quarantine") == {}, peer_state
        finally:
            blocker.close()

        # 1. Two peers lease the same public port.
        step(1, "two peers lease the same public port")
        s, l1 = request(
            "POST", f"{base}/leases",
            {"probe_ipv4": BIND, "probe_port": probe_a.port,
             "peer_ipv4": BIND, "peer_port": peer1.port},
        )
        print("peer 1:", s, json.dumps(l1, ensure_ascii=False))
        assert s == 201 and l1["public_port"] == public_port
        s, l2 = request(
            "POST", f"{base}/leases",
            {"probe_ipv4": BIND, "probe_port": probe_b.port,
             "peer_ipv4": BIND, "peer_port": peer2.port},
        )
        print("peer 2:", s, json.dumps(l2, ensure_ascii=False))
        assert s == 201 and l2["public_port"] == public_port
        assert l2["lease_id"] != l1["lease_id"]

        # 2. Send from the shared public port; peers see the public source port.
        step(2, "POST /send for both leases, both peers reply through one port")
        for lease, probe, peer, payload in (
            (l1, probe_a, peer1, b"polling-1"),
            (l2, probe_b, peer2, b"polling-2"),
        ):
            s, res = send(base, lease["lease_id"], payload)
            print("send:", s, json.dumps(res, ensure_ascii=False))
            assert s == 200, (s, res)
            assert res["public_port"] == public_port
            got = probe.recv()
            assert got is not None, "no reply reached the probe"
            data, addr = got
            print(f"  probe got {data!r} from {addr}")
            assert data == b"echo:" + payload
            assert addr == (BIND, public_port), addr

        print("peer 1 saw source:", peer1.sources[-1])
        print("peer 2 saw source:", peer2.sources[-1])
        assert peer1.sources[-1] == (BIND, public_port)
        assert peer2.sources[-1] == (BIND, public_port)

        # 3. Unmapped traffic on that port is dropped.
        step(3, "reply from an unknown source port is dropped")
        stranger = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            stranger.bind((BIND, 0))
            stranger.sendto(b"not-yours", (BIND, public_port))
        finally:
            stranger.close()
        probe_a.assert_silent(0.7)
        probe_b.assert_silent(0.2)
        print("  dropped, neither probe received it")

        # 4. Releasing one lease keeps the shared port alive for the other.
        step(4, "release peer 1 -> peer 2 still sends and receives")
        s, rel = request("POST", f"{base}/leases/{l1['lease_id']}/release")
        assert s == 200, (s, rel)
        state = request("GET", f"{base}/debug/state")[1]
        print("bound ports after peer 1 release:",
              state["udp_forwarding"]["bound_ports"])
        assert state["udp_forwarding"]["bound_ports"][str(public_port)] == {
            "active_leases": 1
        }

        s, res = send(base, l2["lease_id"], b"still-alive")
        assert s == 200, (s, res)
        got = probe_b.recv()
        assert got is not None and got[0] == b"echo:still-alive", got
        print(f"  peer 2 round-trip ok: {got[0]!r}")

        s, old = send(base, l1["lease_id"], b"late-poll")
        print("send with released lease:", s, json.dumps(old, ensure_ascii=False))
        assert s == 404

        # Late reply from peer 1's address is unmapped now -> dropped.
        peer1.sock.sendto(b"late-reply", (BIND, public_port))
        probe_a.assert_silent(0.7)
        probe_b.assert_silent(0.2)
        print("  late reply from peer 1 dropped, peer 2 unaffected")

        # 5. Last lease ends -> socket released without any further request.
        step(5, "release peer 2 (last lease) -> public socket closed immediately")
        s, _ = request("POST", f"{base}/leases/{l2['lease_id']}/release")
        assert s == 200
        time.sleep(0.2)
        state = request("GET", f"{base}/debug/state")[1]
        print("bound ports:", state["udp_forwarding"]["bound_ports"])
        assert state["udp_forwarding"]["bound_ports"] == {}

        # Prove the OS port is actually free by binding it ourselves.
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.bind((BIND, public_port))
            print("  port is free: a fresh socket could bind it")
        finally:
            probe.close()

        # 6. Expiry alone closes the socket, with no further API call: we poll
        #    a local bind() of the port, which only succeeds once the gateway
        #    has released the OS descriptor.
        step(6, "lease expiry alone closes the socket (no follow-up request)")
        print(f"  waiting out peer 1 isolation ({isolation}s) ...")
        time.sleep(isolation + 0.3)
        s, l3 = request(
            "POST", f"{base}/leases",
            {"probe_ipv4": BIND, "probe_port": probe_a.port,
             "peer_ipv4": BIND, "peer_port": peer1.port},
        )
        assert s == 201, (s, l3)
        ttl = l3["ttl_seconds"]
        print(f"  lease {l3['lease_id']} issued, ttl={ttl}s; polling local bind()")
        started = time.monotonic()
        deadline = started + ttl + 3
        released_at: float | None = None
        while time.monotonic() < deadline:
            time.sleep(0.2)
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                probe.bind((BIND, public_port))
                released_at = time.monotonic() - started
                break
            except OSError:
                continue
            finally:
                probe.close()
        assert released_at is not None, "socket was not released after expiry"
        assert released_at >= ttl - 0.5, released_at
        print(f"  port was free {released_at:.1f}s after issue (ttl {ttl}s)")

        print("\nwaiting out expiry isolation before re-lease ...")
        time.sleep(isolation + 0.3)
        s, again = request(
            "POST", f"{base}/leases",
            {"probe_ipv4": BIND, "probe_port": probe_a.port,
             "peer_ipv4": BIND, "peer_port": peer1.port},
        )
        assert s == 201, (s, again)
        s, res = send(base, again["lease_id"], b"after-expiry")
        assert s == 200, (s, res)
        got = probe_a.recv()
        assert got is not None and got[0] == b"echo:after-expiry", got
        print("re-leased and round-trip ok:", got[0])
        request("POST", f"{base}/leases/{again['lease_id']}/release")
    finally:
        peer1.stop()
        peer2.stop()
        probe_a.sock.close()
        probe_b.sock.close()

    print("\nALL UDP FORWARDING DEMO CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
