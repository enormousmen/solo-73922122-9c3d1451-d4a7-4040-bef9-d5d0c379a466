#!/usr/bin/env python3
"""End-to-end demo for the gateway port allocation API.

Runs against a live server (``docker compose up`` then this script) and walks
through the required scenario on a single-port pool:

  1. Peer 1 and peer 2 each get the SAME public port.
  2. A repeat request for peer 1 returns the original lease id.
  3. The reply lookup resolves to the probe's address/port.
  4. After release, the (port, peer 1) pair is quarantined: lookups for late
     replies report no mapping, while peer 2's mapping on the same port keeps
     working, and re-allocation for peer 1 is refused.
  5. After the 5 s isolation window, peer 1 gets a fresh lease (new id) and
     lookups resolve again.

Only the Python standard library is used.

Usage:
    python3 scripts/demo.py [--base-url http://127.0.0.1:8080]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

PROBE = {"probe_ipv4": "10.10.0.5", "probe_port": 5000}
PEER_1 = {"peer_ipv4": "198.51.100.11", "peer_port": 7000}
PEER_2 = {"peer_ipv4": "198.51.100.12", "peer_port": 7000}


def _request(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def step(num: int, title: str) -> None:
    print(f"\n[{num}] {title}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    status, info = _request("GET", f"{base}/")
    print("service info:", json.dumps(info, ensure_ascii=False))
    pool = info["pool"]
    isolation = info["isolation_ttl_seconds"]
    if pool["start"] != pool["end"]:
        print(
            f"NOTE: pool is {pool['start']}..{pool['end']}; this demo is written "
            "for a single-port pool (see docker-compose.yml)."
        )

    # 1. First peer takes the public port.
    step(1, "peer 1 allocates a public port")
    s, l1 = _request("POST", f"{base}/leases", {**PROBE, **PEER_1})
    print(s, json.dumps(l1, ensure_ascii=False))
    assert s == 201 and l1["reused"] is False
    port = l1["public_port"]

    # 2. Idempotent repeat returns the same lease id.
    step(2, "peer 1 repeats the same request -> original lease, reused=true")
    s, l1b = _request("POST", f"{base}/leases", {**PROBE, **PEER_1})
    print(s, json.dumps(l1b, ensure_ascii=False))
    assert s == 201 and l1b["lease_id"] == l1["lease_id"] and l1b["reused"] is True

    # 3. Second peer gets the same public port (sharing).
    step(3, "peer 2 allocates -> same public port, different lease id")
    s, l2 = _request("POST", f"{base}/leases", {**PROBE, **PEER_2})
    print(s, json.dumps(l2, ensure_ascii=False))
    assert s == 201 and l2["public_port"] == port and l2["lease_id"] != l1["lease_id"]

    # 4. Full pool per peer: another probe for peer 1 is refused.
    step(4, "another probe for the full peer 1 -> 503 pool exhausted")
    s, err = _request(
        "POST", f"{base}/leases",
        {"probe_ipv4": "10.10.0.9", "probe_port": 5009, **PEER_1},
    )
    print(s, json.dumps(err, ensure_ascii=False))
    assert s == 503

    # 5. Lookup maps the public port + peer back to the probe.
    step(5, "lookup reply destination while mapped")
    qs = f"public_port={port}&peer_ipv4={PEER_1['peer_ipv4']}&peer_port={PEER_1['peer_port']}"
    s, mapping = _request("GET", f"{base}/mappings?{qs}")
    print(s, json.dumps(mapping, ensure_ascii=False))
    assert s == 200 and mapping["mapped"] is True
    assert (mapping["probe_ipv4"], mapping["probe_port"]) == (
        PROBE["probe_ipv4"], PROBE["probe_port"]
    )

    # 6. Release peer 1's lease -> quarantine only this (port, peer 1) pair.
    step(6, f"release peer 1 lease, then wait out the {isolation}s isolation window")
    s, rel = _request("POST", f"{base}/leases/{l1['lease_id']}/release")
    print("release:", s, json.dumps(rel, ensure_ascii=False))
    assert s == 200

    s, missing = _request("GET", f"{base}/mappings?{qs}")
    print("lookup during isolation:", s, json.dumps(missing, ensure_ascii=False))
    assert s == 404 and missing["mapped"] is False

    qs2 = f"public_port={port}&peer_ipv4={PEER_2['peer_ipv4']}&peer_port={PEER_2['peer_port']}"
    s, other = _request("GET", f"{base}/mappings?{qs2}")
    print("peer 2 lookup on same port:", s, json.dumps(other, ensure_ascii=False))
    assert s == 200 and other["mapped"] is True

    s, refused = _request("POST", f"{base}/leases", {**PROBE, **PEER_1})
    print("re-allocate during isolation:", s, json.dumps(refused, ensure_ascii=False))
    assert s == 503

    s, old = _request("POST", f"{base}/leases/{l1['lease_id']}/renew")
    print("renew with the old (released) id:", s, json.dumps(old, ensure_ascii=False))
    assert s == 404

    print(f"-- sleeping {isolation}s until quarantine ends ...")
    time.sleep(isolation + 0.5)

    # 7. Fresh lease after isolation; new id, lookup works again.
    step(7, "isolation over: peer 1 re-allocates and gets a new lease id")
    s, l1_new = _request("POST", f"{base}/leases", {**PROBE, **PEER_1})
    print(s, json.dumps(l1_new, ensure_ascii=False))
    assert s == 201 and l1_new["public_port"] == port
    assert l1_new["lease_id"] != l1["lease_id"]

    s, back = _request("GET", f"{base}/mappings?{qs}")
    print("lookup after re-allocation:", s, json.dumps(back, ensure_ascii=False))
    assert s == 200 and back["mapped"] is True

    print("\nALL DEMO CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
