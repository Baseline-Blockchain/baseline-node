# Networking & Peer Discovery

Baseline peers speak a Bitcoin-like protocol over TCP. The node’s P2P server handles inbound/outbound peers, address relay, and header/block sync.

## Ports & Bind Addresses

- **P2P**: TCP `9333` (default). Controlled by `network.host`/`network.port`.
- **RPC**: TCP `8832`. Exposed locally unless you bind to `0.0.0.0`.
- **Stratum**: TCP `3333`.

Bind addresses accept IPv4/IPv6. If you bind to `0.0.0.0`, keep a firewall in front of RPC.

## Bootstrapping Peers

The node keeps a persistent address book at `data_dir/peers/known_peers.json`. On first boot it uses two sources:

1. **Manual seeds** (`network.seeds`): list of `"host:port"` strings. Specify at least one reachable peer for predictable startup.
2. **DNS seeds**: read from `config.network.dns_seeds` (populated by env overrides or future releases). The resolver ignores localhost, RFC1918, link-local, or reserved prefixes, so lab clusters must rely on manual seeds.

You can hot-add peers by editing `config.json` and restarting or by exposing new `addnode`-style RPC endpoints (planned). For now, keep static lists for test networks.

## Connection Limits

`max_peers`, `min_peers`, and `target_outbound` shape the peer graph:

- `min_peers`: inbound peers kept before the dialer starts searching for more.
- `target_outbound`: number of outbound connections the dialer maintains via `PeerDiscovery`.
- `max_peers`: hard cap across inbound + outbound.

Peers that idle for `idle_timeout` seconds or fail to respond to pings are dropped. The sync watchdog restarts header/block sync whenever progress stalls for more than 30/60 seconds.

## Address Sharing

`addr`/`addrv2` equivalents advertise up to 32 entries per message. Baseline stores the union in `known_addresses`; you can pre-seed that file to accelerate bootstrap on air-gapped networks.

```
known_peers.json
[
  {"host": "seed1.baseline.org", "port": 9333, "last_seen": 0},
  {"host": "203.0.113.15", "port": 9333, "last_seen": 1692124800}
]
```

## Local-Only Clusters

Because the DNS seeder filters private ranges, you must explicitly list `127.0.0.1:<port>` or `192.168.x.x:<port>` entries in `network.seeds` (or copy the remote node’s `known_peers.json`) when running integration tests on a single host.

## Banning & Rate Limits

`baseline/net/security.py` tracks failed handshakes, DoS attempts, and per-IP connection counts. Offending hosts are throttled or banned until the cleanup loop removes stale entries (default every 5 minutes). Keep logs on to diagnose automatic disconnects.
