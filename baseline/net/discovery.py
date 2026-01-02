"""
Enhanced peer discovery with DNS seeds, address book persistence, and peer exchange.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import socket
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .address import PeerAddress


class DNSSeeder:
    """DNS seed resolver for peer discovery."""

    def __init__(self, dns_seeds: list[str], timeout: float = 10.0, max_addresses_per_seed: int = 100):
        self.dns_seeds = dns_seeds
        self.timeout = timeout
        self.max_addresses_per_seed = max_addresses_per_seed
        self.log = logging.getLogger("baseline.dns_seeder")

    async def resolve_seeds(self) -> list[PeerAddress]:
        """Resolve DNS seeds to peer addresses."""
        addresses = []

        for seed in self.dns_seeds:
            try:
                resolved = await self._resolve_dns_seed(seed)
                addresses.extend(resolved)
                self.log.info("Resolved %d addresses from DNS seed %s", len(resolved), seed)
            except Exception as exc:
                self.log.warning("Failed to resolve DNS seed %s: %s", seed, exc)

        return addresses

    async def _resolve_dns_seed(self, seed: str) -> list[PeerAddress]:
        """Resolve a single DNS seed."""
        addresses = []

        try:
            # Parse seed format: hostname[:port]
            if ":" in seed:
                hostname, port_str = seed.rsplit(":", 1)
                default_port = int(port_str)
            else:
                hostname = seed
                default_port = 9333  # Default Baseline port

            # Resolve hostname to IP addresses with timeout
            loop = asyncio.get_event_loop()
            addrinfo = await asyncio.wait_for(
                loop.getaddrinfo(
                    hostname, None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
                ),
                timeout=self.timeout
            )

            for family, _type, _proto, _canonname, sockaddr in addrinfo:
                if family in (socket.AF_INET, socket.AF_INET6):
                    ip = sockaddr[0]
                    # Skip localhost and private IPs for DNS seeds
                    if not self._is_routable_ip(ip):
                        continue

                    addr = PeerAddress(
                        host=ip,
                        port=default_port,
                        last_seen=time.time(),
                        source="dns"
                    )
                    addresses.append(addr)

                    # Limit addresses per seed to prevent DNS amplification
                    if len(addresses) >= self.max_addresses_per_seed:
                        self.log.debug("Reached address limit for seed %s", seed)
                        break

        except TimeoutError:
            self.log.warning("DNS resolution timeout for seed %s", seed)
        except Exception as exc:
            self.log.debug("DNS resolution failed for %s: %s", seed, exc)

        return addresses

    def _is_routable_ip(self, ip: str) -> bool:
        """Check if IP address is routable (not localhost/private/reserved)."""
        try:
            import ipaddress
            addr = ipaddress.ip_address(ip)

            # Check for various non-routable ranges
            if addr.is_loopback:
                return False
            if addr.is_private:
                return False
            if addr.is_reserved:
                return False
            if addr.is_multicast:
                return False
            if addr.is_link_local:
                return False

            # Additional checks for IPv4
            if isinstance(addr, ipaddress.IPv4Address):
                # Check for broadcast
                if addr.is_unspecified:
                    return False
                # Check for CGNAT (100.64.0.0/10)
                if int(addr) >= int(ipaddress.IPv4Address('100.64.0.0')) and \
                   int(addr) <= int(ipaddress.IPv4Address('100.127.255.255')):
                    return False
                # Check for test networks (198.18.0.0/15)
                if int(addr) >= int(ipaddress.IPv4Address('198.18.0.0')) and \
                   int(addr) <= int(ipaddress.IPv4Address('198.19.255.255')):
                    return False

            # Additional checks for IPv6
            elif isinstance(addr, ipaddress.IPv6Address):
                # Check for unique local (fc00::/7)
                if addr.is_site_local:
                    return False
                # Check for documentation prefix (2001:db8::/32)
                if int(addr) >= int(ipaddress.IPv6Address('2001:db8::')) and \
                   int(addr) <= int(ipaddress.IPv6Address('2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')):
                    return False

            return True
        except ValueError:
            return False


class AddressBook:
    """Persistent address book for peer management."""

    def __init__(self, path: Path, max_addresses: int = 50000):
        self.path = path
        self.max_addresses = max_addresses
        self.addresses: dict[tuple[str, int], PeerAddress] = {}
        self.log = logging.getLogger("baseline.address_book")
        self._load()

    def _load(self) -> None:
        """Load addresses from disk."""
        if not self.path.exists():
            return

        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            raw_addresses = data.get("addresses", [])
            deduped: dict[str, PeerAddress] = {}

            def _is_better(new: PeerAddress, existing: PeerAddress) -> bool:
                # Prefer peers with successes, then fewer failures, then non-inbound,
                # then the default port, then most recent last_seen.
                if new.success_count != existing.success_count:
                    return new.success_count > existing.success_count
                if new.failure_count != existing.failure_count:
                    return new.failure_count < existing.failure_count
                if (new.source == "inbound") != (existing.source == "inbound"):
                    return existing.source == "inbound"
                if (new.port == 9333) != (existing.port == 9333):
                    return new.port == 9333
                return new.last_seen > existing.last_seen

            for addr_data in raw_addresses:
                addr = PeerAddress(**addr_data)
                # Drop inbound-only addresses that never succeeded (likely ephemeral ports).
                if addr.source == "inbound" and addr.success_count == 0:
                    continue
                current = deduped.get(addr.host)
                if current is None or _is_better(addr, current):
                    deduped[addr.host] = addr

            for addr in deduped.values():
                self.addresses[addr.key()] = addr

            self.log.info("Loaded %d addresses from address book", len(self.addresses))
        except Exception as exc:
            self.log.warning("Failed to load address book: %s", exc)

    def save(self) -> None:
        """Save addresses to disk."""
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)

            persisted = []
            for addr in self.addresses.values():
                # Do not persist inbound-only addresses that have never succeeded outbound.
                if addr.source == "inbound" and addr.success_count == 0:
                    continue
                persisted.append(asdict(addr))

            data = {
                "version": 1,
                "timestamp": time.time(),
                "addresses": persisted
            }

            with self.path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            self.log.debug("Saved %d addresses to address book", len(self.addresses))
        except Exception as exc:
            self.log.warning("Failed to save address book: %s", exc)

    def add_address(self, address: PeerAddress) -> bool:
        """Add or update an address. Returns True if added/updated, False if rejected."""
        key = address.key()
        existing = self.addresses.get(key)

        if existing:
            # Update existing address with newer information
            if address.last_seen > existing.last_seen:
                existing.last_seen = address.last_seen
                existing.services = address.services
            # Keep attempt/success/failure counts
            return True
        else:
            # Check size limit before adding new address
            if len(self.addresses) >= self.max_addresses:
                self._evict_stale_addresses()

                # If still at limit after eviction, reject
                if len(self.addresses) >= self.max_addresses:
                    self.log.debug("Address book full, rejecting new address %s:%d",
                                 address.host, address.port)
                    return False

            self.addresses[key] = address
            return True

    def add_addresses(self, addresses: list[PeerAddress]) -> int:
        """Add multiple addresses. Returns count of successfully added addresses."""
        added_count = 0
        for addr in addresses:
            if self.add_address(addr):
                added_count += 1
        return added_count

    def _evict_stale_addresses(self) -> None:
        """Evict stale addresses using LRU policy."""
        # Sort by last_seen (oldest first) and reliability score (worst first)
        sorted_addresses = sorted(
            self.addresses.items(),
            key=lambda x: (x[1].last_seen, x[1].reliability_score())
        )

        # Remove oldest 10% of addresses
        evict_count = max(1, len(self.addresses) // 10)
        for i in range(evict_count):
            if i < len(sorted_addresses):
                key = sorted_addresses[i][0]
                del self.addresses[key]

        if evict_count > 0:
            self.log.debug("Evicted %d stale addresses from address book", evict_count)

    def get_addresses(self, count: int, exclude: set[tuple[str, int]] | None = None) -> list[PeerAddress]:
        """Get addresses for connection attempts."""
        exclude = exclude or set()

        def _filtered(include_stale: bool) -> list[PeerAddress]:
            results = []
            for addr in self.addresses.values():
                if addr.key() in exclude:
                    continue
                if not include_stale and addr.is_stale():
                    continue
                if not addr.should_retry():
                    continue
                results.append(addr)
            return results

        # Prefer fresh addresses; if none available, fall back to stale ones so seed files are used.
        candidates = _filtered(include_stale=False)
        if not candidates:
            candidates = _filtered(include_stale=True)

        # Sort by reliability score (best first)
        candidates.sort(key=lambda a: a.reliability_score(), reverse=True)

        # Add some randomization to avoid always trying the same addresses
        if len(candidates) > count * 2:
            # Take top 50% by reliability, then randomize
            top_half = candidates[:len(candidates) // 2]
            random.shuffle(top_half)
            candidates = top_half
        else:
            random.shuffle(candidates)

        return candidates[:count]

    def record_attempt(self, host: str, port: int) -> None:
        """Record a connection attempt."""
        key = (host, port)
        if key in self.addresses:
            self.addresses[key].record_attempt()

    def record_success(self, host: str, port: int) -> None:
        """Record a successful connection."""
        key = (host, port)
        if key in self.addresses:
            self.addresses[key].record_success()

    def record_failure(self, host: str, port: int) -> None:
        """Record a failed connection."""
        key = (host, port)
        if key in self.addresses:
            self.addresses[key].record_failure()

    def cleanup_stale(self, max_age: float = 86400 * 30) -> None:
        """Remove stale addresses."""
        stale_keys = [
            key for key, addr in self.addresses.items()
            if addr.is_stale(max_age)
        ]

        for key in stale_keys:
            del self.addresses[key]

        if stale_keys:
            self.log.info("Removed %d stale addresses", len(stale_keys))

    def cleanup_old_addresses(self, max_age: float = 86400 * 30, max_failures: int = 5) -> None:
        """Remove addresses that consistently fail or have been unseen for too long."""
        import time

        now = time.time()
        removed = 0
        for key, addr in list(self.addresses.items()):
            age = now - addr.last_seen
            if age > max_age and (addr.failure_count >= max_failures or addr.success_count == 0):
                del self.addresses[key]
                removed += 1

        if removed:
            self.log.info("Recovery removed %d unreliable addresses", removed)

    def get_stats(self) -> dict[str, Any]:
        """Get address book statistics."""
        total = len(self.addresses)
        by_source = {}
        reliable_count = 0

        for addr in self.addresses.values():
            by_source[addr.source] = by_source.get(addr.source, 0) + 1
            if addr.reliability_score() > 0.7:
                reliable_count += 1

        return {
            "total_addresses": total,
            "reliable_addresses": reliable_count,
            "by_source": by_source,
        }


class PeerExchange:
    """Peer address exchange protocol."""

    def __init__(self, address_book: AddressBook):
        self.address_book = address_book
        self.log = logging.getLogger("baseline.peer_exchange")

        # Rate limiting for addr messages
        self.peer_addr_timestamps: dict[str, float] = {}
        self.min_addr_interval = 2.0  # 5 minutes between addr messages per peer
        self.max_addresses_per_message = 1000

    def create_addr_message(self, max_addresses: int = 1000) -> dict[str, Any]:
        """Create an addr message with known addresses."""
        addresses = self.address_book.get_addresses(max_addresses)

        addr_list = []
        for addr in addresses:
            # Only share addresses we've successfully connected to
            if addr.success_count > 0:
                addr_list.append(
                    {
                        "host": addr.host,
                        "port": addr.port,
                        "services": addr.services,
                        "last_seen": int(addr.last_seen),
                    }
                )

        return {
            "type": "addr",
            "peers": addr_list,
        }

    def handle_addr_message(self, message: dict[str, Any], peer_id: str) -> bool:
        """Handle incoming addr message with rate limiting."""
        import time
        current_time = time.time()

        # Check rate limiting
        last_addr_time = self.peer_addr_timestamps.get(peer_id, 0)
        if current_time - last_addr_time < self.min_addr_interval:
            self.log.warning("Rate limiting addr message from peer %s", peer_id)
            return False

        addresses = message.get("peers")
        if addresses is None:
            addresses = message.get("addresses", [])

        # Limit number of addresses per message
        if len(addresses) > self.max_addresses_per_message:
            self.log.warning("Peer %s sent too many addresses (%d), truncating to %d",
                           peer_id, len(addresses), self.max_addresses_per_message)
            addresses = addresses[:self.max_addresses_per_message]

        new_addresses = []
        for addr_data in addresses:
            try:
                last_seen = addr_data.get("last_seen")
                if last_seen is None:
                    last_seen = addr_data.get("timestamp", time.time())
                addr = PeerAddress(
                    host=addr_data["host"],
                    port=addr_data["port"],
                    last_seen=last_seen,
                    services=addr_data.get("services", 1),
                    source="peer"
                )

                # Basic validation
                if not self._is_valid_address(addr):
                    continue

                new_addresses.append(addr)
            except (KeyError, ValueError) as exc:
                self.log.debug("Invalid address in addr message: %s", exc)

        if new_addresses:
            added_count = self.address_book.add_addresses(new_addresses)
            self.peer_addr_timestamps[peer_id] = current_time
            self.log.debug("Added %d/%d addresses from peer %s",
                          added_count, len(new_addresses), peer_id)

        return True

    def _is_valid_address(self, addr: PeerAddress) -> bool:
        """Validate a peer address."""
        # Check port range
        if not (1 <= addr.port <= 65535):
            return False

        # Check if it's a routable IP
        try:
            socket.inet_pton(socket.AF_INET, addr.host)
            return True
        except OSError:
            try:
                socket.inet_pton(socket.AF_INET6, addr.host)
                return True
            except OSError:
                return False


class PeerDiscovery:
    """Main peer discovery coordinator."""

    def __init__(self, data_dir: Path, dns_seeds: list[str], manual_seeds: list[str]):
        self.address_book = AddressBook(data_dir / "peers.json")
        self.dns_seeder = DNSSeeder(dns_seeds)
        self.peer_exchange = PeerExchange(self.address_book)
        self.manual_seeds = manual_seeds
        self.log = logging.getLogger("baseline.peer_discovery")
        self._warned_no_dns = False

        # Error tracking for recovery
        self.error_count = 0
        self.last_error_time = 0.0
        self.max_errors_per_hour = 100

        # Add manual seeds to address book
        self._add_manual_seeds()

    def _add_manual_seeds(self) -> None:
        """Add manual seed addresses to the address book."""
        for seed in self.manual_seeds:
            try:
                if ":" in seed:
                    host, port_str = seed.rsplit(":", 1)
                    port = int(port_str)
                else:
                    host = seed
                    port = 9333

                addr = PeerAddress(
                    host=host,
                    port=port,
                    last_seen=time.time(),
                    source="manual"
                )
                self.address_book.add_address(addr)
            except ValueError as exc:
                self.log.warning("Invalid manual seed %s: %s", seed, exc)

    async def discover_peers(self, count: int, *, seeds_only: bool = False) -> list[tuple[str, int]]:
        """Discover peer addresses for connection."""
        try:
            picks: list[tuple[str, int]] = []
            seen: set[tuple[str, int]] = set()

            # Seed-only mode: return manual seeds and stop (no duplicates).
            if seeds_only and self.manual_seeds:
                for seed in self.manual_seeds:
                    try:
                        if ":" in seed:
                            host, port_str = seed.rsplit(":", 1)
                            port = int(port_str)
                        else:
                            host = seed
                            port = 9333
                        key = (host, port)
                        if key not in seen:
                            picks.append(key)
                            seen.add(key)
                    except ValueError:
                        continue
                random.shuffle(picks)
                return picks[:count]

            # Seed-first: try manual seeds before address book/DNS
            for seed in self.manual_seeds:
                try:
                    if ":" in seed:
                        host, port_str = seed.rsplit(":", 1)
                        port = int(port_str)
                    else:
                        host = seed
                        port = 9333
                    key = (host, port)
                    if key not in seen:
                        picks.append(key)
                        seen.add(key)
                    if len(picks) >= count:
                        return picks[:count]
                except ValueError:
                    continue

            # Next pull from the address book, excluding already chosen seeds
            remaining = max(0, count - len(picks))
            if remaining:
                existing = self.address_book.get_addresses(remaining, exclude=seen)
                picks.extend([addr.key() for addr in existing if addr.key() not in seen])
                seen.update(addr.key() for addr in existing)

            # If still short, try DNS seeds then pull again
            if len(picks) < count:
                try:
                    dns_addresses = await self.dns_seeder.resolve_seeds()
                    if dns_addresses:
                        self.address_book.add_addresses(dns_addresses)
                        self.log.info("Discovered %d addresses from DNS seeds", len(dns_addresses))
                    elif not self.manual_seeds and not self.address_book.addresses and not self._warned_no_dns:
                        self.log.info("DNS seeds returned no peers; operating without public connections")
                        self._warned_no_dns = True
                    else:
                        self.log.debug(
                            "DNS seeds returned no new addresses; reusing %d cached/manual entries",
                            len(self.address_book.addresses),
                        )
                except Exception as exc:
                    self.log.warning("DNS seed discovery failed: %s", exc)
                    self._record_error()

                remaining = max(0, count - len(picks))
                if remaining:
                    existing = self.address_book.get_addresses(remaining, exclude=seen)
                    picks.extend([addr.key() for addr in existing if addr.key() not in seen])

            return picks[:count]

        except Exception as exc:
            self.log.error("Peer discovery failed: %s", exc, exc_info=True)
            self._record_error()
            return []

    def _record_error(self) -> None:
        """Record an error for rate limiting and recovery."""
        import time
        current_time = time.time()

        # Reset error count if it's been more than an hour
        if current_time - self.last_error_time > 3600:
            self.error_count = 0

        self.error_count += 1
        self.last_error_time = current_time

        # If too many errors, attempt recovery
        if self.error_count > self.max_errors_per_hour:
            self.log.warning("Too many discovery errors, attempting recovery")
            self._attempt_recovery()

    def _attempt_recovery(self) -> None:
        """Attempt to recover from discovery errors."""
        try:
            # Clear old addresses that might be causing issues
            self.address_book.cleanup_old_addresses()

            # Reset error count
            self.error_count = 0

            self.log.info("Discovery recovery completed")
        except Exception as exc:
            self.log.error("Discovery recovery failed: %s", exc)

    def record_connection_attempt(self, host: str, port: int) -> None:
        """Record a connection attempt."""
        self.address_book.record_attempt(host, port)

    def record_connection_success(self, host: str, port: int) -> None:
        """Record a successful connection."""
        self.address_book.record_success(host, port)

    def record_connection_failure(self, host: str, port: int) -> None:
        """Record a failed connection."""
        self.address_book.record_failure(host, port)

    def handle_addr_message(self, message: dict[str, Any], peer_host: str) -> None:
        """Handle incoming addr message from peer."""
        self.peer_exchange.handle_addr_message(message, peer_host)

    def create_addr_message(self) -> dict[str, Any]:
        """Create addr message to send to peers."""
        return self.peer_exchange.create_addr_message()

    def save_address_book(self) -> None:
        """Save address book to disk."""
        self.address_book.save()

    def cleanup(self) -> None:
        """Periodic cleanup of stale addresses."""
        self.address_book.cleanup_stale()
        self.save_address_book()

    def get_discovery_stats(self) -> dict[str, Any]:
        """Get peer discovery statistics."""
        return self.address_book.get_stats()
