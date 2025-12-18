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
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class PeerAddress:
    """Enhanced peer address with metadata."""
    host: str
    port: int
    last_seen: float
    services: int = 1
    attempts: int = 0
    last_attempt: float = 0.0
    success_count: int = 0
    failure_count: int = 0
    source: str = "unknown"  # "seed", "dns", "peer", "manual"
    
    def key(self) -> tuple[str, int]:
        return self.host, self.port
    
    def is_stale(self, max_age: float = 86400 * 7) -> bool:
        """Check if address is stale (not seen for max_age seconds)."""
        return time.time() - self.last_seen > max_age
    
    def should_retry(self, min_retry_delay: float = 3600) -> bool:
        """Check if we should retry connecting to this address."""
        if self.last_attempt == 0:
            return True
        return time.time() - self.last_attempt > min_retry_delay
    
    def record_attempt(self) -> None:
        """Record a connection attempt."""
        self.attempts += 1
        self.last_attempt = time.time()
    
    def record_success(self) -> None:
        """Record a successful connection."""
        self.success_count += 1
        self.last_seen = time.time()
    
    def record_failure(self) -> None:
        """Record a failed connection."""
        self.failure_count += 1
    
    def reliability_score(self) -> float:
        """Calculate reliability score (0.0 to 1.0)."""
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.5  # Neutral score for untested addresses
        return self.success_count / total


class DNSSeeder:
    """DNS seed resolver for peer discovery."""
    
    def __init__(self, dns_seeds: list[str]):
        self.dns_seeds = dns_seeds
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
            
            # Resolve hostname to IP addresses
            loop = asyncio.get_event_loop()
            addrinfo = await loop.getaddrinfo(
                hostname, None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
            )
            
            for family, type_, proto, canonname, sockaddr in addrinfo:
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
        
        except Exception as exc:
            self.log.debug("DNS resolution failed for %s: %s", seed, exc)
        
        return addresses
    
    def _is_routable_ip(self, ip: str) -> bool:
        """Check if IP address is routable (not localhost/private)."""
        try:
            addr = socket.inet_pton(socket.AF_INET, ip)
            # Check for localhost (127.x.x.x)
            if addr[0] == 127:
                return False
            # Check for private networks (10.x.x.x, 172.16-31.x.x, 192.168.x.x)
            if addr[0] == 10:
                return False
            if addr[0] == 172 and 16 <= addr[1] <= 31:
                return False
            if addr[0] == 192 and addr[1] == 168:
                return False
            return True
        except socket.error:
            try:
                # IPv6 check
                addr = socket.inet_pton(socket.AF_INET6, ip)
                # Skip localhost and link-local
                if ip.startswith("::1") or ip.startswith("fe80:"):
                    return False
                return True
            except socket.error:
                return False


class AddressBook:
    """Persistent address book for peer management."""
    
    def __init__(self, path: Path):
        self.path = path
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
            
            for addr_data in data.get("addresses", []):
                addr = PeerAddress(**addr_data)
                self.addresses[addr.key()] = addr
            
            self.log.info("Loaded %d addresses from address book", len(self.addresses))
        except Exception as exc:
            self.log.warning("Failed to load address book: %s", exc)
    
    def save(self) -> None:
        """Save addresses to disk."""
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            
            data = {
                "version": 1,
                "timestamp": time.time(),
                "addresses": [asdict(addr) for addr in self.addresses.values()]
            }
            
            with self.path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            
            self.log.debug("Saved %d addresses to address book", len(self.addresses))
        except Exception as exc:
            self.log.warning("Failed to save address book: %s", exc)
    
    def add_address(self, address: PeerAddress) -> None:
        """Add or update an address."""
        key = address.key()
        existing = self.addresses.get(key)
        
        if existing:
            # Update existing address with newer information
            if address.last_seen > existing.last_seen:
                existing.last_seen = address.last_seen
                existing.services = address.services
            # Keep attempt/success/failure counts
        else:
            self.addresses[key] = address
    
    def add_addresses(self, addresses: list[PeerAddress]) -> None:
        """Add multiple addresses."""
        for addr in addresses:
            self.add_address(addr)
    
    def get_addresses(self, count: int, exclude: set[tuple[str, int]] | None = None) -> list[PeerAddress]:
        """Get addresses for connection attempts."""
        exclude = exclude or set()
        
        # Filter addresses
        candidates = []
        for addr in self.addresses.values():
            if addr.key() in exclude:
                continue
            if addr.is_stale():
                continue
            if not addr.should_retry():
                continue
            candidates.append(addr)
        
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
    
    def create_addr_message(self, max_addresses: int = 1000) -> dict[str, Any]:
        """Create an addr message with known addresses."""
        addresses = self.address_book.get_addresses(max_addresses)
        
        addr_list = []
        for addr in addresses:
            # Only share addresses we've successfully connected to
            if addr.success_count > 0:
                addr_list.append({
                    "host": addr.host,
                    "port": addr.port,
                    "services": addr.services,
                    "timestamp": int(addr.last_seen),
                })
        
        return {
            "type": "addr",
            "addresses": addr_list,
        }
    
    def handle_addr_message(self, message: dict[str, Any], peer_host: str) -> None:
        """Handle incoming addr message."""
        addresses = message.get("addresses", [])
        
        new_addresses = []
        for addr_data in addresses:
            try:
                addr = PeerAddress(
                    host=addr_data["host"],
                    port=addr_data["port"],
                    last_seen=addr_data.get("timestamp", time.time()),
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
            self.address_book.add_addresses(new_addresses)
            self.log.debug("Added %d addresses from peer %s", len(new_addresses), peer_host)
    
    def _is_valid_address(self, addr: PeerAddress) -> bool:
        """Validate a peer address."""
        # Check port range
        if not (1 <= addr.port <= 65535):
            return False
        
        # Check if it's a routable IP
        try:
            socket.inet_pton(socket.AF_INET, addr.host)
            return True
        except socket.error:
            try:
                socket.inet_pton(socket.AF_INET6, addr.host)
                return True
            except socket.error:
                return False


class PeerDiscovery:
    """Main peer discovery coordinator."""
    
    def __init__(self, data_dir: Path, dns_seeds: list[str], manual_seeds: list[str]):
        self.address_book = AddressBook(data_dir / "peers.json")
        self.dns_seeder = DNSSeeder(dns_seeds)
        self.peer_exchange = PeerExchange(self.address_book)
        self.manual_seeds = manual_seeds
        self.log = logging.getLogger("baseline.peer_discovery")
        
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
    
    async def discover_peers(self, count: int) -> list[tuple[str, int]]:
        """Discover peer addresses for connection."""
        # First try existing addresses
        existing = self.address_book.get_addresses(count)
        
        # If we don't have enough, try DNS seeds
        if len(existing) < count:
            try:
                dns_addresses = await self.dns_seeder.resolve_seeds()
                self.address_book.add_addresses(dns_addresses)
                self.log.info("Discovered %d addresses from DNS seeds", len(dns_addresses))
            except Exception as exc:
                self.log.warning("DNS seed discovery failed: %s", exc)
            
            # Get addresses again after adding DNS results
            existing = self.address_book.get_addresses(count)
        
        return [addr.key() for addr in existing]
    
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