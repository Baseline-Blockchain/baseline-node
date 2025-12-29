"""
P2P security enhancements for DoS protection and rate limiting.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


@dataclass
class SecurityMetrics:
    """Security metrics for monitoring and alerting."""
    connections_rejected: int = 0
    messages_dropped: int = 0
    peers_banned: int = 0
    rate_limit_violations: int = 0
    invalid_messages: int = 0


class RateLimiter:
    """Token bucket rate limiter for message throttling."""

    def __init__(self, max_tokens: int, refill_rate: float):
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate  # tokens per second
        self.tokens = max_tokens
        self.last_refill = time.time()

    def consume(self, tokens: int = 1) -> bool:
        """Attempt to consume tokens. Returns True if allowed."""
        now = time.time()
        elapsed = now - self.last_refill
        self.last_refill = now

        # Refill tokens based on elapsed time
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False


class PeerReputation:
    """Track peer reputation and behavior scoring."""

    def __init__(self):
        self.score = 100  # Start with neutral score
        self.violations = 0
        self.last_violation = 0.0
        self.connection_time = time.time()
        self.messages_sent = 0
        self.invalid_messages = 0

    def record_violation(self, severity: int = 1) -> None:
        """Record a protocol violation."""
        self.violations += 1
        self.score -= severity * 10
        self.last_violation = time.time()

    def record_invalid_message(self) -> None:
        """Record an invalid message."""
        self.invalid_messages += 1
        self.score -= 5

    def record_message(self) -> None:
        """Record a valid message."""
        self.messages_sent += 1
        # Slowly improve score for good behavior
        if self.messages_sent % 100 == 0 and self.score < 100:
            self.score += 1

    def is_banned(self) -> bool:
        """Check if peer should be banned."""
        return self.score <= 0 or self.violations >= 10

    def ban_duration(self) -> float:
        """Calculate ban duration in seconds."""
        if not self.is_banned():
            return 0.0
        # Progressive ban duration based on violations
        return min(3600 * (2 ** min(self.violations, 10)), 86400)  # Max 24 hours


class ConnectionLimiter:
    """Limit connections per IP and enforce global connection limits."""

    def __init__(self, max_per_ip: int = 3, max_total: int = 125):
        self.max_per_ip = max_per_ip
        self.max_total = max_total
        self.connections_per_ip: dict[str, int] = defaultdict(int)
        self.total_connections = 0

    def can_accept(self, ip: str) -> bool:
        """Check if a new connection from IP can be accepted."""
        if self.total_connections >= self.max_total:
            return False
        if self.connections_per_ip[ip] >= self.max_per_ip:
            return False
        return True

    def add_connection(self, ip: str) -> bool:
        """Add a connection if allowed."""
        if not self.can_accept(ip):
            return False
        self.connections_per_ip[ip] += 1
        self.total_connections += 1
        return True

    def remove_connection(self, ip: str) -> None:
        """Remove a connection."""
        if self.connections_per_ip[ip] > 0:
            self.connections_per_ip[ip] -= 1
            self.total_connections -= 1
            if self.connections_per_ip[ip] == 0:
                del self.connections_per_ip[ip]


class BanManager:
    """Manage banned peers and IP addresses."""

    def __init__(self):
        self.banned_ips: dict[str, float] = {}  # IP -> ban_until_timestamp
        self.banned_peers: dict[str, float] = {}  # peer_id -> ban_until_timestamp

    def ban_ip(self, ip: str, duration: float) -> None:
        """Ban an IP address for specified duration."""
        self.banned_ips[ip] = time.time() + duration

    def ban_peer(self, peer_id: str, duration: float) -> None:
        """Ban a peer ID for specified duration."""
        self.banned_peers[peer_id] = time.time() + duration

    def is_ip_banned(self, ip: str) -> bool:
        """Check if IP is currently banned."""
        if ip not in self.banned_ips:
            return False
        if time.time() >= self.banned_ips[ip]:
            del self.banned_ips[ip]
            return False
        return True

    def is_peer_banned(self, peer_id: str) -> bool:
        """Check if peer is currently banned."""
        if peer_id not in self.banned_peers:
            return False
        if time.time() >= self.banned_peers[peer_id]:
            del self.banned_peers[peer_id]
            return False
        return True

    def cleanup_expired(self) -> None:
        """Remove expired bans."""
        now = time.time()
        self.banned_ips = {ip: until for ip, until in self.banned_ips.items() if until > now}
        self.banned_peers = {peer: until for peer, until in self.banned_peers.items() if until > now}


class MessageValidator:
    """Validate P2P messages for security and protocol compliance."""

    # Maximum message sizes by type
    MAX_MESSAGE_SIZES = {
        "version": 4096,
        "verack": 64,
        "ping": 64,
        "pong": 64,
        "inv": 50000,  # Max 50KB for inventory
        "getdata": 50000,
        "tx": 100000,  # Max 100KB for transaction
        "block": 4000000,  # Max 4MB for block
        "headers": 2000000,  # Max 2MB for headers
        "getblocks": 4096,
        "getheaders": 4096,
        "addr": 10000,  # Max 10KB for addresses
        "getaddr": 64,
    }

    @classmethod
    def validate_message(cls, message: dict[str, Any]) -> tuple[bool, str]:
        """Validate a P2P message. Returns (is_valid, error_reason)."""
        msg_type = message.get("type")
        if not msg_type:
            return False, "Missing message type"

        if not isinstance(msg_type, str):
            return False, "Invalid message type format"

        # Check message size limits
        if msg_type in cls.MAX_MESSAGE_SIZES:
            # Estimate message size (rough JSON serialization)
            estimated_size = len(str(message))
            if estimated_size > cls.MAX_MESSAGE_SIZES[msg_type]:
                return False, f"Message too large: {estimated_size} > {cls.MAX_MESSAGE_SIZES[msg_type]}"

        # Type-specific validation
        if msg_type == "version":
            return cls._validate_version(message)
        elif msg_type == "inv":
            return cls._validate_inv(message)
        elif msg_type == "getdata":
            return cls._validate_getdata(message)
        elif msg_type == "tx":
            return cls._validate_tx(message)
        elif msg_type == "block":
            return cls._validate_block(message)
        elif msg_type == "addr":
            return cls._validate_addr(message)

        return True, ""

    @classmethod
    def _validate_version(cls, message: dict[str, Any]) -> tuple[bool, str]:
        """Validate version message."""
        required_fields = ["network", "services", "height", "port"]
        for field in required_fields:
            if field not in message:
                return False, f"Missing required field: {field}"

        if not isinstance(message.get("height"), int) or message["height"] < 0:
            return False, "Invalid height"

        if not isinstance(message.get("port"), int) or not (1 <= message["port"] <= 65535):
            return False, "Invalid listen port"

        return True, ""

    @classmethod
    def _validate_inv(cls, message: dict[str, Any]) -> tuple[bool, str]:
        """Validate inventory message."""
        inventory = message.get("inventory")
        if inventory is None:
            inventory = message.get("items", [])
        if not isinstance(inventory, list):
            return False, "Invalid inventory format"

        if len(inventory) > 1000:  # Limit inventory size
            return False, "Too many inventory items"

        for item in inventory:
            if not isinstance(item, dict):
                return False, "Invalid inventory item format"
            if "type" not in item or "hash" not in item:
                return False, "Missing inventory item fields"

        return True, ""

    @classmethod
    def _validate_getdata(cls, message: dict[str, Any]) -> tuple[bool, str]:
        """Validate getdata message."""
        return cls._validate_inv(message)  # Same format as inv

    @classmethod
    def _validate_tx(cls, message: dict[str, Any]) -> tuple[bool, str]:
        """Validate transaction message."""
        raw_hex = message.get("raw")
        txid = message.get("txid")
        if raw_hex is not None:
            if not isinstance(raw_hex, str):
                return False, "Invalid transaction encoding"
            if len(raw_hex) % 2 != 0:
                return False, "Raw transaction hex must have even length"
            try:
                bytes.fromhex(raw_hex)
            except ValueError:
                return False, "Invalid transaction hex"
            if not isinstance(txid, str) or len(txid) != 64:
                return False, "Invalid txid"
            return True, ""

        tx_data = message.get("transaction")
        if not isinstance(tx_data, dict):
            return False, "Missing transaction data"

        required_fields = ["version", "inputs", "outputs", "lock_time"]
        for field in required_fields:
            if field not in tx_data:
                return False, f"Missing transaction field: {field}"

        if not isinstance(tx_data["version"], int) or tx_data["version"] < 1:
            return False, "Invalid transaction version"

        if not isinstance(tx_data["inputs"], list) or len(tx_data["inputs"]) == 0:
            return False, "Transaction must have at least one input"

        for i, inp in enumerate(tx_data["inputs"]):
            if not isinstance(inp, dict):
                return False, f"Invalid input {i}: not a dict"
            if "prev_txid" not in inp or "prev_vout" not in inp:
                return False, f"Invalid input {i}: missing prev_txid or prev_vout"
            if not isinstance(inp["prev_txid"], str) or len(inp["prev_txid"]) != 64:
                return False, f"Invalid input {i}: invalid prev_txid format"
            if not isinstance(inp["prev_vout"], int) or inp["prev_vout"] < 0:
                return False, f"Invalid input {i}: invalid prev_vout"

        if not isinstance(tx_data["outputs"], list) or len(tx_data["outputs"]) == 0:
            return False, "Transaction must have at least one output"

        total_output_value = 0
        for i, out in enumerate(tx_data["outputs"]):
            if not isinstance(out, dict):
                return False, f"Invalid output {i}: not a dict"
            if "value" not in out or "script_pubkey" not in out:
                return False, f"Invalid output {i}: missing value or script_pubkey"
            if not isinstance(out["value"], int) or out["value"] < 0:
                return False, f"Invalid output {i}: invalid value"
            if out["value"] > 300_000_000 * 100_000_000:
                return False, f"Invalid output {i}: value exceeds maximum"
            total_output_value += out["value"]

        if total_output_value > 300_000_000 * 100_000_000:
            return False, "Total output value exceeds maximum"

        if not isinstance(tx_data["lock_time"], int) or tx_data["lock_time"] < 0:
            return False, "Invalid lock_time"
        if tx_data["lock_time"] > 0xFFFFFFFF:
            return False, "Invalid lock_time"

        return True, ""

    @classmethod
    def _validate_block(cls, message: dict[str, Any]) -> tuple[bool, str]:
        """Validate block message."""
        raw_hex = message.get("raw")
        if raw_hex is not None:
            if not isinstance(raw_hex, str):
                return False, "Invalid block payload"
            if len(raw_hex) % 2 != 0:
                return False, "Raw block hex must have even length"
            try:
                bytes.fromhex(raw_hex)
            except ValueError:
                return False, "Invalid block hex"
            block_hash = message.get("hash")
            if not isinstance(block_hash, str) or len(block_hash) != 64:
                return False, "Invalid block hash"
            return True, ""

        block_data = message.get("block")
        if not isinstance(block_data, dict):
            return False, "Missing block data"

        if "header" not in block_data or "transactions" not in block_data:
            return False, "Missing block header or transactions"

        header = block_data["header"]
        if not isinstance(header, dict):
            return False, "Block header must be a dict"

        required_header_fields = ["version", "prev_hash", "merkle_root", "timestamp", "bits", "nonce"]
        for field in required_header_fields:
            if field not in header:
                return False, f"Missing header field: {field}"

        if not isinstance(header["version"], int) or header["version"] < 1:
            return False, "Invalid block version"

        if not isinstance(header["prev_hash"], str) or len(header["prev_hash"]) != 64:
            return False, "Invalid prev_hash format"

        if not isinstance(header["merkle_root"], str) or len(header["merkle_root"]) != 64:
            return False, "Invalid merkle_root format"

        if not isinstance(header["timestamp"], int) or header["timestamp"] < 0:
            return False, "Invalid timestamp"

        if not isinstance(header["bits"], int) or header["bits"] < 0:
            return False, "Invalid bits"

        if not isinstance(header["nonce"], int) or header["nonce"] < 0:
            return False, "Invalid nonce"

        transactions = block_data["transactions"]
        if not isinstance(transactions, list) or len(transactions) == 0:
            return False, "Block must have at least one transaction"

        for i, tx in enumerate(transactions):
            tx_valid, tx_error = cls._validate_tx({"transaction": tx})
            if not tx_valid:
                return False, f"Invalid transaction {i}: {tx_error}"

        first_tx = transactions[0]
        if (
            not isinstance(first_tx.get("inputs"), list)
            or len(first_tx["inputs"]) != 1
            or first_tx["inputs"][0].get("prev_txid") != "00" * 32
            or first_tx["inputs"][0].get("prev_vout") != 0xFFFFFFFF
        ):
            return False, "First transaction must be coinbase"

        for i, tx in enumerate(transactions[1:], 1):
            if (
                isinstance(tx.get("inputs"), list)
                and len(tx["inputs"]) == 1
                and tx["inputs"][0].get("prev_txid") == "00" * 32
                and tx["inputs"][0].get("prev_vout") == 0xFFFFFFFF
            ):
                return False, f"Transaction {i} is coinbase but not first transaction"

        return True, ""

    @classmethod
    def _validate_addr(cls, message: dict[str, Any]) -> tuple[bool, str]:
        """Validate address message."""
        addresses = message.get("addresses")
        if addresses is None:
            addresses = message.get("peers", [])
        if not isinstance(addresses, list):
            return False, "Invalid addresses format"

        if len(addresses) > 1000:  # Limit address count
            return False, "Too many addresses"

        for addr in addresses:
            if not isinstance(addr, dict):
                return False, "Invalid address format"
            if "host" not in addr or "port" not in addr:
                return False, "Missing address fields"

        return True, ""


class P2PSecurity:
    """Main P2P security coordinator."""

    def __init__(self, enable_rate_limit: bool = True):
        self.connection_limiter = ConnectionLimiter()
        self.ban_manager = BanManager()
        self.peer_reputations: dict[str, PeerReputation] = {}
        self.rate_limiters: dict[str, RateLimiter] = {}  # per-peer rate limiters
        self.metrics = SecurityMetrics()
        self.enable_rate_limit = enable_rate_limit

        # Global rate limiters
        self.global_connection_limiter = (
            RateLimiter(max_tokens=100, refill_rate=10.0) if enable_rate_limit else None
        )
        self.global_message_limiter = (
            RateLimiter(max_tokens=10000, refill_rate=500.0) if enable_rate_limit else None
        )

    def can_accept_connection(self, ip: str) -> bool:
        """Check if connection from IP should be accepted."""
        if self.ban_manager.is_ip_banned(ip):
            self.metrics.connections_rejected += 1
            return False

        if self.enable_rate_limit and self.global_connection_limiter:
            if not self.global_connection_limiter.consume():
                self.metrics.connections_rejected += 1
                return False

        if not self.connection_limiter.can_accept(ip):
            self.metrics.connections_rejected += 1
            return False

        return True

    def add_connection(self, ip: str, peer_id: str) -> bool:
        """Add a new connection."""
        if not self.connection_limiter.add_connection(ip):
            return False

        # Initialize peer reputation and rate limiter
        self.peer_reputations[peer_id] = PeerReputation()
        if self.enable_rate_limit:
            # Allow higher burst for initial sync; refill fast enough for steady flow.
            self.rate_limiters[peer_id] = RateLimiter(max_tokens=300, refill_rate=120.0)

        return True

    def remove_connection(self, ip: str, peer_id: str) -> None:
        """Remove a connection."""
        self.connection_limiter.remove_connection(ip)

        # Clean up peer data
        self.peer_reputations.pop(peer_id, None)
        self.rate_limiters.pop(peer_id, None)

    def should_accept_message(
        self,
        peer_id: str,
        message: dict[str, Any],
        *,
        skip_rate_limit: bool = False,
    ) -> tuple[bool, str]:
        """Check if message from peer should be accepted."""
        # Check global rate limit
        if self.enable_rate_limit and not skip_rate_limit and self.global_message_limiter:
            if not self.global_message_limiter.consume():
                self.metrics.rate_limit_violations += 1
                return False, "Global rate limit exceeded"

        # Check peer-specific rate limit
        if self.enable_rate_limit and not skip_rate_limit:
            rate_limiter = self.rate_limiters.get(peer_id)
            if rate_limiter and not rate_limiter.consume():
                self.metrics.rate_limit_violations += 1
                return False, "Peer rate limit exceeded"

        # Check if peer is banned
        if self.ban_manager.is_peer_banned(peer_id):
            return False, "Peer is banned"

        # Validate message format and content
        is_valid, error = MessageValidator.validate_message(message)
        if not is_valid:
            self.metrics.invalid_messages += 1
            reputation = self.peer_reputations.get(peer_id)
            if reputation:
                reputation.record_invalid_message()
            return False, f"Invalid message: {error}"

        # Record valid message
        reputation = self.peer_reputations.get(peer_id)
        if reputation:
            reputation.record_message()

        return True, ""

    def record_violation(self, peer_id: str, severity: int = 1) -> None:
        """Record a protocol violation for a peer."""
        reputation = self.peer_reputations.get(peer_id)
        if reputation:
            reputation.record_violation(severity)
            if reputation.is_banned():
                ban_duration = reputation.ban_duration()
                self.ban_manager.ban_peer(peer_id, ban_duration)
                self.metrics.peers_banned += 1

    def cleanup(self) -> None:
        """Periodic cleanup of expired data."""
        self.ban_manager.cleanup_expired()

        # Remove old peer reputations for disconnected peers
        # (Called every 5 minutes by the P2P server cleanup loop)

    def get_security_status(self) -> dict[str, Any]:
        """Get current security status for monitoring."""
        return {
            "metrics": {
                "connections_rejected": self.metrics.connections_rejected,
                "messages_dropped": self.metrics.messages_dropped,
                "peers_banned": self.metrics.peers_banned,
                "rate_limit_violations": self.metrics.rate_limit_violations,
                "invalid_messages": self.metrics.invalid_messages,
            },
            "active_connections": self.connection_limiter.total_connections,
            "banned_ips": len(self.ban_manager.banned_ips),
            "banned_peers": len(self.ban_manager.banned_peers),
            "peer_reputations": len(self.peer_reputations),
        }
