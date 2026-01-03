import asyncio
import unittest

from baseline.net import protocol
from baseline.net.server import P2PServer


class DummyMempool:
    def __init__(self):
        self._entries = {}
        self.listeners = []

    def contains(self, txid: str) -> bool:
        return txid in self._entries

    def get(self, txid: str):
        return self._entries.get(txid)

    def add(self, txid: str, tx_hex: str) -> None:
        self._entries[txid] = DummyTx(txid, tx_hex)

    def register_listener(self, callback):
        self.listeners.append(callback)


class DummyTx:
    def __init__(self, txid: str, hexval: str):
        self._txid = txid
        self._hex = hexval

    def serialize(self) -> bytes:
        return bytes.fromhex(self._hex)


class DummyBlockStore:
    def has_block(self, _hash: str) -> bool:
        return False


class DummyStateDB:
    def get_best_tip(self):
        return None


class DummyChain:
    def __init__(self):
        self.genesis_hash = "00" * 32
        self.state_db = DummyStateDB()
        self.block_store = DummyBlockStore()


class FakePeer:
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.known_inventory: set[str] = set()
        self.sent: list[dict] = []
        self.remote_version = {"height": 1, "services": 1}
        self.outbound = True
        self.address = ("127.0.0.1", 0)
        self.closed = False

    async def send_message(self, payload):
        self.sent.append(payload)

    async def close(self):
        self.closed = True


def make_server() -> P2PServer:
    from baseline.config import NodeConfig

    cfg = NodeConfig()
    mempool = DummyMempool()
    chain = DummyChain()
    server = P2PServer(cfg, chain, mempool)
    return server


class P2PTxBroadcastTests(unittest.IsolatedAsyncioTestCase):
    async def test_broadcast_inv_to_all_peers(self) -> None:
        server = make_server()
        peer_a = FakePeer("A")
        peer_b = FakePeer("B")
        server.peers = {"A": peer_a, "B": peer_b}

        await server.broadcast_inv("tx", "deadbeef")
        await asyncio.sleep(0.05)
        self.assertTrue(any(msg["type"] == "inv" for msg in peer_a.sent))
        self.assertTrue(any(msg["type"] == "inv" for msg in peer_b.sent))

    async def test_handle_inv_requests_unknown_tx(self) -> None:
        server = make_server()
        peer = FakePeer("A")
        msg = protocol.inv_payload([{"type": "tx", "hash": "deadbeef"}])
        await server.handle_inv(peer, msg)
        self.assertEqual(len(peer.sent), 1)
        self.assertEqual(peer.sent[0]["type"], "getdata")
        self.assertEqual(peer.sent[0]["items"][0]["hash"], "deadbeef")

    async def test_handle_inv_skips_known_tx(self) -> None:
        server = make_server()
        peer = FakePeer("A")
        peer.known_inventory.add("deadbeef")
        server.mempool.add("deadbeef", "00")
        msg = protocol.inv_payload([{"type": "tx", "hash": "deadbeef"}])
        await server.handle_inv(peer, msg)
        self.assertEqual(peer.sent, [])
