import tempfile
from pathlib import Path

from baseline.wallet import WalletManager


class DummyHeader:
    def __init__(self, hash_: str, height: int):
        self.hash = hash_
        self.height = height
        self.timestamp = 0


class DummyStateDB:
    def __init__(self):
        self.best = ("abc", 0)

    def get_best_tip(self):
        return self.best

    def get_main_header_at_height(self, height: int):
        return DummyHeader(self.best[0], height)


class DummyBlockStore:
    def get_block(self, _hash: str):
        raise RuntimeError("Unknown block")


class DummyMempool:
    def __init__(self):
        self.spent_outpoints = set()
        self.lock = None

    def register_listener(self, _):
        return


class DummyNetwork:
    def __init__(self):
        self.requested = []

    def request_block(self, block_hash: str) -> bool:
        self.requested.append(block_hash)
        return True


def test_wallet_sync_handles_missing_block_and_returns() -> None:
    tmp = tempfile.TemporaryDirectory()
    wallet_path = Path(tmp.name) / "wallet.json"
    state_db = DummyStateDB()
    block_store = DummyBlockStore()
    mempool = DummyMempool()
    network = DummyNetwork()

    wallet = WalletManager(wallet_path, state_db, block_store, mempool, network)
    ok = wallet.sync_chain()

    assert not ok
    status = wallet.sync_status()
    assert status["last_error"]
    assert network.requested == ["abc"]

    # Second attempt within retry window should not spam or re-request.
    ok = wallet.sync_chain()
    assert not ok
    assert network.requested == ["abc"]

    tmp.cleanup()
