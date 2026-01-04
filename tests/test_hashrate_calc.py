import math
import threading

from baseline.config import NodeConfig
from baseline.core import difficulty
from baseline.mining.stratum import StratumServer
from baseline.rpc.handlers import RPCHandlers


class _DummyStateDB:
    pass


class _DummyChain:
    def __init__(self):
        self.config = NodeConfig()
        self.config.mining.block_interval_target = 20
        # Use the chain pow limit as the diff1 target.
        self.max_target = difficulty.compact_to_target(self.config.mining.pow_limit_bits)
        self.state_db = _DummyStateDB()


class _DummyMempool:
    def __init__(self):
        self.lock = threading.Lock()
        self.entries = {}

    def register_listener(self, _callback):
        # Stratum hooks a listener; not needed for these tests.
        return


class _DummyBlockStore:
    data_path = None
    index_path = None


class _DummyBuilder:
    extranonce1_size = 4
    extranonce2_size = 4


class _DummyPayoutTracker:
    def register_worker(self, *_args, **_kwargs):
        return

    def record_share(self, *_args, **_kwargs):
        return

    def record_block(self, *_args, **_kwargs):
        return


def test_network_hashps_uses_pow_limit_constant() -> None:
    chain = _DummyChain()
    handlers = RPCHandlers(
        chain=chain,
        mempool=_DummyMempool(),
        block_store=_DummyBlockStore(),
        template_builder=None,
    )
    difficulty_value = 18_139_166_620
    spacing = chain.config.mining.block_interval_target

    expected = (2**256 / chain.max_target) * difficulty_value / spacing
    actual = handlers._network_hashps(difficulty_value, spacing)

    assert math.isclose(actual, expected, rel_tol=1e-9)


def test_stratum_difficulty_to_target_uses_pow_limit() -> None:
    chain = _DummyChain()
    mempool = _DummyMempool()
    builder = _DummyBuilder()
    payout = _DummyPayoutTracker()
    server = StratumServer(chain.config, chain, mempool, builder, payout)

    target = server.difficulty_to_target(4.0)
    assert target == chain.max_target // 4
