import asyncio
import time

from baseline.config import NodeConfig
from baseline.mining.stratum import MIN_DIFF_FLOOR, StratumServer, StratumSession


class DummyReader:
    pass


class DummyWriter:
    def __init__(self):
        self.closed = False
        self.buffer = []

    def get_extra_info(self, _name, default=None):
        return default

    def write(self, data):
        self.buffer.append(data)

    async def drain(self):
        return

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return


class DummyChain:
    def __init__(self):
        self.config = NodeConfig()
        self.config.stratum.min_difficulty = 0.0001
        self.max_target = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
        self.state_db = None


class DummyMempool:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.entries = {}

    def register_listener(self, _):
        return


class DummyBuilder:
    extranonce1_size = 4
    extranonce2_size = 4

    def build_template(self):
        raise RuntimeError("not used")


class DummyPayout:
    def register_worker(self, *_args, **_kwargs):
        return

    def record_share(self, *_args, **_kwargs):
        return

    def record_block(self, *_args, **_kwargs):
        return


def make_session():
    chain = DummyChain()
    server = StratumServer(chain.config, chain, DummyMempool(), DummyBuilder(), DummyPayout())
    session = StratumSession(server, DummyReader(), DummyWriter())
    return server, session


def test_set_difficulty_respects_floor():
    server, session = make_session()
    session.set_difficulty(1e-7)
    assert session.difficulty >= MIN_DIFF_FLOOR


def test_vardiff_adjusts_on_first_window():
    server, session = make_session()
    session.share_times.clear()
    now = time.time()
    # Simulate very fast shares to trigger an increase.
    session.share_times.extend([now - 0.6, now - 0.4, now - 0.2, now])
    session.last_diff_update = 0.0

    asyncio.run(server._maybe_adjust_difficulty(session))

    assert session.difficulty > server.config.stratum.min_difficulty
    assert session.last_diff_update > 0
