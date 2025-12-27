import tempfile
import unittest
from pathlib import Path

import baseline.net.server as net_server
from baseline.config import NodeConfig
from baseline.core.chain import (
    MAINNET_GENESIS_HASH,
    MAINNET_GENESIS_MERKLE_ROOT,
    MAINNET_GENESIS_NONCE,
    MAINNET_GENESIS_TIMESTAMP,
    Chain,
)
from baseline.mempool import Mempool
from baseline.storage import BlockStore, StateDB


class MainnetFreezeTests(unittest.TestCase):
    def test_mainnet_genesis_is_fixed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            config = NodeConfig()
            config.data_dir = data_dir
            config.ensure_data_layout()

            block_store = BlockStore(data_dir / "blocks")
            state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
            chain = Chain(config, state_db, block_store)

            try:
                genesis = chain.state_db.get_header(chain.genesis_hash)
                self.assertIsNotNone(genesis)

                self.assertEqual(chain.genesis_hash, MAINNET_GENESIS_HASH)
                self.assertEqual(chain.genesis_block.header.timestamp, MAINNET_GENESIS_TIMESTAMP)
                self.assertEqual(chain.genesis_block.header.nonce, MAINNET_GENESIS_NONCE)
                self.assertEqual(chain.genesis_block.header.merkle_root, MAINNET_GENESIS_MERKLE_ROOT)
                self.assertEqual(chain.genesis_block.header.bits, config.mining.pow_limit_bits)
                self.assertEqual(chain._expected_bits(1, genesis), config.mining.initial_bits)  # noqa: SLF001
            finally:
                state_db.close()

    def test_mainnet_network_id_is_fixed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            config = NodeConfig()
            config.data_dir = data_dir
            config.ensure_data_layout()

            block_store = BlockStore(data_dir / "blocks")
            state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
            try:
                chain = Chain(config, state_db, block_store)
                mempool = Mempool(chain)
                original = net_server.P2PServer._init_local_addresses
                net_server.P2PServer._init_local_addresses = lambda self: None
                try:
                    network = net_server.P2PServer(config, chain, mempool)
                finally:
                    net_server.P2PServer._init_local_addresses = original
                    mempool.close()
            finally:
                state_db.close()

            self.assertEqual(network.network_id, net_server.MAINNET_NETWORK_ID)
