import tempfile
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import difficulty
from baseline.core.chain import Chain
from baseline.storage import BlockStore, HeaderData, StateDB


class LWMADifficultyTests(unittest.TestCase):
    def test_lwma_stable_at_target_spacing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            config = NodeConfig()
            config.data_dir = data_dir
            config.mining.allow_consensus_overrides = True
            config.mining.initial_bits = config.mining.pow_limit_bits
            config.ensure_data_layout()

            block_store = BlockStore(data_dir / "blocks")
            state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
            try:
                chain = Chain(config, state_db, block_store)
                genesis = state_db.get_header(chain.genesis_hash)
                assert genesis is not None

                bits = genesis.bits
                ts = genesis.timestamp
                prev_hash = genesis.hash
                chainwork = int(genesis.chainwork)

                # Add a few headers at exact target spacing; difficulty should stay the same.
                for height in range(1, 70):
                    ts += config.mining.block_interval_target
                    chainwork += difficulty.block_work(bits)
                    header_hash = f"{height:064x}"
                    state_db.store_header(
                        HeaderData(
                            hash=header_hash,
                            prev_hash=prev_hash,
                            height=height,
                            bits=bits,
                            nonce=0,
                            timestamp=ts,
                            merkle_root="00" * 32,
                            chainwork=str(chainwork),
                            version=1,
                            status=0,
                        )
                    )
                    prev_hash = header_hash

                parent = state_db.get_header(prev_hash)
                assert parent is not None
                expected = chain._expected_bits(parent.height + 1, parent)  # noqa: SLF001
                self.assertEqual(expected, bits)
            finally:
                state_db.close()

    def test_lwma_hardens_after_fast_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            config = NodeConfig()
            config.data_dir = data_dir
            config.mining.allow_consensus_overrides = True
            config.mining.initial_bits = config.mining.pow_limit_bits
            config.ensure_data_layout()

            block_store = BlockStore(data_dir / "blocks")
            state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
            try:
                chain = Chain(config, state_db, block_store)
                genesis = state_db.get_header(chain.genesis_hash)
                assert genesis is not None

                bits = genesis.bits
                ts = genesis.timestamp
                prev_hash = genesis.hash
                chainwork = int(genesis.chainwork)

                # Simulate a sudden hashrate spike by creating very fast blocks.
                for height in range(1, 80):
                    ts += 1
                    chainwork += difficulty.block_work(bits)
                    header_hash = f"{height:064x}"
                    state_db.store_header(
                        HeaderData(
                            hash=header_hash,
                            prev_hash=prev_hash,
                            height=height,
                            bits=bits,
                            nonce=0,
                            timestamp=ts,
                            merkle_root="00" * 32,
                            chainwork=str(chainwork),
                            version=1,
                            status=0,
                        )
                    )
                    prev_hash = header_hash

                parent = state_db.get_header(prev_hash)
                assert parent is not None
                expected = chain._expected_bits(parent.height + 1, parent)  # noqa: SLF001

                old_target = difficulty.compact_to_target(bits)
                new_target = difficulty.compact_to_target(expected)
                self.assertLess(new_target, old_target)
                # With 1-second blocks vs a 20-second target, LWMA should harden materially (roughly ~20×).
                self.assertLessEqual(new_target, old_target // 10)
            finally:
                state_db.close()
