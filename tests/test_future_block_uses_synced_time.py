import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import (
    GENESIS_PUBKEY,
    MAX_FUTURE_BLOCK_TIME,
    Chain,
    ChainError,
)
from baseline.core.tx import Transaction, TxInput, TxOutput
from baseline.storage import BlockStore, StateDB
from baseline.time_sync import NTPResponse, TimeManager, set_time_manager


class FutureBlockTimeSyncTests(unittest.TestCase):
    GENESIS_ADDRESS = crypto.address_from_pubkey(GENESIS_PUBKEY)

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)

        self.config = NodeConfig()
        self.config.data_dir = data_dir
        self.config.mining.allow_consensus_overrides = True
        self.config.mining.coinbase_maturity = 2
        self.config.mining.foundation_address = self.GENESIS_ADDRESS
        self.config.ensure_data_layout()

        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)

        # P2PKH script to pay to genesis pubkey (same pattern used in other tests)
        self.script_pubkey = b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac"

        # Install a global time manager for synchronized_time_int()
        self.time_manager = TimeManager()
        set_time_manager(self.time_manager)

    def tearDown(self) -> None:
        set_time_manager(None)
        self.state_db.close()
        self.tmpdir.cleanup()

    def _make_coinbase(self, height: int) -> Transaction:
        value = self.chain._block_subsidy(height)
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        script_sig = len(height_bytes).to_bytes(1, "little") + height_bytes + b"\x01"

        foundation = self.chain._foundation_reward(value)
        outputs = []
        if foundation:
            outputs.append(TxOutput(value=foundation, script_pubkey=self.chain.foundation_script))
        outputs.append(TxOutput(value=value - foundation, script_pubkey=self.script_pubkey))

        return Transaction(
            version=1,
            inputs=[
                TxInput(
                    prev_txid="00" * 32,
                    prev_vout=0xFFFFFFFF,
                    script_sig=script_sig,
                    sequence=0xFFFFFFFF,
                )
            ],
            outputs=outputs,
            lock_time=0,
        )

    def _mine_block(self, prev_hash: str, timestamp: int, txs: list[Transaction]) -> Block:
        parent_header = self.state_db.get_header(prev_hash)
        if parent_header is None:
            raise RuntimeError("Missing parent header")

        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash(txs),
            timestamp=timestamp,
            bits=parent_header.bits,
            nonce=0,
        )
        block = Block(header=header, transactions=txs)

        # Solve PoW for this header (should be fast with test difficulty)
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
            if block.header.nonce > 0xFFFFFFFF:
                raise RuntimeError("Failed to solve block")

        return block

    def test_future_block_check_uses_synced_time(self) -> None:
        # Freeze system time so the test is deterministic.
        # Must be after genesis timestamp (which is fixed in code).
        frozen_now = 1733967400.0

        # Create a block whose timestamp is 1 second beyond the allowed future window
        # when offset == 0.
        block_ts = int(frozen_now) + MAX_FUTURE_BLOCK_TIME + 1
        coinbase = self._make_coinbase(height=1)
        block = self._mine_block(self.chain.genesis_hash, block_ts, [coinbase])

        with patch("time.time", return_value=frozen_now):
            # offset = 0 => should reject (timestamp too far in future)
            self.time_manager._update_offset(
                NTPResponse(offset=0.0, delay=0.0, server="test", timestamp=frozen_now)
            )
            with self.assertRaises(ChainError):
                self.chain.add_block(block)

            # negative offset => even stricter, still reject
            self.time_manager._update_offset(
                NTPResponse(offset=-10.0, delay=0.0, server="test", timestamp=frozen_now)
            )
            with self.assertRaises(ChainError):
                self.chain.add_block(block)

            # positive offset => widens the allowed future window, should accept
            self.time_manager._update_offset(
                NTPResponse(offset=10.0, delay=0.0, server="test", timestamp=frozen_now)
            )
            res = self.chain.add_block(block)
            self.assertEqual(res["status"], "connected")


if __name__ == "__main__":
    unittest.main()
