import tempfile
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import GENESIS_PRIVKEY, GENESIS_PUBKEY, Chain, ChainError
from baseline.core.tx import COIN, Transaction, TxInput, TxOutput
from baseline.mempool import Mempool
from baseline.policy import MIN_RELAY_FEE_RATE
from baseline.storage import BlockStore, StateDB


class ReorgForkChoiceTests(unittest.TestCase):
    GENESIS_ADDRESS = crypto.address_from_pubkey(GENESIS_PUBKEY)

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)

        self.config = NodeConfig()
        self.config.data_dir = data_dir
        self.config.mining.allow_consensus_overrides = True
        self.config.mining.coinbase_maturity = 2
        self.config.mining.block_interval_target = 1
        self.config.mining.initial_bits = 0x207FFFFF
        self.config.mining.foundation_address = self.GENESIS_ADDRESS
        self.config.ensure_data_layout()

        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)

        # P2PKH to genesis pubkey (same as other tests)
        self.script_pubkey = b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac"

        # Make reorg deterministic in tests: disable fork-manager reorg rate limiting.
        self.chain.fork_manager.detector.min_reorg_interval = 0.0
        self.chain.fork_manager.detector.max_reorgs_per_hour = 10_000
        self.chain.fork_manager.detector.last_reorg_time = 0.0
        self.chain.fork_manager.detector.reorg_window_start = 0.0
        self.chain.fork_manager.detector.reorg_count = 0

    def tearDown(self) -> None:
        try:
            self.mempool.close()
        finally:
            self.state_db.close()
            self.tmpdir.cleanup()

    def _make_coinbase(self, height: int, value: int | None = None) -> Transaction:
        if value is None:
            value = self.chain._block_subsidy(height)
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        script_sig = len(height_bytes).to_bytes(1, "little") + height_bytes + b"\x01"
        foundation = self.chain._foundation_reward(value)
        outputs: list[TxOutput] = []
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

    def _mine_block(self, prev_hash: str, transactions: list[Transaction], time_delta: int | None = None) -> Block:
        parent_header = self.state_db.get_header(prev_hash)
        parent_ts = parent_header.timestamp if parent_header else self.chain.genesis_block.header.timestamp
        if time_delta is None:
            time_delta = self.config.mining.block_interval_target
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash(transactions),
            timestamp=parent_ts + time_delta,
            bits=self.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=transactions)
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
        return block

    def _payout_vout(self, coinbase: Transaction) -> int:
        # coinbase may have foundation output at vout 0; find our P2PKH payout
        for idx, out in enumerate(coinbase.outputs):
            if out.script_pubkey == self.script_pubkey:
                return idx
        raise AssertionError("coinbase payout output not found")

    def _signed_spend_from_coinbase(self, coinbase: Transaction) -> Transaction:
        vout = self._payout_vout(coinbase)
        amount_in = coinbase.outputs[vout].value
        tx = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=coinbase.txid(), prev_vout=vout, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=amount_in - MIN_RELAY_FEE_RATE, script_pubkey=self.script_pubkey)],
            lock_time=0,
        )
        sighash = tx.signature_hash(0, self.script_pubkey, 0x01)
        signature = crypto.sign(sighash, GENESIS_PRIVKEY) + b"\x01"
        pubkey = GENESIS_PUBKEY
        tx.inputs[0].script_sig = (
            len(signature).to_bytes(1, "little")
            + signature
            + len(pubkey).to_bytes(1, "little")
            + pubkey
        )
        tx.validate_basic()
        return tx

    def test_reorg_fork_choice_maturity_and_mempool_readd(self) -> None:
        """
        Covers, in one test:
          - competing chains + fork-choice (higher-work chain wins)
          - deep reorg (detach >= 3 blocks)
          - spending at maturity boundary on main chain
          - tx from disconnected blocks can be re-added to mempool after reorg (if still valid)
          - invalid fork block rejected (coinbase maturity violation)
        """
        # --- Build main chain A to height 5, including two spends at exact maturity ---
        prev = self.chain.genesis_hash

        # A1
        cb1 = self._make_coinbase(1)
        b1 = self._mine_block(prev, [cb1])
        res = self.chain.add_block(b1)
        self.assertEqual(res["status"], "connected")
        h1 = b1.block_hash()
        prev = h1

        # A2
        cb2 = self._make_coinbase(2)
        b2 = self._mine_block(prev, [cb2])
        res = self.chain.add_block(b2)
        self.assertEqual(res["status"], "connected")
        h2 = b2.block_hash()
        prev = h2

        # tx1 spends A1 coinbase at next_height=3 => exactly mature (maturity=2)
        tx1 = self._signed_spend_from_coinbase(cb1)
        mp_res = self.mempool.accept_transaction(tx1)
        self.assertEqual(mp_res["status"], "accepted")

        # A3 includes tx1
        cb3 = self._make_coinbase(3)
        b3 = self._mine_block(prev, [cb3, tx1])
        res = self.chain.add_block(b3)
        self.assertEqual(res["status"], "connected")
        self.mempool.remove_confirmed(b3.transactions)
        self.assertFalse(self.mempool.contains(tx1.txid()))
        h3 = b3.block_hash()
        prev = h3

        # tx2 spends A2 coinbase at next_height=4 => exactly mature
        tx2 = self._signed_spend_from_coinbase(cb2)
        mp_res = self.mempool.accept_transaction(tx2)
        self.assertEqual(mp_res["status"], "accepted")

        # A4 includes tx2
        cb4 = self._make_coinbase(4)
        b4 = self._mine_block(prev, [cb4, tx2])
        res = self.chain.add_block(b4)
        self.assertEqual(res["status"], "connected")
        self.mempool.remove_confirmed(b4.transactions)
        self.assertFalse(self.mempool.contains(tx2.txid()))
        h4 = b4.block_hash()
        prev = h4

        # A5
        cb5 = self._make_coinbase(5)
        b5 = self._mine_block(prev, [cb5])
        res = self.chain.add_block(b5)
        self.assertEqual(res["status"], "connected")
        h5 = b5.block_hash()

        best = self.state_db.get_best_tip()
        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best[1], 5)
        self.assertEqual(best[0], h5)

        # --- Invalid fork handling: block that violates coinbase maturity is rejected ---
        # Fork off A1 (height 1). At fork height 2, spending A1 coinbase is NOT mature (2-1 < 2).
        immature_spend = self._signed_spend_from_coinbase(cb1)
        cb_f2 = self._make_coinbase(2)
        bad_block = self._mine_block(h1, [cb_f2, immature_spend])
        with self.assertRaises(ChainError):
            self.chain.add_block(bad_block)

        # Tip unchanged after invalid fork attempt
        best = self.state_db.get_best_tip()
        assert best is not None
        self.assertEqual(best[0], h5)
        self.assertEqual(best[1], 5)

        # --- Competing chain B from A1 becomes longer => triggers deep reorg ---
        fork_prev = h1
        fork_tip = None
        saw_reorg = False
        # Build B2..B6 (5 blocks) so fork has more work than main from the fork point (A1)
        for height in range(2, 7):
            cb = self._make_coinbase(height)
            # Make fork coinbases differ from main-chain coinbases so their txids are different.
            # Otherwise A2 coinbase == B2 coinbase, and tx2 remains valid after the reorg.
            cb.inputs[0].script_sig += b"\x99"
            # Use a different timestamp delta to ensure fork blocks are not identical to main-chain blocks.
            blk = self._mine_block(fork_prev, [cb], time_delta=2)
            fork_prev = blk.block_hash()
            fork_tip = fork_prev
            res = self.chain.add_block(blk)
            if res.get("status") == "reorganized":
                saw_reorg = True
            # Early fork blocks may be returned as "rejected" by fork manager until it has more work.
            self.assertIn(
                res["status"],
                {"rejected", "side", "reorganized", "connected", "duplicate"},
                f"unexpected status {res['status']} for fork block at height {height}",
            )

        assert fork_tip is not None

        # After B6, we should have reorganized to the fork tip.
        self.assertTrue(saw_reorg, "expected at least one fork block to trigger a reorganization")
        best = self.state_db.get_best_tip()
        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best[0], fork_tip)
        self.assertEqual(best[1], 6)
        # Ensure the old tip (h5) is no longer on the main chain after the reorg.
        main_h5 = self.state_db.get_main_header_at_height(5)
        self.assertIsNotNone(main_h5)
        assert main_h5 is not None
        self.assertNotEqual(main_h5.hash, h5, "expected height-5 main-chain block to change after reorg")

        # Deep reorg: detach old A2..A5 (4 blocks), attach B2..B6 (5 blocks)
        fork_hash, old_branch, new_branch = self.chain._find_fork(h5, fork_tip)
        self.assertEqual(fork_hash, h1)
        self.assertGreaterEqual(len(old_branch), 3, f"expected deep reorg, detach={len(old_branch)}")
        self.assertGreaterEqual(len(new_branch), 4)

        # Transactions from disconnected blocks are no longer indexed in main chain.
        self.assertIsNone(self.state_db.get_transaction_location(tx1.txid()))
        self.assertIsNone(self.state_db.get_transaction_location(tx2.txid()))

        # --- Mempool re-add after reorg (manual re-accept): tx1 becomes valid again ---
        # tx1 spent A1 coinbase which is still on the new main chain, and its spend was undone.
        mp_res = self.mempool.accept_transaction(tx1)
        self.assertEqual(mp_res["status"], "accepted")
        self.assertTrue(self.mempool.contains(tx1.txid()))

        # tx2 spent A2 coinbase; A2 is no longer on main chain after reorg => missing UTXO -> orphan
        mp_res2 = self.mempool.accept_transaction(tx2)
        self.assertEqual(mp_res2["status"], "orphan")


if __name__ == "__main__":
    unittest.main()
