import tempfile
import time
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.address import script_from_address
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import Chain, ChainError
from baseline.core.tx import Transaction, TxInput, TxOutput
from baseline.mempool import Mempool
from baseline.mining.payout import PayoutTracker
from baseline.storage import BlockStore, StateDB
from baseline.wallet import WalletManager


def _make_wif(priv_int: int, *, compressed: bool = True) -> str:
    priv_bytes = priv_int.to_bytes(32, "big")
    payload = b"\x80" + priv_bytes + (b"\x01" if compressed else b"")
    return crypto.base58check_encode(payload)


class NodeSim:
    def __init__(self, root: Path, node_id: int):
        self.config = NodeConfig()
        self.config.data_dir = root / f"node{node_id}"
        self.config.mining.allow_consensus_overrides = True
        self.config.mining.coinbase_maturity = 1
        self.config.ensure_data_layout()
        self.block_store = BlockStore(self.config.data_dir / "blocks")
        self.state_db = StateDB(self.config.data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)
        self.privkey = node_id + 2
        self.pubkey = crypto.generate_pubkey(self.privkey)
        self.script = b"\x76\xa9\x14" + crypto.hash160(self.pubkey) + b"\x88\xac"
        self.mempool = Mempool(self.chain)

    def close(self) -> None:
        self.state_db.close()

    def _make_coinbase(self, height: int) -> Transaction:
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        script_sig = len(height_bytes).to_bytes(1, "little") + height_bytes
        subsidy = self.chain._block_subsidy(height)
        foundation = self.chain._foundation_reward(subsidy)
        outputs = []
        if foundation:
            outputs.append(TxOutput(value=foundation, script_pubkey=self.chain.foundation_script))
        outputs.append(TxOutput(value=subsidy - foundation, script_pubkey=self.script))
        return Transaction(
            version=1,
            inputs=[TxInput(prev_txid="00" * 32, prev_vout=0xFFFFFFFF, script_sig=script_sig, sequence=0xFFFFFFFF)],
            outputs=outputs,
            lock_time=0,
        )

    def create_spend_transaction(
        self,
        prev_tx: Transaction,
        prev_vout: int,
        dest_script: bytes,
        amount: int,
        *,
        fee: int = 1_000,
        change_script: bytes | None = None,
    ) -> Transaction:
        output = prev_tx.outputs[prev_vout]
        if amount + fee > output.value:
            raise ValueError("Insufficient input value")
        change = output.value - amount - fee
        inputs = [TxInput(prev_txid=prev_tx.txid(), prev_vout=prev_vout, script_sig=b"", sequence=0xFFFFFFFF)]
        outputs = [TxOutput(value=amount, script_pubkey=dest_script)]
        if change > 0:
            change_script = change_script or self.script
            outputs.append(TxOutput(value=change, script_pubkey=change_script))
        tx = Transaction(version=1, inputs=inputs, outputs=outputs, lock_time=0)
        sighash = tx.signature_hash(0, output.script_pubkey, 0x01)
        signature = crypto.sign(sighash, self.privkey) + b"\x01"
        script_sig = bytes([len(signature)]) + signature + bytes([len(self.pubkey)]) + self.pubkey
        tx.inputs[0].script_sig = script_sig
        return tx

    def mine_block(self, parent_hash: str | None = None, extra_txs: list[Transaction] | None = None) -> Block:
        if parent_hash:
            parent = self.state_db.get_header(parent_hash)
            if parent is None:
                raise ValueError("Unknown parent hash")
            prev_hash = parent.hash
            height = parent.height + 1
            base_time = parent.timestamp
        else:
            tip = self.state_db.get_best_tip()
            prev_hash = tip[0] if tip else self.chain.genesis_hash
            parent = self.state_db.get_header(prev_hash)
            height = (parent.height + 1) if parent else 1
            base_time = parent.timestamp if parent else self.chain.genesis_block.header.timestamp
        coinbase = self._make_coinbase(height)
        txs = [coinbase]
        if extra_txs:
            txs.extend(extra_txs)
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash(txs),
            timestamp=base_time + self.config.mining.block_interval_target,
            bits=self.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=txs)
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
        return block


class NetworkMiningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)
        self.nodes = [NodeSim(root, idx) for idx in range(10)]

    def tearDown(self) -> None:
        for node in self.nodes:
            node.close()
        self.tmpdir.cleanup()

    def test_simulated_network_many_miners(self) -> None:
        rounds = 6
        for round_idx in range(rounds):
            miner_index = round_idx % 9  # 9 miners, 1 passive node
            block = self.nodes[miner_index].mine_block()
            raw = block.serialize()
            for node in self.nodes:
                result = node.chain.add_block(block, raw)
                self.assertIn(result["status"], {"connected", "reorganized", "duplicate"})
        tips = {node.state_db.get_best_tip() for node in self.nodes}
        self.assertEqual(len(tips), 1)
        final_hash, final_height = tips.pop()
        self.assertGreaterEqual(final_height, rounds)
        for node in self.nodes:
            best = node.state_db.get_best_tip()
            self.assertEqual(best[0], final_hash)

    def test_rejects_double_spend_block(self) -> None:
        miner = self.nodes[0]
        base_block = miner.mine_block()
        raw_base = base_block.serialize()
        for node in self.nodes:
            node.chain.add_block(base_block, raw_base)
        prev_tx = base_block.transactions[0]
        spend_output_index = len(prev_tx.outputs) - 1
        spend_amount = prev_tx.outputs[spend_output_index].value - 1_000
        dest_script = miner.script
        spend_tx = miner.create_spend_transaction(prev_tx, spend_output_index, dest_script, spend_amount, fee=1_000)
        conflicting_tx = miner.create_spend_transaction(prev_tx, spend_output_index, dest_script, spend_amount, fee=1_000)
        bad_block = miner.mine_block(parent_hash=base_block.block_hash(), extra_txs=[spend_tx, conflicting_tx])
        raw_bad = bad_block.serialize()
        for node in self.nodes:
            with self.assertRaises(ChainError) as ctx:
                node.chain.add_block(bad_block, raw_bad)
            self.assertIn("Double spend inside block", str(ctx.exception))


class PayoutTrackerReorgTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.primary = NodeSim(self.root, 0)
        self.fork = NodeSim(self.root, 1)

    def tearDown(self) -> None:
        self.primary.close()
        self.fork.close()
        self.tmpdir.cleanup()

    def _wait_for_header(self, node: NodeSim, block_hash: str) -> None:
        deadline = time.time() + 1.0
        while time.time() < deadline:
            if node.state_db.get_header(block_hash):
                return
            time.sleep(0.01)
        raise AssertionError(f"Header {block_hash} never appeared")

    def _wait_for_best_tip(self, node: NodeSim, block_hash: str) -> int:
        deadline = time.time() + 1.0
        while time.time() < deadline:
            tip = node.state_db.get_best_tip()
            if tip and tip[0] == block_hash:
                return tip[1]
            time.sleep(0.01)
        raise AssertionError(f"Best tip {block_hash} never became active")

    def _build_tracker(self, ledger_name: str) -> PayoutTracker:
        ledger = self.root / ledger_name
        return PayoutTracker(
            ledger,
            self.primary.privkey,
            self.primary.pubkey,
            self.primary.script,
            maturity=1,
            min_payout=50_000_000,
            pool_fee_percent=1.0,
        )

    def _assert_payout_distribution(self, payout_tx: Transaction, expected: dict[bytes, int], pool_fee: int) -> None:
        assert payout_tx is not None
        worker_balance = {script: amount for script, amount in expected.items()}
        pool_amount = 0
        for output in payout_tx.outputs:
            if output.script_pubkey in worker_balance:
                self.assertEqual(output.value, worker_balance[output.script_pubkey])
                worker_balance.pop(output.script_pubkey, None)
            if output.script_pubkey == self.primary.script:
                pool_amount += output.value
        self.assertFalse(worker_balance)
        self.assertGreaterEqual(pool_amount, pool_fee)

    def test_payout_tracker_survives_long_reorg(self) -> None:
        base_blocks: list[Block] = []
        for _ in range(2):
            block = self.primary.mine_block()
            raw = block.serialize()
            self.primary.chain.add_block(block, raw)
            self._wait_for_header(self.primary, block.block_hash())
            self.fork.chain.add_block(block, raw)
            base_blocks.append(block)
        fork_blocks: list[Block] = []
        parent_hash = base_blocks[0].block_hash()
        self._wait_for_header(self.primary, parent_hash)
        for _ in range(3):
            block = self.primary.mine_block(parent_hash=parent_hash)
            fork_blocks.append(block)
            self.primary.chain.add_block(block, block.serialize())
            self._wait_for_header(self.primary, block.block_hash())
            parent_hash = block.block_hash()
        matured_hash = fork_blocks[-1].block_hash()
        matured_header = self.primary.state_db.get_header(matured_hash)
        assert matured_header is not None
        matured_height = matured_header.height
        extension_block = self.primary.mine_block(parent_hash=matured_hash)
        self.primary.chain.add_block(extension_block, extension_block.serialize())
        self._wait_for_header(self.primary, extension_block.block_hash())
        height = self._wait_for_best_tip(self.primary, extension_block.block_hash())
        tracker = self._build_tracker("payout-reorg.json")
        primary_address = crypto.address_from_pubkey(self.primary.pubkey)
        worker2_pub = crypto.generate_pubkey(7)
        worker2_address = crypto.address_from_pubkey(worker2_pub)
        tracker.min_fee_rate = 0
        tracker.record_share("primary", primary_address, difficulty=1.0)
        tracker.record_share("worker-2", worker2_address, difficulty=2.0)
        coinbase_tx = fork_blocks[-1].transactions[0]
        reward = coinbase_tx.outputs[1].value
        txid = coinbase_tx.txid()
        tracker.record_block(height=matured_height, coinbase_txid=txid, reward=reward, vout=1)
        tracker.process_maturity(best_height=height)
        self.assertTrue(tracker.matured_utxos)
        entry = tracker.matured_utxos[0]
        self.assertEqual(entry.get("vout"), 1)
        payees = tracker._gather_payees(16)
        self.assertTrue(payees)
        total_balance = sum(ws.balance for _, ws in tracker.workers.items())
        self.assertLessEqual(total_balance, reward)
        utxo_record = self.primary.state_db.get_utxo(txid, 1)
        self.assertIsNotNone(utxo_record)
        payout_tx = tracker.create_payout_transaction(self.primary.state_db)
        pool_fee = int(reward * (tracker.pool_fee_percent / 100.0))
        distributable = reward - pool_fee
        expected_worker1 = int(distributable * (1.0 / 3.0))
        expected_worker2 = int(distributable * (2.0 / 3.0))
        expected = {
            script_from_address(primary_address): expected_worker1,
            script_from_address(worker2_address): expected_worker2,
        }
        self._assert_payout_distribution(payout_tx, expected, pool_fee)

    def test_payout_tracker_survives_long_reorg_with_transactions(self) -> None:
        wallet_path = self.root / "wallet.dat"
        wallet_path.parent.mkdir(parents=True, exist_ok=True)
        wallet = WalletManager(wallet_path, self.primary.state_db, self.primary.block_store, self.primary.mempool)
        wallet_address = wallet.get_new_address("reorg")
        base_blocks: list[Block] = []
        for _ in range(2):
            block = self.primary.mine_block()
            self.primary.chain.add_block(block, block.serialize())
            self._wait_for_header(self.primary, block.block_hash())
            base_blocks.append(block)
        parent_hash = base_blocks[0].block_hash()
        dest_script = script_from_address(wallet_address)
        prev_tx = base_blocks[0].transactions[0]
        spend_output = prev_tx.outputs[1].value
        spend_amount = spend_output - 1_000
        spend_tx = self.primary.create_spend_transaction(prev_tx, 1, dest_script, spend_amount, fee=1_000)
        fork_blocks: list[Block] = []
        branch_parent = parent_hash
        self._wait_for_header(self.primary, branch_parent)
        for idx in range(4):
            extra = [spend_tx] if idx == 0 else None
            block = self.primary.mine_block(parent_hash=branch_parent, extra_txs=extra)
            fork_blocks.append(block)
            self.primary.chain.add_block(block, block.serialize())
            self._wait_for_header(self.primary, block.block_hash())
            branch_parent = block.block_hash()
        matured_hash = fork_blocks[-1].block_hash()
        matured_header = self.primary.state_db.get_header(matured_hash)
        assert matured_header is not None
        matured_height = matured_header.height
        extension_block = self.primary.mine_block(parent_hash=matured_hash)
        self.primary.chain.add_block(extension_block, extension_block.serialize())
        self._wait_for_header(self.primary, extension_block.block_hash())
        height = self._wait_for_best_tip(self.primary, extension_block.block_hash())
        wallet.sync_chain()
        tx_entry = wallet.get_transaction(spend_tx.txid())
        self.assertIsNotNone(tx_entry)
        self.assertGreater(tx_entry["confirmations"], 0)
        tracker = self._build_tracker("payout-reorg-wallet.json")
        primary_address = crypto.address_from_pubkey(self.primary.pubkey)
        worker2_pub = crypto.generate_pubkey(7)
        worker2_address = crypto.address_from_pubkey(worker2_pub)
        tracker.min_fee_rate = 0
        tracker.record_share("primary", primary_address, difficulty=1.0)
        tracker.record_share("worker-2", worker2_address, difficulty=2.0)
        coinbase_tx = fork_blocks[-1].transactions[0]
        reward = coinbase_tx.outputs[1].value
        txid = coinbase_tx.txid()
        tracker.record_block(height=matured_height, coinbase_txid=txid, reward=reward, vout=1)
        tracker.process_maturity(best_height=height)
        self.assertTrue(tracker.matured_utxos)
        entry = tracker.matured_utxos[0]
        self.assertEqual(entry.get("vout"), 1)
        payees = tracker._gather_payees(16)
        self.assertTrue(payees)
        utxo_record = self.primary.state_db.get_utxo(txid, 1)
        self.assertIsNotNone(utxo_record)
        payout_tx = tracker.create_payout_transaction(self.primary.state_db)
        pool_fee = int(reward * (tracker.pool_fee_percent / 100.0))
        distributable = reward - pool_fee
        expected_worker1 = int(distributable * (1.0 / 3.0))
        expected_worker2 = int(distributable * (2.0 / 3.0))
        expected = {
            script_from_address(primary_address): expected_worker1,
            script_from_address(worker2_address): expected_worker2,
        }
        self._assert_payout_distribution(payout_tx, expected, pool_fee)

    def test_payout_tracker_cleans_matured_utxo_on_reorg(self) -> None:
        base_blocks: list[Block] = []
        for _ in range(2):
            block = self.primary.mine_block()
            raw = block.serialize()
            self.primary.chain.add_block(block, raw)
            self.fork.chain.add_block(block, raw)
            base_blocks.append(block)
        parent_hash = base_blocks[-1].block_hash()
        matured_branch: list[Block] = []
        for _ in range(3):
            block = self.primary.mine_block(parent_hash=parent_hash)
            matured_branch.append(block)
            self.primary.chain.add_block(block, block.serialize())
            self.fork.chain.add_block(block, block.serialize())
            self._wait_for_header(self.primary, block.block_hash())
            parent_hash = block.block_hash()
        matured_hash = matured_branch[-1].block_hash()
        matured_header = self.primary.state_db.get_header(matured_hash)
        assert matured_header is not None
        matured_height = matured_header.height
        extension_block = self.primary.mine_block(parent_hash=matured_hash)
        self.primary.chain.add_block(extension_block, extension_block.serialize())
        self.fork.chain.add_block(extension_block, extension_block.serialize())
        self._wait_for_header(self.primary, extension_block.block_hash())
        best_height = self._wait_for_best_tip(self.primary, extension_block.block_hash())
        tracker = self._build_tracker("payout-reorg-mature-removal.json")
        primary_address = crypto.address_from_pubkey(self.primary.pubkey)
        worker2_address = crypto.address_from_pubkey(crypto.generate_pubkey(7))
        tracker.min_fee_rate = 0
        tracker.record_share("primary", primary_address, difficulty=1.0)
        tracker.record_share("worker-2", worker2_address, difficulty=2.0)
        coinbase_tx = matured_branch[-1].transactions[0]
        reward = coinbase_tx.outputs[1].value
        txid = coinbase_tx.txid()
        tracker.record_block(height=matured_height, coinbase_txid=txid, reward=reward, vout=1)
        tracker.process_maturity(best_height=best_height)
        self.assertTrue(tracker.matured_utxos)
        self.assertIsNotNone(self.primary.state_db.get_utxo(txid, 1))

        reorg_parent_hash = base_blocks[0].block_hash()
        reorg_parent_header = self.primary.state_db.get_header(reorg_parent_hash)
        assert reorg_parent_header is not None
        reorg_branch_parent = reorg_parent_hash
        blocks_needed = best_height - reorg_parent_header.height + 2
        reorg_blocks: list[Block] = []
        # Extend a branch from the earlier base block so the matured block is orphaned.
        for _ in range(blocks_needed):
            block = self.fork.mine_block(parent_hash=reorg_branch_parent)
            raw = block.serialize()
            self.fork.chain.add_block(block, raw)
            self.primary.chain.add_block(block, raw)
            reorg_blocks.append(block)
            reorg_branch_parent = block.block_hash()
        self._wait_for_header(self.primary, reorg_blocks[-1].block_hash())
        new_height = self._wait_for_best_tip(self.primary, reorg_blocks[-1].block_hash())
        self.assertGreater(new_height, best_height)
        self.assertIsNone(self.primary.state_db.get_utxo(txid, 1))

        payout_tx = tracker.create_payout_transaction(self.primary.state_db)
        self.assertIsNone(payout_tx)
        self.assertFalse(tracker.matured_utxos)
