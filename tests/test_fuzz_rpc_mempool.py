import random
import tempfile
import unittest
from pathlib import Path

from baseline.core import crypto
from baseline.core.tx import COIN, Transaction, TxInput, TxOutput, TxSerializationError
from baseline.mempool import Mempool, MempoolError
from baseline.net.security import MessageValidator
from baseline.storage import BlockStore, StateDB, UTXORecord
from baseline.config import NodeConfig
from baseline.core.chain import Chain, GENESIS_PRIVKEY, GENESIS_PUBKEY
from baseline.policy import MIN_RELAY_FEE_RATE


class RpcMempoolFuzzTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)
        config = NodeConfig()
        config.data_dir = data_dir
        config.mining.allow_consensus_overrides = True
        config.mining.foundation_address = crypto.address_from_pubkey(GENESIS_PUBKEY)
        config.mining.coinbase_maturity = 1
        config.ensure_data_layout()
        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)
        self.script_pubkey = b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac"
        self._prepare_utxo()

    def tearDown(self) -> None:
        self.state_db.close()
        self.tmpdir.cleanup()
        self.mempool.close()

    def _prepare_utxo(self) -> None:
        txid = self.chain.genesis_block.transactions[0].txid()
        record = UTXORecord(
            txid=txid,
            vout=0,
            amount=COIN,
            script_pubkey=self.script_pubkey,
            height=0,
            coinbase=True,
        )
        self.state_db.add_utxo(record)

    def _build_valid_tx(self) -> Transaction:
        txid = self.chain.genesis_block.transactions[0].txid()
        tx = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=txid, prev_vout=0, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=COIN - MIN_RELAY_FEE_RATE, script_pubkey=self.script_pubkey)],
            lock_time=0,
        )
        sighash = tx.signature_hash(0, self.script_pubkey, 0x01)
        signature = crypto.sign(sighash, GENESIS_PRIVKEY) + b"\x01"
        script_sig = len(signature).to_bytes(1, "little") + signature + len(GENESIS_PUBKEY).to_bytes(1, "little") + GENESIS_PUBKEY
        tx.inputs[0].script_sig = script_sig
        return tx

    def test_rpc_message_validator_fuzz(self) -> None:
        msg = {
            "type": "tx",
            "transaction": {
                "version": 1,
                "inputs": [{"prev_txid": self.chain.genesis_block.transactions[0].txid(), "prev_vout": 0}],
                "outputs": [{"value": MIN_RELAY_FEE_RATE, "script_pubkey": "00"}],
                "lock_time": 0,
            },
        }
        for seed in range(200):
            random.seed(seed)
            fuzzed = dict(msg)
            tx = dict(msg["transaction"])
            if random.random() < 0.5:
                tx["lock_time"] = random.randint(-5, 0x1_0000_0000)
            if random.random() < 0.5:
                tx.pop("outputs", None)
            if random.random() < 0.5:
                tx["outputs"] = []
            fuzzed["transaction"] = tx
            try:
                valid, _ = MessageValidator.validate_message(fuzzed)
            except Exception:
                self.fail("MessageValidator raised unexpectedly")
            self.assertIn(valid, (True, False))

    def test_mempool_tx_fuzz(self) -> None:
        base = self._build_valid_tx()
        raw = bytearray(base.serialize())
        for seed in range(200):
            random.seed(seed)
            mutated = bytearray(raw)
            for _ in range(random.randint(1, 8)):
                idx = random.randrange(len(mutated))
                mutated[idx] = random.randrange(256)
            try:
                tx = Transaction.parse(bytes(mutated))
            except TxSerializationError:
                continue
            try:
                self.mempool.accept_transaction(tx, peer_id="fuzz")
            except MempoolError:
                continue
            finally:
                if self.mempool.contains(tx.txid()):
                    self.mempool.drop_transaction(tx.txid())
