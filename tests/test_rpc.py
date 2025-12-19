import tempfile
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.address import script_from_address
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import GENESIS_PRIVKEY, GENESIS_PUBKEY, Chain
from baseline.core.tx import COIN, Transaction, TxInput, TxOutput
from baseline.mempool import Mempool
from baseline.mining.templates import TemplateBuilder
from baseline.policy import MIN_RELAY_FEE_RATE
from baseline.rpc.handlers import RPCError, RPCHandlers
from baseline.storage import BlockStore, StateDB
from baseline.wallet import WalletManager


class DummyNetwork:
    def __init__(self):
        self.peers = {}


class RPCTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)
        self.config = NodeConfig()
        self.config.data_dir = data_dir
        self.config.mining.allow_consensus_overrides = True
        self.config.mining.coinbase_maturity = 1
        self.config.ensure_data_layout()
        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)
        pool_pub = crypto.generate_pubkey(2)
        payout_script = b"\x76\xa9\x14" + crypto.hash160(pool_pub) + b"\x88\xac"
        self.template_builder = TemplateBuilder(self.chain, self.mempool, payout_script)
        wallet_path = data_dir / "wallet" / "wallet.json"
        self.wallet = WalletManager(wallet_path, self.state_db, self.block_store, self.mempool)
        self.handlers = RPCHandlers(
            self.chain,
            self.mempool,
            self.block_store,
            self.template_builder,
            DummyNetwork(),
            self.wallet,
        )

    def tearDown(self) -> None:
        self.state_db.close()
        self.tmpdir.cleanup()

    def _build_spend_tx(self) -> Transaction:
        genesis_txid = self.chain.genesis_block.transactions[0].txid()
        script_pubkey = b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac"
        tx = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=genesis_txid, prev_vout=0, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[
                TxOutput(value=50 * COIN - MIN_RELAY_FEE_RATE, script_pubkey=script_pubkey)
            ],
            lock_time=0,
        )
        sighash = tx.signature_hash(0, script_pubkey, 0x01)
        signature = crypto.sign(sighash, GENESIS_PRIVKEY) + b"\x01"
        script_sig = bytes([len(signature)]) + signature + bytes([len(GENESIS_PUBKEY)]) + GENESIS_PUBKEY
        tx.inputs[0].script_sig = script_sig
        return tx

    def _build_payment_tx(self, address: str, value: int) -> Transaction:
        dest_script = script_from_address(address)
        tx = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=self.chain.genesis_block.transactions[0].txid(), prev_vout=0, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=value, script_pubkey=dest_script)],
            lock_time=0,
        )
        source_script = b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac"
        sighash = tx.signature_hash(0, source_script, 0x01)
        signature = crypto.sign(sighash, GENESIS_PRIVKEY) + b"\x01"
        script_sig = bytes([len(signature)]) + signature + bytes([len(GENESIS_PUBKEY)]) + GENESIS_PUBKEY
        tx.inputs[0].script_sig = script_sig
        tx.validate_basic()
        return tx

    def _make_coinbase(self, height: int) -> Transaction:
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        script_sig = len(height_bytes).to_bytes(1, "little") + height_bytes + b"\x01"
        return Transaction(
            version=1,
            inputs=[TxInput(prev_txid="00" * 32, prev_vout=0xFFFFFFFF, script_sig=script_sig, sequence=0xFFFFFFFF)],
            outputs=[
                TxOutput(value=50 * COIN, script_pubkey=b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac")
            ],
            lock_time=0,
        )

    def _solve_block(self, prev_hash: str, transactions: list[Transaction]) -> Block:
        parent_header = self.state_db.get_header(prev_hash)
        height = (parent_header.height + 1) if parent_header else 1
        coinbase = self._make_coinbase(height)
        block_txs = [coinbase] + transactions
        base_time = parent_header.timestamp if parent_header else self.chain.genesis_block.header.timestamp
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash(block_txs),
            timestamp=base_time + self.config.mining.block_interval_target,
            bits=self.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=block_txs)
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
        return block

    def _mine_block(self, prev_hash: str, transactions: list[Transaction]) -> Block:
        block = self._solve_block(prev_hash, transactions)
        res = self.chain.add_block(block, block.serialize())
        self.assertIn(res["status"], {"connected", "reorganized"})
        return block

    def test_getblockhash(self) -> None:
        result = self.handlers.dispatch("getblockhash", [0])
        self.assertEqual(result, self.chain.genesis_hash)

    def test_getblocktemplate_structure(self) -> None:
        tpl = self.handlers.dispatch("getblocktemplate", [])
        self.assertIn("coinb1", tpl)
        self.assertIn("coinb2", tpl)
        self.assertEqual(tpl["height"], 1)

    def test_sendrawtransaction_and_getrawtransaction(self) -> None:
        tx = self._build_spend_tx()
        tx_hex = tx.serialize().hex()
        txid = self.handlers.dispatch("sendrawtransaction", [tx_hex])
        self.assertEqual(txid, tx.txid())
        fetched = self.handlers.dispatch("getrawtransaction", [txid, True])
        self.assertEqual(fetched["txid"], txid)
        self.assertEqual(fetched["confirmations"], 0)

    def test_gettxout_finds_mempool_entry(self) -> None:
        tx = self._build_spend_tx()
        self.mempool.accept_transaction(tx, peer_id="test")
        entry = self.handlers.dispatch("gettxout", [tx.txid(), 0, True])
        self.assertIsNotNone(entry)
        self.assertEqual(entry["value"], tx.outputs[0].value)

    def test_getrawtransaction_missing(self) -> None:
        with self.assertRaises(RPCError):
            self.handlers.dispatch("getrawtransaction", ["00" * 32, False])

    def test_wallet_receive_and_balance(self) -> None:
        address = self.handlers.dispatch("getnewaddress", [])
        payment = self._build_payment_tx(address, 10 * COIN)
        self._mine_block(self.chain.genesis_hash, [payment])
        self.wallet.sync_chain()
        balance = self.handlers.dispatch("getbalance", [])
        self.assertAlmostEqual(balance, 10.0, places=8)

    def test_listaddressbalances_via_rpc(self) -> None:
        address = self.handlers.dispatch("getnewaddress", ["bal"])
        payment = self._build_payment_tx(address, 5 * COIN)
        self._mine_block(self.chain.genesis_hash, [payment])
        self.wallet.sync_chain()
        entries = self.handlers.dispatch("listaddressbalances", [])
        entry = next(item for item in entries if item["address"] == address)
        self.assertAlmostEqual(entry["balance"], 5.0, places=8)

    def test_wallet_sendtoaddress_records_transaction(self) -> None:
        recv_address = self.handlers.dispatch("getnewaddress", [])
        payment = self._build_payment_tx(recv_address, 5 * COIN)
        self._mine_block(self.chain.genesis_hash, [payment])
        self.wallet.sync_chain()
        dest_pub = crypto.generate_pubkey(11)
        dest_address = crypto.address_from_pubkey(dest_pub)
        txid = self.handlers.dispatch("sendtoaddress", [dest_address, 1.0])
        tx_info = self.handlers.dispatch("gettransaction", [txid])
        self.assertLess(tx_info["amount"], 0)
        entries = self.handlers.dispatch("listtransactions", ["*", 5, 0, False])
        self.assertTrue(any(item["txid"] == txid for item in entries))

    def test_wallet_encrypt_and_unlock_via_rpc(self) -> None:
        self.handlers.dispatch("getnewaddress", [])
        self.handlers.dispatch("encryptwallet", ["passphrase"])
        with self.assertRaises(RPCError) as ctx:
            self.handlers.dispatch("getnewaddress", [])
        self.assertEqual(ctx.exception.code, -13)
        self.handlers.dispatch("walletpassphrase", ["passphrase", 2])
        addr = self.handlers.dispatch("getnewaddress", [])
        self.assertIsInstance(addr, str)
        self.handlers.dispatch("walletlock", [])
        with self.assertRaises(RPCError):
            self.handlers.dispatch("getnewaddress", [])

    def test_dumpwallet_and_importwallet_rpc(self) -> None:
        self.handlers.dispatch("getnewaddress", [])
        backup = Path(self.tmpdir.name) / "dump.json"
        dumped_path = self.handlers.dispatch("dumpwallet", [str(backup)])
        self.assertEqual(dumped_path, str(backup))
        self.handlers.dispatch("importwallet", [str(backup), False])
        # ensure import preserved ability to derive addresses
        addr = self.handlers.dispatch("getnewaddress", [])
        self.assertIsInstance(addr, str)

    def test_importaddress_creates_watch_only_via_rpc(self) -> None:
        watch_pub = crypto.generate_pubkey(2222)
        watch_addr = crypto.address_from_pubkey(watch_pub)
        res = self.handlers.dispatch("importaddress", [watch_addr, "label", False])
        self.assertEqual(res, "address imported")
        entry = self.wallet.data["addresses"][watch_addr]
        self.assertTrue(entry["watch_only"])

    def test_listaddresses_rpc_returns_wallet_entries(self) -> None:
        addr = self.handlers.dispatch("getnewaddress", ["payout"])
        entries = self.handlers.dispatch("listaddresses", [])
        self.assertTrue(any(entry["address"] == addr and entry["label"] == "payout" for entry in entries))

    def test_walletinfo_reports_state(self) -> None:
        info = self.handlers.dispatch("walletinfo", [])
        self.assertFalse(info["encrypted"])
        self.handlers.dispatch("encryptwallet", ["passphrase"])
        info = self.handlers.dispatch("walletinfo", [])
        self.assertTrue(info["encrypted"])

    def test_address_index_rpcs(self) -> None:
        recv_address = self.handlers.dispatch("getnewaddress", [])
        payment = self._build_payment_tx(recv_address, 5 * COIN)
        self._mine_block(self.chain.genesis_hash, [payment])
        self.wallet.sync_chain()
        utxos = self.handlers.dispatch("getaddressutxos", [{"addresses": [recv_address]}])
        self.assertEqual(len(utxos), 1)
        self.assertEqual(utxos[0]["liners"], 5 * COIN)
        balance = self.handlers.dispatch("getaddressbalance", [{"addresses": [recv_address]}])
        self.assertAlmostEqual(balance["balance"], 5.0)
        self.assertAlmostEqual(balance["received"], 5.0)
        txids = self.handlers.dispatch("getaddresstxids", [{"addresses": [recv_address]}])
        self.assertIn(payment.txid(), txids)

    def test_importprivkey_via_rpc(self) -> None:
        priv = 424242
        priv_bytes = priv.to_bytes(32, "big")
        wif = crypto.base58check_encode(b"\x80" + priv_bytes)
        result = self.handlers.dispatch("importprivkey", [wif, "label", False])
        self.assertEqual(result, "key imported")

    def test_submitblock_accepts_valid_block(self) -> None:
        block = self._solve_block(self.chain.genesis_hash, [])
        raw_hex = block.serialize().hex()
        result = self.handlers.dispatch("submitblock", [raw_hex])
        self.assertEqual(result["status"], "connected")
        self.assertEqual(result["hash"], block.block_hash())

    def test_getblockchaininfo_returns_expected_fields(self) -> None:
        result = self.handlers.dispatch("getblockchaininfo", [])
        expected_fields = [
            "chain", "blocks", "headers", "bestblockhash", "difficulty",
            "time", "mediantime", "verificationprogress", "initialblockdownload",
            "chainwork", "size_on_disk", "pruned", "warnings"
        ]
        for field in expected_fields:
            self.assertIn(field, result)
        self.assertEqual(result["chain"], "main")
        self.assertEqual(result["blocks"], 0)  # Genesis only
        self.assertFalse(result["pruned"])

    def test_getnetworkinfo_returns_expected_fields(self) -> None:
        result = self.handlers.dispatch("getnetworkinfo", [])
        expected_fields = [
            "version", "subversion", "protocolversion", "localservices",
            "localservicesnames", "localrelay", "timeoffset", "connections",
            "networkactive", "networks", "relayfee", "incrementalfee",
            "localaddresses", "warnings"
        ]
        for field in expected_fields:
            self.assertIn(field, result)
        self.assertEqual(result["subversion"], "/Baseline:0.1.0/")
        self.assertTrue(result["networkactive"])
        self.assertIsInstance(result["networks"], list)
