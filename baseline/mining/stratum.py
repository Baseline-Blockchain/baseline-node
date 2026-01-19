"""
Async Stratum server that hands out block templates and validates miner shares.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import json
import logging
import os
import time
from collections import OrderedDict, deque
from collections.abc import Sequence
from dataclasses import dataclass

from ..config import NodeConfig
from ..core.address import script_from_address
from ..core.block import Block
from ..core.chain import MAX_FUTURE_BLOCK_TIME, Chain, ChainError
from ..mempool import Mempool
from ..net.server import P2PServer
from ..storage import StateDB
from ..time_sync import synchronized_time_int
from .payout import PayoutTracker
from .templates import Template, TemplateBuilder

MAX_MESSAGE_BYTES = 4_096
MAX_SHARE_ERRORS = 8
JOB_EXPIRY = 15 * 60
EXPECTED_SOCKET_ERRNOS = {errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED}
EXPECTED_WINERRORS = {64, 995}
REQUIRED_USER_AGENT = "baseline-miner/0.1"
TARGET_SHARE_INTERVAL = 15.0  # seconds per accepted share
VARDIFF_TOLERANCE = 0.25  # +/-25% before retuning
MIN_VARDIFF_INTERVAL = 30.0  # seconds between difficulty changes
MAX_VARDIFF_STEP = 2.0
MIN_VARDIFF_STEP = 0.5
MIN_DIFF_FLOOR = 0.01  # Absolute floor to avoid absurdly tiny share targets
MAX_VARDIFF_SAMPLES = 4096  # Cap share samples per session to bound memory/CPU
SNAPSHOT_TIMEOUT = 2.0
JOB_SEND_TIMEOUT = 2.0


@dataclass(slots=True)
class TemplateJob:
    job_id: str
    template: Template
    created: float
    clean: bool


class StratumSession:
    def __init__(self, server: StratumServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.server = server
        self.reader = reader
        self.writer = writer
        self.log = logging.getLogger("baseline.stratum.session")
        self.address = writer.get_extra_info("peername")
        self.session_id = server.next_session_id()
        self.worker_id: str | None = None
        self.worker_address: str | None = None
        self.authorized = False
        self.subscribed = False
        self.difficulty = server._clamp_difficulty(server.config.stratum.min_difficulty)
        self.share_target = server.difficulty_to_target(self.difficulty)
        self.extranonce1 = os.urandom(server.builder.extranonce1_size)
        self.last_activity = time.time()
        self.invalid_shares = 0
        self.stale_shares = 0
        self.closed = False
        self.sent_jobs: dict[str, set[str]] = {}
        self.vardiff_window = max(10.0, float(server.config.stratum.vardiff_window))
        self.share_times: deque[float] = deque(maxlen=MAX_VARDIFF_SAMPLES)
        # Allow an immediate first vardiff adjustment once enough shares are observed.
        self.last_diff_update = 0.0
        self.user_agent: str | None = None

    async def run(self) -> None:
        try:
            while not self.closed:
                line = await self.reader.readline()
                if not line:
                    break
                self.last_activity = time.time()
                if len(line) > MAX_MESSAGE_BYTES:
                    await self.send_error(None, -1, "message too large")
                    break
                try:
                    message = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    await self.send_error(None, -32700, "invalid json")
                    continue
                await self.server.handle_message(self, message)
        except Exception as exc:
            if _is_connection_reset_error(exc):
                self.log.debug("Stratum session %s disconnected: %s", self.session_id, exc)
            else:
                self.log.exception("Stratum session %s failed", self.session_id)
        finally:
            await self.close()

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception as exc:  # noqa: BLE001
            if _is_connection_reset_error(exc):
                self.server.log.debug("Stratum session %s closed by peer: %s", self.session_id, exc)
            else:
                self.server.log.exception("Stratum session close failed")

    async def send_response(self, msg_id: int | str | None, result: object) -> None:
        payload = {"id": msg_id, "result": result, "error": None}
        await self._send(payload)

    async def send_error(self, msg_id: int | str | None, code: int, message: str) -> None:
        payload = {"id": msg_id, "result": None, "error": [code, message]}
        await self._send(payload)

    async def send_notification(self, method: str, params: Sequence[object]) -> None:
        payload = {"id": None, "method": method, "params": list(params)}
        await self._send(payload)

    async def _send(self, payload: dict[str, object]) -> None:
        if self.closed:
            return
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n"
        self.writer.write(data)
        await self.writer.drain()

    async def send_job(self, job: TemplateJob, clean: bool) -> None:
        if not self.authorized:
            return
        
        prev_hash_hex = job.template.prev_hash
        merkle_branches = [branch[::-1].hex() for branch in job.template.merkle_branches]

        params = [
            job.job_id,
            prev_hash_hex,
            job.template.coinb1.hex(),
            job.template.coinb2.hex(),
            merkle_branches,
            f"{job.template.version:08x}",
            f"{job.template.bits:08x}",
            f"{job.template.timestamp:08x}",
            clean,
        ]
        await self.send_notification("mining.notify", params)
        self.sent_jobs.setdefault(job.job_id, set())
        self._prune_old_jobs(self.server.active_job_ids())

    def record_submission(self, job_id: str, extranonce2: str, ntime: str, nonce: str) -> bool:
        bucket = self.sent_jobs.setdefault(job_id, set())
        combo = f"{extranonce2}:{ntime}:{nonce}"
        if combo in bucket:
            return False
        bucket.add(combo)
        if len(bucket) > 1000:
            # keep memory bounded
            oldest = next(iter(bucket))
            bucket.discard(oldest)
        return True

    def set_difficulty(self, value: float) -> None:
        self.difficulty = self.server._clamp_difficulty(value)
        self.share_target = self.server.difficulty_to_target(self.difficulty)

    def _prune_old_jobs(self, valid_ids: Sequence[str]) -> None:
        valid = set(valid_ids)
        for job_id in list(self.sent_jobs):
            if job_id not in valid:
                self.sent_jobs.pop(job_id, None)


class StratumServer:
    def __init__(
        self,
        config: NodeConfig,
        chain: Chain,
        mempool: Mempool,
        builder: TemplateBuilder,
        payout_tracker: PayoutTracker,
        *,
        network: P2PServer | None = None,
    ):
        self.config = config
        self.chain = chain
        self.state_db: StateDB = chain.state_db
        self.mempool = mempool
        self.builder = builder
        self.payouts = payout_tracker
        self.network = network
        self.log = logging.getLogger("baseline.stratum")
        self.server: asyncio.AbstractServer | None = None
        self.sessions: dict[int, StratumSession] = {}
        self._session_seq = 0
        self._jobs: OrderedDict[str, TemplateJob] = OrderedDict()
        self._job_seq = 0
        self._latest_job: TemplateJob | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = asyncio.Event()
        self._template_event = asyncio.Event()
        self._need_clean = True
        self._tasks: list[asyncio.Task] = []
        self._chain_tip: tuple[str, int] | None = None
        self._last_template_time = 0.0
        self.global_shares: dict[int, float] = {}
        self.mempool.register_listener(self._on_mempool_tx)
        
        # Dashboard Cache
        self._stats_cache: dict[str, object] = {
            "sessions": 0,
            "host": config.stratum.host,
            "port": config.stratum.port,
            "min_difficulty": config.stratum.min_difficulty,
            "last_template_time": None,
            "last_template_age": None,
            "pool_hashrate": 0.0,
        }
        self._sessions_cache: list[dict[str, object]] = []

    def next_session_id(self) -> int:
        self._session_seq += 1
        return self._session_seq

    def _call_on_loop(self, fn, default):
        loop = self._loop
        if not loop:
            try:
                return fn()
            except Exception:
                return default
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            return fn()

        async def _runner():
            return fn()

        try:
            future = asyncio.run_coroutine_threadsafe(_runner(), loop)
            return future.result(timeout=SNAPSHOT_TIMEOUT)
        except Exception:
            self.log.debug("Failed to capture stratum snapshot", exc_info=True)
            return default

    def difficulty_to_target(self, difficulty_value: float) -> int:
        difficulty_value = self._clamp_difficulty(difficulty_value)
        base = self.chain.max_target
        target = int(base / difficulty_value)
        return max(target, 1)

    def _clamp_difficulty(self, value: float) -> float:
        floor = max(self.config.stratum.min_difficulty, MIN_DIFF_FLOOR)
        if value <= 0:
            return floor
        return max(floor, value)

    @staticmethod
    def _hash_to_int(block_hash: str) -> int:
        hash_bytes = bytes.fromhex(block_hash)
        return int.from_bytes(hash_bytes[::-1], "big")

    async def start(self) -> None:
        if self.server:
            return
        self._loop = asyncio.get_running_loop()
        host = self.config.stratum.host
        port = self.config.stratum.port
        self.server = await asyncio.start_server(self._handle_client, host, port)
        self.log.info("Stratum listening on %s:%s", host, port)
        self._stop_event.clear()
        self._chain_tip = self.state_db.get_best_tip()
        self._tasks = [
            asyncio.create_task(self._template_loop(), name="stratum-templates"),
            asyncio.create_task(self._tip_monitor_loop(), name="stratum-tip"),
            asyncio.create_task(self._session_gc_loop(), name="stratum-gc"),
            asyncio.create_task(self._payout_flush_loop(), name="stratum-flush"),
            asyncio.create_task(self._stats_updater(), name="stratum-stats"),
        ]
        self._need_clean = True
        self._template_event.set()

    async def stop(self) -> None:
        if not self.server:
            return
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        
        # 1. Stop accepting new connections
        self.server.close()
        await self.server.wait_closed()
        self.server = None

        # 2. Close active sessions in parallel
        # We copy values() to a list because closing them might trigger 
        # the session execution loop to remove them from the dict.
        if self.sessions:
            close_tasks = [asyncio.create_task(s.close()) for s in list(self.sessions.values())]
            await asyncio.gather(*close_tasks, return_exceptions=True)
        self.sessions.clear()

        # 3. Final flush on shutdown (after all shares are processed)
        await asyncio.to_thread(self.payouts.flush)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session = StratumSession(self, reader, writer)
        self.sessions[session.session_id] = session
        try:
            await session.run()
        finally:
            self.sessions.pop(session.session_id, None)

    async def handle_message(self, session: StratumSession, message: dict[str, object]) -> None:
        msg_id = message.get("id")
        method = message.get("method")
        params = message.get("params", [])
        if method == "mining.subscribe":
            await self._handle_subscribe(session, msg_id, params)
        elif method == "mining.authorize":
            await self._handle_authorize(session, msg_id, params)
        elif method == "mining.submit":
            await self._handle_submit(session, msg_id, params)
        elif method == "mining.extranonce.subscribe":
            await session.send_response(msg_id, True)
        else:
            await session.send_error(msg_id, -32601, "unknown method")

    async def _handle_subscribe(self, session: StratumSession, msg_id: object, params: Sequence[object]) -> None:
        if session.subscribed:
            await session.send_error(msg_id, 24, "already subscribed")
            return
        user_agent = str(params[0]) if params else ""
        # We allow any user agent now, but we use it to determine behavior
        session.user_agent = user_agent
        
        result = [
            [["mining.set_difficulty", str(session.session_id)], ["mining.notify", str(session.session_id)]],
            session.extranonce1.hex(),
            self.builder.extranonce2_size,
        ]
        session.subscribed = True
        await session.send_response(msg_id, result)

    async def _handle_authorize(self, session: StratumSession, msg_id: object, params: Sequence[object]) -> None:
        if not session.subscribed:
            await session.send_error(msg_id, 25, "subscribe first")
            return
        if len(params) < 1:
            await session.send_error(msg_id, 26, "missing worker id")
            return
        worker_name = str(params[0])
        password = str(params[1]) if len(params) > 1 else ""
        address = self._extract_address(worker_name, password)
        if address is None:
            await session.send_response(msg_id, False)
            return
        
        # Namespace collision prevention:
        # If the worker name doesn't contain the address, prepend it.
        # This allows "Worker1" on AddrA and "Worker1" on AddrB to be distinct.
        if address and not worker_name.startswith(address):
             unique_id = f"{address}.{worker_name}"
        else:
             unique_id = worker_name

        with self.payouts.lock:
            existing = self.payouts.workers.get(unique_id)
            if existing and existing.address and existing.address != address:
                # This should technically be impossible now pending hash collisions
                # or if the user found a way to spoof the prefix without having the address
                await session.send_response(msg_id, False)
                return
        session.worker_id = unique_id
        session.worker_address = address
        session.authorized = True
        self.payouts.register_worker(unique_id, address)
        await session.send_response(msg_id, True)
        await session.send_notification("mining.set_difficulty", [session.difficulty])
        if self._latest_job:
            await session.send_job(self._latest_job, clean=True)
        else:
            self._template_event.set()

    async def _handle_submit(self, session: StratumSession, msg_id: object, params: Sequence[object]) -> None:
        if not (session.subscribed and session.authorized):
            await session.send_error(msg_id, 27, "unauthorized")
            return
        if len(params) < 5:
            await session.send_error(msg_id, 28, "invalid params")
            return
        worker_name = str(params[0])
        job_id = str(params[1])
        extranonce2_hex = str(params[2])
        ntime_hex = str(params[3])
        nonce_hex = str(params[4])
        job = self._jobs.get(job_id)
        if job is None:
            await session.send_error(msg_id, 29, "stale share")
            session.stale_shares += 1
            return
        if worker_name != session.worker_id:
            await session.send_error(msg_id, 30, "unknown worker")
            return
        if len(extranonce2_hex) != self.builder.extranonce2_size * 2:
            await session.send_error(msg_id, 31, "invalid extranonce2 size")
            return
        if not session.record_submission(job_id, extranonce2_hex, ntime_hex, nonce_hex):
            await session.send_error(msg_id, 32, "duplicate share")
            session.invalid_shares += 1
            await self._maybe_disconnect(session)
            return
        try:
            extranonce1 = session.extranonce1
            extranonce2 = bytes.fromhex(extranonce2_hex)
            ntime = int(ntime_hex, 16)
            nonce = int(nonce_hex, 16)
        except ValueError:
            await session.send_error(msg_id, 33, "malformed fields")
            session.invalid_shares += 1
            await self._maybe_disconnect(session)
            return
        if nonce > 0xFFFFFFFF:
            await session.send_error(msg_id, 34, "nonce out of range")
            session.invalid_shares += 1
            await self._maybe_disconnect(session)
            return
        if self._chain_tip and job.template.prev_hash != self._chain_tip[0]:
            await session.send_error(msg_id, 29, "stale share")
            session.stale_shares += 1
            return
        now = synchronized_time_int()
        if ntime < job.template.timestamp - 600 or ntime > now + MAX_FUTURE_BLOCK_TIME:
            await session.send_error(msg_id, 35, "ntime out of range")
            session.invalid_shares += 1
            await self._maybe_disconnect(session)
            return
        try:
            block = self.builder.assemble_block(job.template, extranonce1, extranonce2, nonce, ntime)
        except Exception as exc:
            self.log.warning("Failed to assemble block for share: %s", exc)
            await session.send_error(msg_id, 36, "assembly error")
            session.invalid_shares += 1
            await self._maybe_disconnect(session)
            return
        block_hash = block.block_hash()
        hash_int = self._hash_to_int(block_hash)
        if hash_int > session.share_target:
            await session.send_error(msg_id, 37, "low difficulty share")
            session.invalid_shares += 1
            await self._maybe_disconnect(session)
            return
        await session.send_response(msg_id, True)
        self.payouts.record_share(session.worker_id or worker_name, session.worker_address or "", session.difficulty)
        await self._record_share_success(session)
        if hash_int <= job.template.target:
            await self._submit_block(block, job)

    def estimate_pool_hashrate(self, window: float = 600.0) -> float:
        """Estimate hashrate over the last `window` seconds (H/s)."""
        return self._call_on_loop(lambda: self._estimate_pool_hashrate(window), 0.0)

    def _estimate_pool_hashrate(self, window: float = 600.0) -> float:
        """Estimate hashrate over the last `window` seconds (H/s)."""
        now = int(time.time())
        cutoff = now - int(window)
        total_work = 0.0
        earliest = now
        
        to_del = []
        found_samples = False
        
        for ts, diff in self.global_shares.items():
            if ts < cutoff:
                to_del.append(ts)
                continue
            
            total_work += diff
            if ts < earliest:
                earliest = ts
            found_samples = True
            
        for ts in to_del:
            self.global_shares.pop(ts, None)
            
        if not found_samples:
            return 0.0
            
        duration = max(1, now - earliest)
    
        hashes = total_work * 2.0
        return hashes / duration

    async def _stats_updater(self) -> None:
        """Periodically update dashboard stats in background to avoid blocking renders."""
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(1.0)
                
                # Update Stats
                now = time.time()
                last_tpl = self._last_template_time
                self._stats_cache = {
                    "sessions": len(self.sessions),
                    "host": self.config.stratum.host,
                    "port": self.config.stratum.port,
                    "min_difficulty": self.config.stratum.min_difficulty,
                    "last_template_time": last_tpl,
                    "last_template_age": max(0.0, now - last_tpl) if last_tpl else None,
                    "pool_hashrate": self._estimate_pool_hashrate(600.0),
                }

                # Update Sessions
                sessions_list = []
                for session in list(self.sessions.values()):
                    rate = 0.0
                    if len(session.share_times) >= 2:
                        elapsed = session.share_times[-1] - session.share_times[0]
                        if elapsed > 0:
                            rate = (len(session.share_times) - 1) / elapsed
                    sessions_list.append(
                        {
                            "session_id": session.session_id,
                            "worker_id": session.worker_id,
                            "address": session.worker_address,
                            "difficulty": session.difficulty,
                            "last_activity": session.last_activity,
                            "idle_seconds": max(0.0, now - session.last_activity),
                            "stale_shares": session.stale_shares,
                            "invalid_shares": session.invalid_shares,
                            "authorized": session.authorized,
                            "subscribed": session.subscribed,
                            "share_rate_per_min": rate * 60 if rate else 0.0,
                            "remote": session.address,
                        }
                    )
                self._sessions_cache = sessions_list
        except asyncio.CancelledError:
            pass

    def snapshot_stats(self) -> dict[str, object]:
        return self._stats_cache

    def snapshot_sessions(self) -> list[dict[str, object]]:
        return self._sessions_cache



    async def _record_share_success(self, session: StratumSession) -> None:
        now = time.time()
        session.share_times.append(now)
        
        ts_int = int(now)
        self.global_shares[ts_int] = self.global_shares.get(ts_int, 0.0) + session.difficulty
        
        if len(self.global_shares) > 3600:
            self.global_shares.pop(next(iter(self.global_shares)), None)
        
        await self._maybe_adjust_difficulty(session)

    async def _maybe_adjust_difficulty(self, session: StratumSession) -> None:
        now = time.time()
        cutoff = now - session.vardiff_window
        while session.share_times and session.share_times[0] < cutoff:
            session.share_times.popleft()
        if len(session.share_times) < 4:
            return
        # Allow a fast first adjustment; afterwards enforce a minimum interval.
        if session.last_diff_update and (now - session.last_diff_update) < MIN_VARDIFF_INTERVAL:
            return
        elapsed = session.share_times[-1] - session.share_times[0]
        if elapsed <= 0:
            return
        avg_interval = elapsed / (len(session.share_times) - 1)
        target = TARGET_SHARE_INTERVAL
        if abs(avg_interval - target) <= target * VARDIFF_TOLERANCE:
            return
        ratio = target / avg_interval
        ratio = max(MIN_VARDIFF_STEP, min(MAX_VARDIFF_STEP, ratio))
        current_diff = session.difficulty or self.config.stratum.min_difficulty
        new_diff = max(self.config.stratum.min_difficulty, current_diff * ratio)
        if abs(new_diff - current_diff) / current_diff < 0.05:
            return
        session.set_difficulty(new_diff)
        session.last_diff_update = now
        session.share_times.clear()
        try:
            await session.send_notification("mining.set_difficulty", [session.difficulty])
            if self._latest_job:
                await session.send_job(self._latest_job, clean=False)
        except Exception:
            self.log.debug("Failed to send vardiff update to session %s", session.session_id, exc_info=True)

    async def _template_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                await self._template_event.wait()
                self._template_event.clear()
                clean = self._need_clean
                self._need_clean = False
                try:
                    template = await asyncio.to_thread(self.builder.build_template)
                except Exception:
                    self.log.exception("Failed to build template")
                    await asyncio.sleep(1)
                    self._template_event.set()
                    continue
                job_id = f"{int(time.time())}-{self._job_seq}"
                self._job_seq += 1
                job = TemplateJob(job_id=job_id, template=template, created=time.time(), clean=clean)
                self._jobs[job_id] = job
                self._latest_job = job
                self._trim_jobs()
                await self._broadcast_job(job, clean)
                self._last_template_time = time.time()
        except asyncio.CancelledError:
            pass

    async def _send_job_safe(self, session: StratumSession, job: TemplateJob, clean: bool) -> bool:
        try:
            await asyncio.wait_for(session.send_job(job, clean), timeout=JOB_SEND_TIMEOUT)
            return True
        except Exception:
            return False

    async def _broadcast_job(self, job: TemplateJob, clean: bool) -> None:
        session_ids: list[int] = []
        tasks: list[asyncio.Task] = []
        for session_id, session in list(self.sessions.items()):
            if session.closed or not session.authorized:
                continue
            session_ids.append(session_id)
            tasks.append(asyncio.create_task(self._send_job_safe(session, job, clean)))
        if not tasks:
            return
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for session_id, result in zip(session_ids, results):
            ok = isinstance(result, bool) and result
            if not ok:
                session = self.sessions.pop(session_id, None)
                if session:
                    await session.close()

    def _trim_jobs(self) -> None:
        while len(self._jobs) > self.config.stratum.max_jobs:
            job_id, _ = self._jobs.popitem(last=False)
            for session in self.sessions.values():
                session._prune_old_jobs(self._jobs.keys())
        cutoff = time.time() - JOB_EXPIRY
        for job_id in list(self._jobs.keys()):
            if self._jobs[job_id].created < cutoff:
                self._jobs.pop(job_id, None)
        for session in self.sessions.values():
            session._prune_old_jobs(self._jobs.keys())

    def active_job_ids(self) -> list[str]:
        return list(self._jobs.keys())

    async def _tip_monitor_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                best = self.state_db.get_best_tip()
                if best and (self._chain_tip is None or best[0] != self._chain_tip[0]):
                    self._chain_tip = best
                    self._need_clean = True
                    self._template_event.set()
                elif not best:
                    self._chain_tip = None
                else:
                    self._chain_tip = best
                # refresh template timestamp periodically
                if time.time() - self._last_template_time > 30:
                    self._template_event.set()
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

    async def _session_gc_loop(self) -> None:
        timeout = self.config.stratum.session_timeout
        try:
            while not self._stop_event.is_set():
                now = time.time()
                for session_id, session in list(self.sessions.items()):
                    if now - session.last_activity > timeout:
                        await session.close()
                        self.sessions.pop(session_id, None)
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass

    async def _payout_flush_loop(self) -> None:
        """Periodically flush payout state to disk to reduce I/O contention."""
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(5)
                # Run flush() in a thread to avoid blocking the event loop on disk I/O
                await asyncio.to_thread(self.payouts.flush)
        except asyncio.CancelledError:
            pass


    async def _submit_block(self, block: Block, job: TemplateJob) -> None:
        def _add_block() -> dict[str, object]:
            try:
                return self.chain.add_block(block)
            except ChainError as exc:
                return {"status": "error", "error": str(exc)}

        result = await asyncio.to_thread(_add_block)
        status = result.get("status")
        accepted = {"connected", "reorganized", "deferred"}
        if status in accepted:
            # "deferred" is returned when fork reorgs are rate-limited; still
            # count the block for payouts so miner rewards are not lost.
            if status != "deferred" and self.mempool:
                try:
                    self.mempool.remove_confirmed(block.transactions)
                except Exception as exc:
                    self.log.debug("Failed to prune mempool after block submission: %s", exc)
            coinbase_tx = block.transactions[0]
            coinbase_txid = coinbase_tx.txid()
            self.payouts.record_block(
                job.template.height, coinbase_txid, job.template.miner_reward, job.template.pool_vout
            )
            if self.network:
                self.network.announce_block(block.block_hash())
            self.log.info(
                "Block found at height %s hash=%s (%s)", job.template.height, block.block_hash(), status
            )
        elif status in {"rejected", "duplicate", "side"}:
            self.log.debug("Submitted block %s rejected (%s)", block.block_hash(), status)
        else:
            error_text = result.get("error")
            if error_text:
                self.log.info("Submitted block %s rejected (%s: %s)", block.block_hash(), status, error_text)
            else:
                self.log.info("Submitted block %s rejected (%s)", block.block_hash(), status)

    def _extract_address(self, worker_name: str, password: str) -> str | None:
        candidates: list[str] = []
        if password:
            candidates.append(password.strip())
        if worker_name:
            if "." in worker_name:
                candidates.append(worker_name.split(".", 1)[0])
            elif ":" in worker_name:
                candidates.append(worker_name.split(":", 1)[0])
            else:
                candidates.append(worker_name)
        for candidate in candidates:
            sanitized = candidate.strip()
            if not sanitized:
                continue
            try:
                script_from_address(sanitized)
            except Exception:  # noqa: BLE001
                continue
            return sanitized
        return None

    def _on_mempool_tx(self, _tx) -> None:
        if not self._loop:
            return
        self._loop.call_soon_threadsafe(self._request_template_update, False)

    def _request_template_update(self, clean: bool) -> None:
        if clean:
            self._need_clean = True
        self._template_event.set()

    async def _maybe_disconnect(self, session: StratumSession) -> None:
        if session.invalid_shares >= MAX_SHARE_ERRORS:
            self.log.warning("Disconnecting %s due to invalid shares", session.session_id)
            await session.close()
            self.sessions.pop(session.session_id, None)
def _is_connection_reset_error(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError):
        if exc.errno in EXPECTED_SOCKET_ERRNOS:
            return True
        winerror = getattr(exc, "winerror", None)
        if winerror in EXPECTED_WINERRORS:
            return True
    return False
