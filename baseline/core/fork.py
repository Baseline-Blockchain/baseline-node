"""
Fork detection and chain reorganization handling.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from ..storage import BlockStore, StateDB
from . import difficulty
from .block import Block, BlockHeader


@dataclass
class ForkInfo:
    """Information about a detected fork."""
    fork_height: int
    main_chain_tip: str
    fork_chain_tip: str
    fork_length: int
    main_chain_work: int
    fork_chain_work: int
    should_reorganize: bool


class OrphanBlockManager:
    """Manage orphan blocks that don't connect to the main chain."""
    
    def __init__(self, max_orphans: int = 1000):
        self.max_orphans = max_orphans
        self.orphans: dict[str, Block] = {}  # block_hash -> block
        self.orphans_by_prev: dict[str, list[str]] = {}  # prev_hash -> [block_hashes]
        self.log = logging.getLogger("baseline.orphan_manager")
    
    def add_orphan(self, block: Block) -> None:
        """Add an orphan block."""
        block_hash = block.hash()
        
        # Don't add if already exists
        if block_hash in self.orphans:
            return
        
        # Enforce size limit
        if len(self.orphans) >= self.max_orphans:
            self._remove_oldest_orphan()
        
        self.orphans[block_hash] = block
        
        # Index by previous hash for quick lookup
        prev_hash = block.header.prev_hash
        if prev_hash not in self.orphans_by_prev:
            self.orphans_by_prev[prev_hash] = []
        self.orphans_by_prev[prev_hash].append(block_hash)
        
        self.log.debug("Added orphan block %s (prev: %s)", block_hash[:16], prev_hash[:16])
    
    def get_orphans_by_prev(self, prev_hash: str) -> list[Block]:
        """Get orphan blocks that build on the given hash."""
        block_hashes = self.orphans_by_prev.get(prev_hash, [])
        return [self.orphans[h] for h in block_hashes if h in self.orphans]
    
    def remove_orphan(self, block_hash: str) -> Block | None:
        """Remove and return an orphan block."""
        block = self.orphans.pop(block_hash, None)
        if block:
            prev_hash = block.header.prev_hash
            if prev_hash in self.orphans_by_prev:
                try:
                    self.orphans_by_prev[prev_hash].remove(block_hash)
                    if not self.orphans_by_prev[prev_hash]:
                        del self.orphans_by_prev[prev_hash]
                except ValueError:
                    pass
        return block
    
    def _remove_oldest_orphan(self) -> None:
        """Remove the oldest orphan block."""
        if not self.orphans:
            return
        
        # Find oldest by timestamp (simple heuristic)
        oldest_hash = min(self.orphans.keys(), 
                         key=lambda h: self.orphans[h].header.timestamp)
        self.remove_orphan(oldest_hash)
    
    def cleanup_orphans(self, max_age: int = 3600) -> None:
        """Remove orphans older than max_age seconds."""
        import time
        current_time = int(time.time())
        
        to_remove = []
        for block_hash, block in self.orphans.items():
            if current_time - block.header.timestamp > max_age:
                to_remove.append(block_hash)
        
        for block_hash in to_remove:
            self.remove_orphan(block_hash)
        
        if to_remove:
            self.log.info("Cleaned up %d stale orphan blocks", len(to_remove))
    
    def get_stats(self) -> dict[str, Any]:
        """Get orphan manager statistics."""
        return {
            "total_orphans": len(self.orphans),
            "orphan_chains": len(self.orphans_by_prev),
        }


class ForkDetector:
    """Detect and handle blockchain forks."""
    
    def __init__(self, block_store: BlockStore, state_db: StateDB):
        self.block_store = block_store
        self.state_db = state_db
        self.orphan_manager = OrphanBlockManager()
        self.log = logging.getLogger("baseline.fork_detector")
    
    def detect_fork(self, new_block: Block) -> ForkInfo | None:
        """Detect if a new block creates a fork."""
        block_hash = new_block.hash()
        prev_hash = new_block.header.prev_hash
        
        # Get current chain tip
        current_tip = self.state_db.get_tip_hash()
        if not current_tip:
            return None
        
        # If block builds on current tip, no fork
        if prev_hash == current_tip:
            return None
        
        # Check if previous block exists in our chain
        prev_header = self.state_db.get_header(prev_hash)
        if not prev_header:
            # Previous block unknown - this is an orphan
            self.orphan_manager.add_orphan(new_block)
            return None
        
        # We have a fork - calculate work for both chains
        current_tip_header = self.state_db.get_header(current_tip)
        if not current_tip_header:
            return None
        
        # Find fork point
        fork_height = self._find_fork_point(prev_hash, current_tip)
        if fork_height is None:
            return None
        
        # Calculate chain work from fork point
        main_chain_work = self._calculate_chain_work_from_height(current_tip, fork_height)
        fork_chain_work = self._calculate_chain_work_from_height(block_hash, fork_height)
        
        # Determine if we should reorganize
        should_reorganize = fork_chain_work > main_chain_work
        
        return ForkInfo(
            fork_height=fork_height,
            main_chain_tip=current_tip,
            fork_chain_tip=block_hash,
            fork_length=current_tip_header.height - fork_height,
            main_chain_work=main_chain_work,
            fork_chain_work=fork_chain_work,
            should_reorganize=should_reorganize
        )
    
    def _find_fork_point(self, hash1: str, hash2: str) -> int | None:
        """Find the height where two chains diverged."""
        # Walk back from both hashes until we find a common ancestor
        ancestors1 = self._get_ancestors(hash1, max_depth=1000)
        ancestors2 = self._get_ancestors(hash2, max_depth=1000)
        
        # Find common ancestors
        common = set(ancestors1.keys()) & set(ancestors2.keys())
        if not common:
            return None
        
        # Return the highest common ancestor
        return max(ancestors1[h] for h in common)
    
    def _get_ancestors(self, block_hash: str, max_depth: int = 1000) -> dict[str, int]:
        """Get ancestors of a block up to max_depth. Returns hash -> height mapping."""
        ancestors = {}
        current_hash = block_hash
        
        for _ in range(max_depth):
            header = self.state_db.get_header(current_hash)
            if not header:
                break
            
            ancestors[current_hash] = header.height
            
            if header.height == 0:  # Genesis block
                break
            
            current_hash = header.prev_hash
        
        return ancestors
    
    def _calculate_chain_work_from_height(self, tip_hash: str, from_height: int) -> int:
        """Calculate total work from a given height to tip."""
        total_work = 0
        current_hash = tip_hash
        
        while True:
            header = self.state_db.get_header(current_hash)
            if not header or header.height < from_height:
                break
            
            total_work += difficulty.block_work(header.bits)
            
            if header.height == from_height:
                break
            
            current_hash = header.prev_hash
        
        return total_work
    
    def process_orphans(self, new_tip_hash: str) -> list[Block]:
        """Process orphan blocks that might now connect to the chain."""
        connected_blocks = []
        
        # Check if any orphans can now connect
        orphans = self.orphan_manager.get_orphans_by_prev(new_tip_hash)
        for orphan in orphans:
            orphan_hash = orphan.hash()
            self.orphan_manager.remove_orphan(orphan_hash)
            connected_blocks.append(orphan)
            
            # Recursively check for more orphans
            more_orphans = self.process_orphans(orphan_hash)
            connected_blocks.extend(more_orphans)
        
        return connected_blocks
    
    def cleanup(self) -> None:
        """Periodic cleanup of stale data."""
        self.orphan_manager.cleanup_orphans()


class ChainReorganizer:
    """Handle chain reorganizations safely using the existing chain infrastructure."""
    
    def __init__(self, chain):
        """Initialize with a reference to the main Chain object."""
        self.chain = chain
        self.log = logging.getLogger("baseline.reorganizer")
    
    def reorganize_to_fork(self, fork_info: ForkInfo) -> bool:
        """Perform a chain reorganization to the fork chain."""
        try:
            self.log.info(
                "Starting chain reorganization: fork at height %d, "
                "main work=%d, fork work=%d",
                fork_info.fork_height,
                fork_info.main_chain_work,
                fork_info.fork_chain_work
            )
            
            # Use the existing chain reorganization method
            self.chain._reorganize_to(fork_info.fork_chain_tip)
            
            self.log.info(
                "Chain reorganization successful to new tip %s",
                fork_info.fork_chain_tip[:16]
            )
            return True
        
        except Exception as exc:
            self.log.error("Chain reorganization failed: %s", exc)
            return False


class ForkManager:
    """Main fork management coordinator."""
    
    def __init__(self, chain):
        """Initialize with a reference to the main Chain object."""
        self.chain = chain
        self.detector = ForkDetector(chain.block_store, chain.state_db)
        self.reorganizer = ChainReorganizer(chain)
        self.log = logging.getLogger("baseline.fork_manager")
    
    def handle_new_block(self, block: Block) -> tuple[bool, bool]:
        """
        Handle a new block and check for forks.
        Returns (block_accepted, reorganization_occurred).
        """
        # Detect if this creates a fork
        fork_info = self.detector.detect_fork(block)
        
        if fork_info is None:
            # No fork detected, normal block processing
            return True, False
        
        self.log.info(
            "Fork detected: height=%d, main_work=%d, fork_work=%d",
            fork_info.fork_height,
            fork_info.main_chain_work,
            fork_info.fork_chain_work
        )
        
        if fork_info.should_reorganize:
            # Perform reorganization
            success = self.reorganizer.reorganize_to_fork(fork_info)
            if success:
                # Process any orphans that might now connect
                orphans = self.detector.process_orphans(fork_info.fork_chain_tip)
                if orphans:
                    self.log.info("Connected %d orphan blocks after reorganization", len(orphans))
                return True, True
            else:
                self.log.error("Failed to reorganize to fork chain")
                return False, False
        else:
            # Fork has less work, ignore it
            self.log.info("Fork has less work, staying on main chain")
            return False, False
    
    def cleanup(self) -> None:
        """Periodic cleanup."""
        self.detector.cleanup()
    
    def get_fork_stats(self) -> dict[str, Any]:
        """Get fork management statistics."""
        return {
            "orphan_stats": self.detector.orphan_manager.get_stats(),
        }