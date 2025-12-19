"""
Network upgrade and version signaling mechanism.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from ..storage import StateDB
from .block import BlockHeader


class UpgradeState(Enum):
    """States of a network upgrade."""
    DEFINED = "defined"          # Upgrade is defined but not yet started
    STARTED = "started"          # Signaling period has begun
    LOCKED_IN = "locked_in"      # Upgrade has been locked in, waiting for activation
    ACTIVE = "active"            # Upgrade is active
    FAILED = "failed"            # Upgrade failed to activate


@dataclass
class UpgradeDefinition:
    """Definition of a network upgrade."""
    name: str
    bit: int                     # Version bit to signal (0-28)
    start_time: int             # Unix timestamp when signaling starts
    timeout: int                # Unix timestamp when signaling times out
    threshold: int              # Number of blocks in period that must signal (e.g., 1916 out of 2016)
    period: int                 # Number of blocks in a signaling period (e.g., 2016)
    min_activation_height: int  # Minimum height for activation
    description: str
    
    def is_signaling_bit_set(self, version: int) -> bool:
        """Check if the signaling bit is set in a block version."""
        return (version & (1 << self.bit)) != 0


class VersionBitsTracker:
    """Track version bits signaling for network upgrades."""
    
    def __init__(self, state_db: StateDB):
        self.state_db = state_db
        self.log = logging.getLogger("baseline.version_bits")
        
        # Define network upgrades
        self.upgrades: dict[str, UpgradeDefinition] = {}
        self._define_upgrades()
    
    def _define_upgrades(self) -> None:
        """Define known network upgrades."""
        # No upgrades defined by default - add them via add_custom_upgrade() as needed
        # 
        # Example upgrade definitions (commented out):
        #
        # Enhanced transaction validation upgrade:
        # self.upgrades["enhanced_validation"] = UpgradeDefinition(
        #     name="enhanced_validation",
        #     bit=0,
        #     start_time=int(time.time()) + 86400 * 30,  # Start in 30 days
        #     timeout=int(time.time()) + 86400 * 365,    # Timeout in 1 year
        #     threshold=1512,  # 75% of 2016 blocks
        #     period=2016,     # 2016 blocks (like Bitcoin)
        #     min_activation_height=10000,
        #     description="Enhanced transaction validation rules"
        # )
        #
        # New script opcodes upgrade:
        # self.upgrades["new_opcodes"] = UpgradeDefinition(
        #     name="new_opcodes",
        #     bit=1,
        #     start_time=int(time.time()) + 86400 * 60,  # Start in 60 days
        #     timeout=int(time.time()) + 86400 * 395,    # Timeout in ~13 months
        #     threshold=1512,  # 75% of 2016 blocks
        #     period=2016,
        #     min_activation_height=15000,
        #     description="New script opcodes for enhanced functionality"
        # )
        pass
    
    def get_upgrade_state(self, upgrade_name: str, height: int, block_time: int) -> UpgradeState:
        """Get the current state of an upgrade at a given height and time."""
        upgrade = self.upgrades.get(upgrade_name)
        if not upgrade:
            raise ValueError(f"Unknown upgrade: {upgrade_name}")
        
        # Check if we're before the start time
        if block_time < upgrade.start_time:
            return UpgradeState.DEFINED
        
        # Check if we've timed out
        if block_time >= upgrade.timeout:
            return UpgradeState.FAILED
        
        # Check if we're before minimum activation height
        if height < upgrade.min_activation_height:
            return UpgradeState.DEFINED
        
        # Check if already active
        if self._is_upgrade_active(upgrade_name, height):
            return UpgradeState.ACTIVE
        
        # Check if locked in
        if self._is_upgrade_locked_in(upgrade_name, height):
            return UpgradeState.LOCKED_IN
        
        # We're in the signaling period
        return UpgradeState.STARTED
    
    def _is_upgrade_active(self, upgrade_name: str, height: int) -> bool:
        """Check if an upgrade is already active."""
        # Check our database for activation records
        activation_height = self.state_db.get_upgrade_activation_height(upgrade_name)
        return activation_height is not None and height >= activation_height
    
    def _is_upgrade_locked_in(self, upgrade_name: str, height: int) -> bool:
        """Check if an upgrade is locked in (activated in next period)."""
        upgrade = self.upgrades[upgrade_name]
        
        # Find the current signaling period
        period_start = (height // upgrade.period) * upgrade.period
        
        # Check if the previous period had enough signaling
        if period_start >= upgrade.period:
            prev_period_start = period_start - upgrade.period
            signaling_count = self._count_signaling_blocks(upgrade, prev_period_start, upgrade.period)
            return signaling_count >= upgrade.threshold
        
        return False
    
    def _count_signaling_blocks(self, upgrade: UpgradeDefinition, start_height: int, count: int) -> int:
        """Count blocks signaling for an upgrade in a given range."""
        signaling_count = 0
        
        for height in range(start_height, start_height + count):
            header = self.state_db.get_header_by_height(height)
            if header and upgrade.is_signaling_bit_set(header.version):
                signaling_count += 1
        
        return signaling_count
    
    def process_new_block(self, header: BlockHeader, height: int) -> dict[str, Any]:
        """Process a new block for version bits signaling."""
        results = {}
        
        for upgrade_name, upgrade in self.upgrades.items():
            old_state = self.get_upgrade_state(upgrade_name, height - 1, header.timestamp)
            new_state = self.get_upgrade_state(upgrade_name, height, header.timestamp)
            
            # Check for state transitions
            if old_state != new_state:
                self.log.info(
                    "Upgrade %s state transition: %s -> %s at height %d",
                    upgrade_name, old_state.value, new_state.value, height
                )
                
                # Handle activation
                if new_state == UpgradeState.ACTIVE and old_state == UpgradeState.LOCKED_IN:
                    self.state_db.set_upgrade_activation_height(upgrade_name, height)
                    self.log.info("Upgrade %s activated at height %d", upgrade_name, height)
            
            results[upgrade_name] = {
                "state": new_state.value,
                "signaling": upgrade.is_signaling_bit_set(header.version),
                "height": height,
            }
        
        return results
    
    def get_signaling_info(self, height: int) -> dict[str, Any]:
        """Get current signaling information for all upgrades."""
        info = {}
        
        for upgrade_name, upgrade in self.upgrades.items():
            # Get current block for timestamp
            current_header = self.state_db.get_header_by_height(height)
            if not current_header:
                continue
            
            state = self.get_upgrade_state(upgrade_name, height, current_header.timestamp)
            
            # Calculate signaling progress for active periods
            signaling_progress = None
            if state == UpgradeState.STARTED:
                period_start = (height // upgrade.period) * upgrade.period
                blocks_in_period = height - period_start + 1
                signaling_count = self._count_signaling_blocks(upgrade, period_start, blocks_in_period)
                
                signaling_progress = {
                    "period_start": period_start,
                    "blocks_in_period": blocks_in_period,
                    "signaling_count": signaling_count,
                    "threshold": upgrade.threshold,
                    "percentage": (signaling_count / blocks_in_period) * 100 if blocks_in_period > 0 else 0,
                }
            
            info[upgrade_name] = {
                "state": state.value,
                "bit": upgrade.bit,
                "start_time": upgrade.start_time,
                "timeout": upgrade.timeout,
                "threshold": upgrade.threshold,
                "period": upgrade.period,
                "min_activation_height": upgrade.min_activation_height,
                "description": upgrade.description,
                "signaling_progress": signaling_progress,
            }
        
        return info
    
    def should_signal_upgrade(self, upgrade_name: str, height: int, block_time: int) -> bool:
        """Check if we should signal for an upgrade in a new block."""
        upgrade = self.upgrades.get(upgrade_name)
        if not upgrade:
            return False
        
        state = self.get_upgrade_state(upgrade_name, height, block_time)
        
        # Only signal during the STARTED state
        return state == UpgradeState.STARTED
    
    def create_version_for_block(self, height: int, block_time: int, base_version: int = 1) -> int:
        """Create a version field for a new block with appropriate signaling bits."""
        version = base_version
        
        for upgrade_name in self.upgrades:
            if self.should_signal_upgrade(upgrade_name, height, block_time):
                upgrade = self.upgrades[upgrade_name]
                version |= (1 << upgrade.bit)
                self.log.debug("Signaling for upgrade %s (bit %d) at height %d", 
                              upgrade_name, upgrade.bit, height)
        
        return version


class BackwardCompatibility:
    """Handle backward compatibility during upgrades."""
    
    def __init__(self, version_tracker: VersionBitsTracker):
        self.version_tracker = version_tracker
        self.log = logging.getLogger("baseline.compatibility")
    
    def validate_block_version(self, header: BlockHeader) -> tuple[bool, str]:
        """Validate that a block version is acceptable."""
        # Basic version validation
        if header.version < 1:
            return False, "Block version must be at least 1"
        
        # Check for unknown signaling bits
        known_bits = set()
        for upgrade in self.version_tracker.upgrades.values():
            known_bits.add(upgrade.bit)
        
        # Extract signaling bits (bits 0-28)
        signaling_bits = header.version & 0x1FFFFFFF
        
        for bit in range(29):  # Check bits 0-28
            if (signaling_bits & (1 << bit)) and bit not in known_bits:
                self.log.warning("Unknown signaling bit %d in block version %d", bit, header.version)
                # For now, we'll accept unknown bits but log them
        
        return True, ""
    
    def is_feature_active(self, feature_name: str, height: int) -> bool:
        """Check if a feature is active at a given height."""
        return self.version_tracker._is_upgrade_active(feature_name, height)
    
    def get_active_features(self, height: int) -> list[str]:
        """Get list of active features at a given height."""
        active_features = []
        
        for upgrade_name in self.version_tracker.upgrades:
            if self.is_feature_active(upgrade_name, height):
                active_features.append(upgrade_name)
        
        return active_features


class UpgradeManager:
    """Main upgrade management coordinator."""
    
    def __init__(self, state_db: StateDB):
        self.version_tracker = VersionBitsTracker(state_db)
        self.compatibility = BackwardCompatibility(self.version_tracker)
        self.log = logging.getLogger("baseline.upgrade_manager")
    
    def process_new_block(self, header: BlockHeader, height: int) -> dict[str, Any]:
        """Process a new block for upgrade signaling."""
        # Validate block version
        is_valid, error = self.compatibility.validate_block_version(header)
        if not is_valid:
            self.log.error("Invalid block version: %s", error)
            return {"error": error}
        
        # Process version bits signaling
        signaling_results = self.version_tracker.process_new_block(header, height)
        
        return {
            "signaling_results": signaling_results,
            "active_features": self.compatibility.get_active_features(height),
        }
    
    def create_block_version(self, height: int, block_time: int) -> int:
        """Create appropriate version for a new block."""
        return self.version_tracker.create_version_for_block(height, block_time)
    
    def get_upgrade_status(self, height: int) -> dict[str, Any]:
        """Get comprehensive upgrade status."""
        return {
            "signaling_info": self.version_tracker.get_signaling_info(height),
            "active_features": self.compatibility.get_active_features(height),
        }
    
    def is_feature_active(self, feature_name: str, height: int) -> bool:
        """Check if a specific feature is active."""
        return self.compatibility.is_feature_active(feature_name, height)
    
    def add_custom_upgrade(self, upgrade: UpgradeDefinition) -> None:
        """Add a custom upgrade definition."""
        if upgrade.name in self.version_tracker.upgrades:
            raise ValueError(f"Upgrade {upgrade.name} already exists")
        
        # Validate bit is not already used
        used_bits = {u.bit for u in self.version_tracker.upgrades.values()}
        if upgrade.bit in used_bits:
            raise ValueError(f"Bit {upgrade.bit} is already used by another upgrade")
        
        if not (0 <= upgrade.bit <= 28):
            raise ValueError(f"Bit must be between 0 and 28, got {upgrade.bit}")
        
        self.version_tracker.upgrades[upgrade.name] = upgrade
        self.log.info("Added custom upgrade: %s (bit %d)", upgrade.name, upgrade.bit)