"""
Excel File Manager for TURBOPREDICT X PROTEAN
Handles Excel file renaming to avoid save prompts during automation
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Optional
import time
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ExcelFileManager:
    """Manages Excel file operations to avoid save prompts during automation."""
    
    def __init__(self, xlsx_path: Path):
        """Initialize the Excel file manager.
        
        Args:
            xlsx_path: Path to the main Excel file
        """
        self.xlsx_path = Path(xlsx_path)
        self.backup_suffix = "_backup"
        self.temp_suffix = "_temp"
        self.dummy_suffix = "_dummy"
        
    def create_dummy_file(self) -> Path:
        """Create a dummy file by renaming the original Excel file.
        
        This allows Excel to save updated data to the original filename
        without prompting the user.
        
        Returns:
            Path to the created dummy file
        """
        if not self.xlsx_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.xlsx_path}")
            
        # Generate timestamp for unique naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create dummy filename
        dummy_name = f"{self.xlsx_path.stem}{self.dummy_suffix}_{timestamp}{self.xlsx_path.suffix}"
        dummy_path = self.xlsx_path.parent / dummy_name
        
        logger.info(f"Creating dummy file: {self.xlsx_path} -> {dummy_path}")
        
        try:
            # Rename original to dummy
            shutil.move(str(self.xlsx_path), str(dummy_path))
            return dummy_path
            
        except Exception as e:
            logger.error(f"Failed to create dummy file: {e}")
            raise
    
    def create_working_copy(self) -> Path:
        """Create a working copy of the Excel file.
        
        This creates a copy that can be opened and refreshed while
        preserving the original file.
        
        Returns:
            Path to the working copy
        """
        if not self.xlsx_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.xlsx_path}")
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        working_name = f"{self.xlsx_path.stem}_working_{timestamp}{self.xlsx_path.suffix}"
        working_path = self.xlsx_path.parent / working_name
        
        logger.info(f"Creating working copy: {self.xlsx_path} -> {working_path}")
        
        try:
            shutil.copy2(str(self.xlsx_path), str(working_path))
            return working_path
            
        except Exception as e:
            logger.error(f"Failed to create working copy: {e}")
            raise
    
    def restore_from_dummy(self, dummy_path: Path, keep_dummy: bool = False) -> bool:
        """Restore original file from dummy if needed.
        
        Args:
            dummy_path: Path to the dummy file
            keep_dummy: Whether to keep the dummy file after restoration
            
        Returns:
            True if restoration was needed and successful
        """
        if not dummy_path.exists():
            logger.warning(f"Dummy file not found: {dummy_path}")
            return False
            
        # If original doesn't exist, restore from dummy
        if not self.xlsx_path.exists():
            logger.info(f"Restoring from dummy: {dummy_path} -> {self.xlsx_path}")
            try:
                if keep_dummy:
                    shutil.copy2(str(dummy_path), str(self.xlsx_path))
                else:
                    shutil.move(str(dummy_path), str(self.xlsx_path))
                return True
            except Exception as e:
                logger.error(f"Failed to restore from dummy: {e}")
                raise
        
        # Original exists, just clean up dummy if requested
        if not keep_dummy and dummy_path.exists():
            try:
                dummy_path.unlink()
                logger.info(f"Cleaned up dummy file: {dummy_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up dummy file: {e}")
                
        return False
    
    def cleanup_temp_files(self, max_age_hours: int = 24) -> int:
        """Clean up old temporary/dummy files.
        
        Args:
            max_age_hours: Maximum age in hours for temp files to keep
            
        Returns:
            Number of files cleaned up
        """
        cleanup_count = 0
        search_patterns = [
            f"*{self.backup_suffix}*{self.xlsx_path.suffix}",
            f"*{self.temp_suffix}*{self.xlsx_path.suffix}",
            f"*{self.dummy_suffix}*{self.xlsx_path.suffix}",
            f"*_working_*{self.xlsx_path.suffix}"
        ]
        
        cutoff_time = time.time() - (max_age_hours * 3600)
        
        for pattern in search_patterns:
            for temp_file in self.xlsx_path.parent.glob(pattern):
                try:
                    if temp_file.stat().st_mtime < cutoff_time:
                        temp_file.unlink()
                        cleanup_count += 1
                        logger.info(f"Cleaned up old temp file: {temp_file}")
                except Exception as e:
                    logger.warning(f"Failed to clean up {temp_file}: {e}")
        
        return cleanup_count
    
    def create_backup(self) -> Path:
        """Create a backup of the current Excel file.
        
        Returns:
            Path to the backup file
        """
        if not self.xlsx_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.xlsx_path}")
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{self.xlsx_path.stem}{self.backup_suffix}_{timestamp}{self.xlsx_path.suffix}"
        backup_path = self.xlsx_path.parent / backup_name
        
        logger.info(f"Creating backup: {self.xlsx_path} -> {backup_path}")
        
        try:
            shutil.copy2(str(self.xlsx_path), str(backup_path))
            return backup_path
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise


def create_dummy_and_refresh(xlsx_path: Path, settle_seconds: int = 5) -> tuple[Path, Optional[Path]]:
    """Create dummy file and prepare for Excel refresh without save prompts.
    
    This function implements the core strategy:
    1. Create a dummy/backup copy of the Excel file
    2. The original filename is now free for Excel to save to
    3. Return paths for further processing
    
    Args:
        xlsx_path: Path to the Excel file
        settle_seconds: Seconds to wait after operations
        
    Returns:
        Tuple of (original_path, dummy_path)
    """
    manager = ExcelFileManager(xlsx_path)
    
    try:
        # Create backup first for safety
        backup_path = manager.create_backup()
        logger.info(f"Created safety backup: {backup_path}")
        
        # Create dummy file (renames original)
        dummy_path = manager.create_dummy_file()
        
        # Wait for file system operations to settle
        time.sleep(settle_seconds)
        
        return xlsx_path, dummy_path
        
    except Exception as e:
        logger.error(f"Failed to create dummy file setup: {e}")
        raise


def restore_after_refresh(xlsx_path: Path, dummy_path: Path, keep_dummy: bool = True) -> bool:
    """Restore file structure after Excel refresh operation.
    
    Args:
        xlsx_path: Original Excel file path
        dummy_path: Path to the dummy file
        keep_dummy: Whether to keep the dummy file
        
    Returns:
        True if operations completed successfully
    """
    manager = ExcelFileManager(xlsx_path)
    
    try:
        # If Excel created new file, we're good
        if xlsx_path.exists():
            logger.info(f"Excel refresh successful - new data saved to: {xlsx_path}")
            
            # Clean up dummy if requested
            if not keep_dummy and dummy_path.exists():
                dummy_path.unlink()
                logger.info(f"Cleaned up dummy file: {dummy_path}")
            
            return True
        else:
            # Restore from dummy if original missing
            logger.warning("Original file missing after refresh - restoring from dummy")
            return manager.restore_from_dummy(dummy_path, keep_dummy)
            
    except Exception as e:
        logger.error(f"Failed to restore after refresh: {e}")
        # Try to restore from dummy as fallback
        try:
            return manager.restore_from_dummy(dummy_path, keep_dummy=True)
        except:
            logger.error("Failed to restore from dummy - manual recovery may be needed")
            raise