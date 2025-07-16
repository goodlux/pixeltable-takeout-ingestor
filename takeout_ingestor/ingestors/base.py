"""
Base Ingestor Class for Pixeltable Takeout Ingestor

Provides a common interface and functionality for all data ingestors.
Focused on Pixeltable backend with unified error handling.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from pathlib import Path
import logging
import json
from enum import Enum
import os


class IngestorStatus(Enum):
    """Ingestion status states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


class IngestorProgress:
    """Track ingestion progress with resumable state."""
    
    def __init__(self, total_items: int = 0):
        self.total_items = total_items
        self.processed_items = 0
        self.failed_items = 0
        self.start_time = datetime.now()
        self.last_update = self.start_time
        self.status = IngestorStatus.PENDING
        self.errors: List[str] = []
        self.metadata: Dict[str, Any] = {}
    
    @property
    def completion_percentage(self) -> float:
        """Calculate completion percentage."""
        if self.total_items == 0:
            return 0.0
        return (self.processed_items / self.total_items) * 100
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.processed_items == 0:
            return 0.0
        return ((self.processed_items - self.failed_items) / self.processed_items) * 100
    
    def update(self, processed: int = 1, failed: int = 0, metadata: Optional[Dict] = None):
        """Update progress counters."""
        self.processed_items += processed
        self.failed_items += failed
        self.last_update = datetime.now()
        if metadata:
            self.metadata.update(metadata)
    
    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(f"{datetime.now().isoformat()}: {error}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total_items": self.total_items,
            "processed_items": self.processed_items,
            "failed_items": self.failed_items,
            "completion_percentage": self.completion_percentage,
            "success_rate": self.success_rate,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "last_update": self.last_update.isoformat(),
            "error_count": len(self.errors),
            "recent_errors": self.errors[-5:],  # Last 5 errors
            "metadata": self.metadata
        }


class BaseIngestor(ABC):
    """
    Abstract base class for all Pixeltable data ingestors.
    
    Provides common functionality for:
    - Pixeltable backend management
    - Progress tracking and resumable ingestion
    - Error handling and logging
    - Data validation and transformation
    """
    
    def __init__(
        self, 
        batch_size: int = 100,
        enable_progress_tracking: bool = True,
        pixeltable_home: Optional[str] = None
    ):
        """
        Initialize the base ingestor.
        
        Args:
            batch_size: Number of items to process in each batch
            enable_progress_tracking: Whether to track progress
            pixeltable_home: Override default Pixeltable home directory
        """
        self.batch_size = batch_size
        self.enable_progress_tracking = enable_progress_tracking
        
        # Set up logging
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        
        # Initialize backend connections
        self.pixeltable_client = None
        
        # Progress tracking
        self.progress = IngestorProgress() if enable_progress_tracking else None
        
        # Configure Pixeltable home directory
        if pixeltable_home:
            os.environ['PIXELTABLE_HOME'] = str(pixeltable_home)
        
        self.logger.info(f"Initialized {self.__class__.__name__}")
    
    @abstractmethod
    def get_supported_formats(self) -> List[str]:
        """Return list of supported file formats/extensions."""
        pass
    
    @abstractmethod
    def get_table_name(self) -> str:
        """Return the Pixeltable table name for this ingestor."""
        pass
    
    @abstractmethod
    def validate_source(self, source_path: str) -> Tuple[bool, Optional[str]]:
        """
        Validate that the source data is compatible with this ingestor.
        
        Args:
            source_path: Path to the source data
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        pass
    
    @abstractmethod
    def parse_source(self, source_path: str) -> List[Dict[str, Any]]:
        """
        Parse the source data into a standardized format.
        
        Args:
            source_path: Path to the source data
            
        Returns:
            List of normalized data records
        """
        pass
    
    @abstractmethod
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single record into the target schema.
        
        Args:
            record: Raw data record
            
        Returns:
            Transformed record ready for storage
        """
        pass
    
    def initialize_backend(self):
        """Initialize the Pixeltable backend."""
        try:
            import pixeltable as pxt
            self.pixeltable_client = pxt
            
            # Ensure required tables exist
            self._ensure_pixeltable_schema()
            
            self.logger.info("Pixeltable backend initialized")
            
        except ImportError:
            self.logger.error("Pixeltable not available - install with: pip install pixeltable")
            raise
        except Exception as e:
            self.logger.error(f"Pixeltable initialization failed: {e}")
            raise
    
    @abstractmethod
    def _ensure_pixeltable_schema(self):
        """Ensure required Pixeltable tables exist."""
        pass
    
    def ingest(
        self, 
        source_path: str,
        resume: bool = False,
        validate_only: bool = False
    ) -> Dict[str, Any]:
        """
        Main ingestion method.
        
        Args:
            source_path: Path to the source data
            resume: Whether to resume from previous progress
            validate_only: Only validate, don't actually ingest
            
        Returns:
            Ingestion summary
        """
        try:
            self.logger.info(f"Starting ingestion of {source_path}")
            
            # Initialize backend
            self.initialize_backend()
            
            # Validate source
            is_valid, error_msg = self.validate_source(source_path)
            if not is_valid:
                raise ValueError(f"Source validation failed: {error_msg}")
            
            # Parse source data
            records = self.parse_source(source_path)
            
            if self.progress:
                self.progress.total_items = len(records)
                self.progress.status = IngestorStatus.RUNNING
            
            self.logger.info(f"Parsed {len(records)} records from source")
            
            if validate_only:
                return {
                    "validation": "passed",
                    "record_count": len(records),
                    "sample_record": records[0] if records else None
                }
            
            # Process in batches
            processed_count = 0
            failed_count = 0
            
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i + self.batch_size]
                
                try:
                    batch_results = self._process_batch(batch)
                    processed_count += batch_results["processed"]
                    failed_count += batch_results["failed"]
                    
                    if self.progress:
                        self.progress.update(
                            processed=batch_results["processed"],
                            failed=batch_results["failed"]
                        )
                    
                    self.logger.info(f"Processed batch {i//self.batch_size + 1}, "
                                   f"records: {processed_count}/{len(records)}")
                
                except Exception as e:
                    self.logger.error(f"Batch processing failed: {e}")
                    if self.progress:
                        self.progress.add_error(f"Batch {i//self.batch_size + 1}: {e}")
                    failed_count += len(batch)
            
            # Update final status
            if self.progress:
                self.progress.status = IngestorStatus.COMPLETED if failed_count == 0 else IngestorStatus.FAILED
            
            summary = {
                "total_records": len(records),
                "processed_records": processed_count,
                "failed_records": failed_count,
                "success_rate": ((processed_count - failed_count) / processed_count * 100) if processed_count > 0 else 0,
                "table_name": self.get_table_name(),
                "progress": self.progress.to_dict() if self.progress else None
            }
            
            self.logger.info(f"Ingestion completed: {summary}")
            return summary
            
        except Exception as e:
            self.logger.error(f"Ingestion failed: {e}")
            if self.progress:
                self.progress.status = IngestorStatus.FAILED
                self.progress.add_error(str(e))
            raise
    
    def _process_batch(self, batch: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Process a batch of records.
        
        Args:
            batch: List of records to process
            
        Returns:
            Dictionary with processed and failed counts
        """
        processed = 0
        failed = 0
        
        for record in batch:
            try:
                # Transform record
                transformed = self.transform_record(record)
                
                # Store in Pixeltable
                self._store_pixeltable(transformed)
                
                processed += 1
                
            except Exception as e:
                self.logger.warning(f"Failed to process record: {e}")
                if self.progress:
                    self.progress.add_error(f"Record processing: {e}")
                failed += 1
        
        return {"processed": processed, "failed": failed}
    
    @abstractmethod
    def _store_pixeltable(self, record: Dict[str, Any]):
        """Store a record in Pixeltable."""
        pass
    
    def get_progress(self) -> Optional[Dict[str, Any]]:
        """Get current ingestion progress."""
        return self.progress.to_dict() if self.progress else None
    
    def pause(self):
        """Pause the ingestion process."""
        if self.progress:
            self.progress.status = IngestorStatus.PAUSED
            self.logger.info("Ingestion paused")
    
    def resume(self):
        """Resume the ingestion process."""
        if self.progress:
            self.progress.status = IngestorStatus.RUNNING
            self.logger.info("Ingestion resumed")
    
    def cleanup(self):
        """Clean up resources."""
        # Pixeltable cleanup if needed
        self.pixeltable_client = None
        
        self.logger.info("Cleanup completed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
