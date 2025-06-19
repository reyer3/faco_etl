"""
Configuration management for FACO ETL

DRY principle: Single source of truth for all configuration.
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional
from pathlib import Path


def _is_docker_environment() -> bool:
    """Detect if running inside Docker container"""
    return os.path.exists('/.dockerenv') or os.getenv('DOCKER_ENV') == 'true'


def _get_base_path() -> Path:
    """Get base path depending on environment"""
    if _is_docker_environment():
        return Path('/app')
    else:
        return Path(__file__).parent.parent.parent  # Go back to project root


@dataclass
class ETLConfig:
    """Configuration class with sensible defaults"""
    
    # BigQuery
    project_id: str = field(default_factory=lambda: os.getenv("GOOGLE_CLOUD_PROJECT", "mibot-222814"))
    dataset_id: str = field(default_factory=lambda: os.getenv("BIGQUERY_DATASET", "BI_USA"))
    
    # ETL Parameters
    mes_vigencia: str = field(default_factory=lambda: os.getenv("MES_VIGENCIA", "2025-06"))
    estado_vigencia: str = field(default_factory=lambda: os.getenv("ESTADO_VIGENCIA", "abierto"))
    
    # Business Logic
    country_code: str = field(default_factory=lambda: os.getenv("COUNTRY_CODE", "PE"))
    include_saturdays: bool = field(default_factory=lambda: os.getenv("INCLUDE_SATURDAYS", "false").lower() == "true")
    
    # Output Configuration
    output_table_prefix: str = field(default_factory=lambda: os.getenv("OUTPUT_TABLE_PREFIX", "dash_cobranza"))
    overwrite_tables: bool = field(default_factory=lambda: os.getenv("OVERWRITE_TABLES", "true").lower() == "true")
    
    # Performance
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "10000")))
    max_workers: int = field(default_factory=lambda: int(os.getenv("MAX_WORKERS", "4")))
    
    # Logging
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    
    # Runtime flags
    dry_run: bool = False
    
    def __post_init__(self):
        """Initialize paths after object creation"""
        base_path = _get_base_path()
        
        # Set credentials path
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            self.credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        else:
            self.credentials_path = str(base_path / "credentials" / "key.json")
        
        # Set log file path
        if os.getenv("LOG_FILE"):
            self.log_file = os.getenv("LOG_FILE")
        else:
            # Use relative path for local, absolute for Docker
            if _is_docker_environment():
                self.log_file = "/app/logs/etl.log"
            else:
                self.log_file = str(base_path / "logs" / "etl.log")
    
    @property
    def output_tables(self) -> dict:
        """Output table names"""
        return {
            "agregada": f"{self.output_table_prefix}_agregada",
            "comparativas": f"{self.output_table_prefix}_comparativas", 
            "primera_vez": f"{self.output_table_prefix}_primera_vez",
            "base_cartera": f"{self.output_table_prefix}_base_cartera"
        }
    
    @property
    def table_clustering_fields(self) -> List[str]:
        """Fields for BigQuery table clustering optimization"""
        return ["CARTERA", "CANAL", "OPERADOR"]
    
    @property 
    def table_partition_field(self) -> str:
        """Field for BigQuery table partitioning"""
        return "FECHA_SERVICIO"
    
    @property
    def is_local_environment(self) -> bool:
        """Check if running in local environment"""
        return not _is_docker_environment()
    
    def validate(self) -> None:
        """Validate configuration"""
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT is required")
        
        if not self.dataset_id:
            raise ValueError("BIGQUERY_DATASET is required")
        
        # Validate date format
        try:
            year, month = self.mes_vigencia.split("-")
            int(year), int(month)
        except (ValueError, AttributeError):
            raise ValueError("mes_vigencia must be in YYYY-MM format")
        
        if self.estado_vigencia not in ["abierto", "finalizado"]:
            raise ValueError("estado_vigencia must be 'abierto' or 'finalizado'")


def get_config(**overrides) -> ETLConfig:
    """Get configuration with optional overrides"""
    config = ETLConfig(**overrides)
    config.validate()
    return config