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


def _find_credentials_path() -> Optional[str]:
    """Find Google Cloud credentials with fallback options"""
    base_path = _get_base_path()
    
    # Option 1: Environment variable (highest priority)
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if os.path.exists(cred_path):
            return cred_path
    
    # Option 2: Local credentials directory
    local_options = [
        base_path / "credentials" / "key.json",
        base_path / "credentials" / "service-account.json",
        base_path / "credentials" / "gcp-key.json",
    ]
    
    for option in local_options:
        if option.exists():
            return str(option)
    
    # Option 3: Default Google Cloud locations
    home_path = Path.home()
    gcloud_options = [
        home_path / ".config" / "gcloud" / "application_default_credentials.json",
        home_path / ".config" / "gcloud" / "legacy_credentials"
    ]
    
    for option in gcloud_options:
        if option.exists():
            return str(option)
    
    return None


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
        
        # Set credentials path with smart detection
        self.credentials_path = _find_credentials_path()
        
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
    
    @property
    def has_credentials(self) -> bool:
        """Check if valid credentials are available"""
        return self.credentials_path is not None
    
    def get_credentials_help(self) -> str:
        """Get help message for setting up credentials"""
        base_path = _get_base_path()
        
        return f"""
🔑 Google Cloud Credentials Setup:

Option 1 (Recommended for local development):
   gcloud auth application-default login

Option 2 (Service Account Key):
   1. Create credentials directory: mkdir -p {base_path}/credentials
   2. Download service account key from Google Cloud Console
   3. Save as: {base_path}/credentials/key.json
   4. Set environment: export GOOGLE_APPLICATION_CREDENTIALS={base_path}/credentials/key.json

Option 3 (Environment Variable):
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/key.json

Current search locations:
   • Environment: GOOGLE_APPLICATION_CREDENTIALS
   • Project: {base_path}/credentials/key.json
   • Home: ~/.config/gcloud/application_default_credentials.json
"""
    
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
        
        # Validate credentials (only if not dry run)
        if not self.dry_run and not self.has_credentials:
            help_msg = self.get_credentials_help()
            raise ValueError(f"Google Cloud credentials not found.\n{help_msg}")


def get_config(**overrides) -> ETLConfig:
    """Get configuration with optional overrides"""
    config = ETLConfig(**overrides)
    config.validate()
    return config