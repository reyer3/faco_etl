"""
Configuration management for FACO ETL

DRY principle: Single source of truth for all configuration.
This version includes robust, multi-location credential finding and explicit
credentials object loading for reliable authentication in any environment.
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pathlib import Path

# Importaciones necesarias para la carga de credenciales
from google.oauth2 import service_account
from google.auth.credentials import Credentials
from loguru import logger

def _is_docker_environment() -> bool:
    """Detects if the script is running inside a Docker container."""
    return os.path.exists('/.dockerenv') or os.getenv('DOCKER_ENV') == 'true'


@dataclass
class ETLConfig:
    """
    Configuration class with sensible defaults, loaded from environment variables.
    Handles dynamic path resolution and explicit loading of credentials.
    """

    # --- BigQuery Configuration ---
    project_id: str = field(default_factory=lambda: os.getenv("GOOGLE_CLOUD_PROJECT", "mibot-222814"))
    dataset_id: str = field(default_factory=lambda: os.getenv("BIGQUERY_DATASET", "BI_USA"))

    # --- ETL Parameters ---
    mes_vigencia: str = field(default_factory=lambda: os.getenv("MES_VIGENCIA", "2025-06"))
    estado_vigencia: str = field(default_factory=lambda: os.getenv("ESTADO_VIGENCIA", "abierto"))

    # --- Business Logic ---
    country_code: str = field(default_factory=lambda: os.getenv("COUNTRY_CODE", "PE"))
    include_saturdays: bool = field(default_factory=lambda: os.getenv("INCLUDE_SATURDAYS", "false").lower() == "true")

    # --- Output Configuration ---
    output_table_prefix: str = field(default_factory=lambda: os.getenv("OUTPUT_TABLE_PREFIX", "dash_cobranza"))
    overwrite_tables: bool = field(default_factory=lambda: os.getenv("OVERWRITE_TABLES", "true").lower() == "true")

    # --- Performance ---
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "10000")))
    max_workers: int = field(default_factory=lambda: int(os.getenv("MAX_WORKERS", "4")))

    # --- Logging ---
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper())

    # --- Runtime Flags ---
    dry_run: bool = False

    # --- Dynamically set paths and objects ---
    credentials_path: Optional[str] = field(init=False)
    log_file: str = field(init=False)
    credentials_object: Optional[Credentials] = field(init=False, repr=False)

    def __post_init__(self):
        """Initializes paths and credentials object after the main object is created."""

        # 1. Find and Load Credentials
        self.credentials_path = self._find_credentials_path()
        self.credentials_object = None

        if self.credentials_path and os.path.exists(self.credentials_path):
            try:
                self.credentials_object = service_account.Credentials.from_service_account_file(self.credentials_path)
                logger.trace(f"🔑 Credenciales cargadas exitosamente desde: {self.credentials_path}")
            except Exception as e:
                logger.error(f"❌ Falló la carga del archivo de credenciales JSON desde '{self.credentials_path}': {e}")
                raise
        else:
            logger.info("🔑 No se encontró un archivo de credenciales explícito. Se usará la autenticación de entorno (ADC).")

        # 2. Resolve Log File Path
        env_log_file = os.getenv("LOG_FILE")
        if env_log_file:
            self.log_file = env_log_file
        else:
            if _is_docker_environment():
                log_dir = Path("/app/logs")
            else:
                project_root = Path(__file__).resolve().parent.parent.parent
                log_dir = project_root / "logs"

            log_dir.mkdir(parents=True, exist_ok=True)
            self.log_file = str(log_dir / "etl.log")

    def _find_credentials_path(self) -> Optional[str]:
        """Finds a valid Google Cloud credentials file from multiple standard locations."""
        env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if env_path and os.path.exists(env_path):
            return env_path

        project_root = Path(__file__).resolve().parent.parent.parent
        local_options = [
            project_root / "credentials" / "key.json",
            project_root / "credentials" / "service-account.json",
        ]
        for option in local_options:
            if option.exists():
                return str(option)

        gcloud_path = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
        if gcloud_path.exists():
            return str(gcloud_path)

        return None

    @property
    def output_tables(self) -> Dict[str, str]:
        """Generates a dictionary of final output table names."""
        return {
            "agregada": f"{self.output_table_prefix}_agregada",
            "comparativas": f"{self.output_table_prefix}_comparativas",
            "primera_vez": f"{self.output_table_prefix}_primera_vez",
            "base_cartera": f"{self.output_table_prefix}_base_cartera"
        }

    def validate(self) -> None:
        """Validates that essential configuration parameters are correctly set."""
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT is required but not set.")

        try:
            year, month = map(int, self.mes_vigencia.split("-"))
            if not (1900 < year < 3000 and 1 <= month <= 12):
                raise ValueError("Invalid year or month.")
        except (ValueError, AttributeError):
            raise ValueError(f"mes_vigencia '{self.mes_vigencia}' is invalid. Must be in YYYY-MM format.")

        logger.trace("Configuration validated successfully.")

def get_config(**overrides) -> ETLConfig:
    """Factory function to get a validated configuration instance."""
    config = ETLConfig()
    for key, value in overrides.items():
        if hasattr(config, key) and value is not None:
            setattr(config, key, value)
    config.validate()
    return config