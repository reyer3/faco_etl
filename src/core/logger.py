"""
Logging configuration for FACO ETL

KISS principle: Simple, structured logging with good defaults.
"""

import sys
from pathlib import Path
from loguru import logger
from typing import Optional


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Setup structured logging with console and optional file output"""
    
    # Remove default handler
    logger.remove()
    
    # Console logging with colors
    logger.add(
        sys.stdout,
        level=level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    # File logging if specified
    if log_file:
        try:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            logger.add(
                log_file,
                level=level,
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
                rotation="10 MB",
                retention="30 days",
                compression="gz"
            )
            logger.info(f"📄 Archivo de log: {log_file}")
        except (PermissionError, OSError) as e:
            logger.warning(f"⚠️  No se pudo crear archivo de log {log_file}: {e}")
            logger.info("📝 Continuando solo con logging de consola")
    
    logger.info(f"📝 Logging configurado - Nivel: {level}")