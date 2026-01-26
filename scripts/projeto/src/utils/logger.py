import logging
import os
from datetime import datetime

# Função para padronizar os arquivos de log
def setup_logger(
    name: str = "pipeline",
    log_level: int = logging.INFO,
    log_dir: str = "logs"
) -> logging.Logger:
    """
    Configura e retorna um logger com:
    - Formato estruturado
    - Escrita em arquivo e console
    - Histórico por execução (timestamp)
    - Proteção contra duplicação de handlers
    """

    os.makedirs(log_dir, exist_ok=True)

    # Nome do arquivo com timestamp (histórico)
    log_file = os.path.join(
            log_dir,
            f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.propagate = False

    # Evita duplicação de handlers
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
