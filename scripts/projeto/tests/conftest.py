import sys
import os
import logging
import pytest

# =========================
# Ajuste de PATH
# =========================
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

# =========================
# Fixture de Spark
# =========================
from src.utils.spark_session import criar_spark_session


@pytest.fixture(scope="session")
def spark():
    spark = criar_spark_session(app_name="data-quality-tests")
    yield spark
    spark.stop()


# =========================
# Logging dos testes
# =========================
@pytest.fixture(scope="session", autouse=True)
def configurar_logging_testes():
    log_dir = "logs/tests"
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "tests.log")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    if not any(
        isinstance(h, logging.FileHandler) and h.baseFilename == log_file
        for h in root_logger.handlers
    ):
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
