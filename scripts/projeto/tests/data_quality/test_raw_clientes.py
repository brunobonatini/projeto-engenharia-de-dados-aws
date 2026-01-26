# Criação dos testes para a camada RAW Clientes

import logging

logger = logging.getLogger(__name__)

# =========================
# RAW CLIENTES
# =========================

def test_raw_clientes_nao_vazio(spark):
    logger.info("Iniciando teste: RAW clientes não vazio")

    df = spark.read.parquet(
        "s3a://data-lake-local/raw/clientes/"
    )

    total = df.count()
    logger.info(f"Total de registros RAW clientes: {total}")

    assert total > 0


def test_raw_clientes_sem_id_cliente_nulo(spark):
    logger.info("Iniciando teste: RAW clientes sem id_cliente nulo")

    df = spark.read.parquet(
        "s3a://data-lake-local/raw/clientes/"
    )

    qtd_nulos = df.filter("id_cliente IS NULL").count()
    logger.info(f"Quantidade de id_cliente nulos: {qtd_nulos}")

    assert qtd_nulos == 0


def test_raw_clientes_com_data_processamento(spark):
    logger.info("Iniciando teste: RAW clientes com data_processamento")

    df = spark.read.parquet(
        "s3a://data-lake-local/raw/clientes/"
    )

    logger.info(f"Colunas encontradas: {df.columns}")

    assert "data_processamento" in df.columns

