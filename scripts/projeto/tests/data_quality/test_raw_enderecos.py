# Criação das funções de testes da camada RAW Endereços

import logging

logger = logging.getLogger(__name__)

# =========================
# RAW ENDEREÇOS
# =========================

def test_raw_enderecos_nao_vazio(spark):
    logger.info("Iniciando teste: RAW endereços não vazio")

    df = spark.read.parquet(
        "s3a://data-lake-local/raw/enderecos/"
    )

    total = df.count()
    logger.info(f"Total de registros RAW endereços: {total}")

    assert total > 0


def test_raw_enderecos_sem_id_endereco_nulo(spark):
    logger.info("Iniciando teste: RAW endereços sem id_endereco nulo")

    df = spark.read.parquet(
        "s3a://data-lake-local/raw/enderecos/"
    )

    qtd_nulos = df.filter("id_endereco IS NULL").count()
    logger.info(f"Endereços com id_endereco nulo: {qtd_nulos}")

    assert qtd_nulos == 0


def test_raw_enderecos_sem_id_cliente_nulo(spark):
    logger.info("Iniciando teste: RAW endereços sem id_cliente nulo")

    df = spark.read.parquet(
        "s3a://data-lake-local/raw/enderecos/"
    )

    qtd_nulos = df.filter("id_cliente IS NULL").count()
    logger.info(f"Endereços com id_cliente nulo: {qtd_nulos}")

    assert qtd_nulos == 0


def test_raw_enderecos_com_data_processamento(spark):
    logger.info("Iniciando teste: RAW endereços com data_processamento")

    df = spark.read.parquet(
        "s3a://data-lake-local/raw/enderecos/"
    )

    colunas = df.columns
    logger.info(f"Colunas encontradas: {colunas}")

    assert "data_processamento" in colunas
