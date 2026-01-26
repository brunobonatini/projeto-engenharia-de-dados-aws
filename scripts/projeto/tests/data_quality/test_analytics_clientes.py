# Criação dos testes para a camada ANALYTICS Clientes

import logging

logger = logging.getLogger(__name__)

# =========================
# ANALYTICS CLIENTES
# =========================

def test_analytics_nao_vazio(spark):
    logger.info("Iniciando teste: Analytics clientes não vazio")

    df = spark.read.parquet(
        "s3a://data-lake-local/analytics/clientes/"
    )

    total = df.count()
    logger.info(f"Total de registros Analytics clientes: {total}")

    assert total > 0


def test_analytics_somente_clientes_ativos(spark):
    logger.info("Iniciando teste: Analytics somente clientes ativos")

    df = spark.read.parquet(
        "s3a://data-lake-local/analytics/clientes/"
    )

    qtd_invalidos = df.filter("status != 'ativo'").count()
    logger.info(f"Clientes com status diferente de 'ativo': {qtd_invalidos}")

    assert qtd_invalidos == 0


def test_analytics_idade_valida(spark):
    logger.info("Iniciando teste: Analytics idade válida")

    df = spark.read.parquet(
        "s3a://data-lake-local/analytics/clientes/"
    )

    qtd_invalidos = df.filter("idade < 0 OR idade IS NULL").count()
    logger.info(f"Registros com idade inválida: {qtd_invalidos}")

    assert qtd_invalidos == 0


def test_analytics_colunas_obrigatorias(spark):
    logger.info("Iniciando teste: Analytics colunas obrigatórias")

    df = spark.read.parquet(
        "s3a://data-lake-local/analytics/clientes/"
    )

    colunas = set(df.columns)
    obrigatorias = {
        "id_cliente",
        "nome",
        "estado",
        "idade"
    }

    logger.info(f"Colunas encontradas: {sorted(colunas)}")
    logger.info(f"Colunas obrigatórias esperadas: {sorted(obrigatorias)}")

    assert obrigatorias.issubset(colunas)
