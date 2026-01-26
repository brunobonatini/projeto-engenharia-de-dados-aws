# Criação das funções de testes da camada STAGE Endereços

import logging

logger = logging.getLogger(__name__)

# =========================
# STAGE ENDEREÇOS
# =========================

def test_stage_enderecos_nao_vazio(spark):
    logger.info("Iniciando teste: STAGE endereços não vazio")

    df = spark.read.format("delta").load(
        "s3a://data-lake-local/stage/enderecos/"
    )

    total = df.count()
    logger.info(f"Quantidade de registros em stage/enderecos: {total}")

    assert total > 0


def test_stage_enderecos_sem_duplicidade(spark):
    logger.info("Iniciando teste: STAGE endereços sem duplicidade por id_endereco")

    df = spark.read.format("delta").load(
        "s3a://data-lake-local/stage/enderecos/"
    )

    duplicados = (
        df.groupBy("id_endereco")
        .count()
        .filter("count > 1")
        .count()
    )

    logger.info(f"Quantidade de id_endereco duplicados: {duplicados}")

    assert duplicados == 0


def test_stage_enderecos_tipagem(spark):
    logger.info("Iniciando teste: STAGE endereços com tipagem correta")

    df = spark.read.format("delta").load(
        "s3a://data-lake-local/stage/enderecos/"
    )

    dtypes = dict(df.dtypes)
    logger.info(f"Tipos das colunas: {dtypes}")

    assert dtypes["id_endereco"] in ("int", "bigint")
    assert dtypes["id_cliente"] in ("int", "bigint")
    assert dtypes["data_evento"] == "timestamp"


def test_stage_enderecos_integridade_referencial(spark):
    logger.info("Iniciando teste: STAGE endereços com integridade referencial")

    df_end = spark.read.format("delta").load(
        "s3a://data-lake-local/stage/enderecos/"
    )

    df_cli = spark.read.format("delta").load(
        "s3a://data-lake-local/stage/clientes/"
    )

    ids_clientes = [
        r.id_cliente
        for r in df_cli.select("id_cliente").distinct().collect()
    ]

    enderecos_invalidos = df_end.filter(
        ~df_end.id_cliente.isin(ids_clientes)
    ).count()

    logger.info(
        f"Quantidade de endereços com id_cliente inexistente: {enderecos_invalidos}"
    )

    assert enderecos_invalidos == 0
