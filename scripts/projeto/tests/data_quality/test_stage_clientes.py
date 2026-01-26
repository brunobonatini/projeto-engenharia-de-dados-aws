# Criação dos testes para a camada STAGE Clientes

import logging

logger = logging.getLogger(__name__)

# =========================
# STAGE CLIENTES
# =========================

def test_stage_clientes_sem_duplicidade(spark):
    logger.info("Iniciando teste: STAGE clientes sem duplicidade por id_cliente")

    df = spark.read.format("delta").load(
        "s3a://data-lake-local/stage/clientes/"
    )

    duplicados = (
        df.groupBy("id_cliente")
        .count()
        .filter("count > 1")
        .count()
    )

    logger.info(f"Quantidade de id_cliente duplicados: {duplicados}")

    assert duplicados == 0


def test_stage_clientes_com_tipagem(spark):
    logger.info("Iniciando teste: STAGE clientes com tipagem correta")

    df = spark.read.format("delta").load(
        "s3a://data-lake-local/stage/clientes/"
    )

    dtypes = dict(df.dtypes)
    logger.info(f"Tipos das colunas: {dtypes}")

    assert dtypes["id_cliente"] in ("int", "bigint")
    assert dtypes["data_nascimento"] == "date"
