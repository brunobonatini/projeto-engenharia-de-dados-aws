import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.settings import settings
from src.utils.logger import setup_logger
from src.utils.spark_session import criar_spark_session
from src.validacao.validacao_clientes import validar_clientes
from src.validacao.validacao_enderecos import validar_enderecos


def ingestao_raw():
    logger = setup_logger(name="ingestao_raw")
    spark: SparkSession = criar_spark_session()

    logger.info("Iniciando ingestão Raw")

    # Caminho do Excel dentro do container
    path = "data/dados_entrada.xlsx"

    # =========================
    # CLIENTES
    # =========================
    logger.info("Lendo aba CLIENTES do Excel")
    df_clientes = pd.read_excel(
        path,
        sheet_name="clientes",
        dtype=str
    )

    clientes_validos, clientes_invalidos = validar_clientes(df_clientes)

    ids_clientes_validos = (
        set(clientes_validos["id_cliente"])
        if not clientes_validos.empty
        else set()
    )

    if not clientes_validos.empty:
        logger.info("Escrevendo clientes válidos na camada RAW")

        df_clientes_spark = (
            spark.createDataFrame(clientes_validos)
            .withColumn("data_processamento", F.lit(settings.data_processamento))
        )
            
        (
            df_clientes_spark
            .write
            .mode("append")
            .partitionBy("data_processamento")
            .format("parquet")
            .option("compression", "snappy")
            .save(
                f"s3a://{settings.s3_bucket}/"
                f"{settings.s3_raw_path}/clientes/"
            )
        )

    else:
        logger.warning("Nenhum cliente válido encontrado")

    # =========================
    # ENDEREÇOS
    # =========================
    logger.info("Lendo aba ENDEREÇOS do Excel")
    df_enderecos = pd.read_excel(
        path,
        sheet_name="enderecos",
        dtype=str
    )

    end_validos, end_invalidos = validar_enderecos(
        df_enderecos,
        ids_clientes_validos
    )

    if not end_validos.empty:
        logger.info("Escrevendo endereços válidos na camada RAW")

        df_enderecos_spark = (
            spark.createDataFrame(end_validos)
            .withColumn(
                "data_processamento",
                F.lit(settings.data_processamento)
            )
        )

        (
            df_enderecos_spark
            .write
            .mode("append")
            .partitionBy("data_processamento")
            .format("parquet")
            .option("compression", "snappy")
            .save(
                f"s3a://{settings.s3_bucket}/"
                f"{settings.s3_raw_path}/enderecos/"
            )
        )

    else:
        logger.warning("Nenhum endereço válido encontrado")

    logger.info("Ingestão Raw finalizada com sucesso")

    logger.info(f"Clientes válidos gravados: {df_clientes_spark.count()}")

    logger.info(f"Endereços válidos gravados: {df_enderecos_spark.count()}")

    spark.stop()