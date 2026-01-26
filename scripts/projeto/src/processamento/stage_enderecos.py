from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, TimestampType
from delta.tables import DeltaTable

from src.utils.spark_session import criar_spark_session
from src.config.settings import settings
from src.utils.logger import setup_logger


def processar_stage_enderecos():
    logger = setup_logger(name="stage_enderecos")
    spark = criar_spark_session()

    logger.info("Iniciando processamento Stage - Endereços")

    # =========================
    # Paths (S3A + Delta)
    # =========================
    raw_path = (
        f"s3a://{settings.s3_bucket}/"
        f"{settings.s3_raw_path}/enderecos/"
    )

    stage_path = (
        f"s3a://{settings.s3_bucket}/"
        f"{settings.s3_stage_path}/enderecos/"
    )

    # =========================
    # Leitura Raw
    # =========================
    df_raw = spark.read.parquet(raw_path)
    logger.info(f"Registros lidos da Raw: {df_raw.count()}")

    # =========================
    # Tipagem explícita (Stage)
    # =========================
    df_tipado = (
        df_raw
        .withColumn("id_endereco", F.col("id_endereco").cast(IntegerType()))
        .withColumn("id_cliente", F.col("id_cliente").cast(IntegerType()))
        .withColumn("cep", F.col("cep").cast(StringType()))
        .withColumn("logradouro", F.col("logradouro").cast(StringType()))
        .withColumn("numero", F.col("numero").cast(StringType()))
        .withColumn("complemento", F.col("complemento").cast(StringType()))
        .withColumn("bairro", F.col("bairro").cast(StringType()))
        .withColumn("cidade", F.col("cidade").cast(StringType()))
        .withColumn("estado", F.col("estado").cast(StringType()))
        .withColumn("data_evento", F.col("data_evento").cast(TimestampType()))
    )

    # =========================
    # Filtro mínimo de integridade
    # =========================
    df_tipado = df_tipado.filter(
        F.col("id_endereco").isNotNull() &
        F.col("id_cliente").isNotNull()
    )

    # =========================
    # Deduplicação por id_endereco
    # (mantém último evento)
    # =========================
    window = (
        Window
        .partitionBy("id_endereco")
        .orderBy(F.col("data_evento").desc())
    )

    df_stage = (
        df_tipado
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("data_atualizacao", F.current_timestamp())
    )

    logger.info(f"Registros após deduplicação: {df_stage.count()}")

    # =========================
    # Escrita Stage com Delta Lake (MERGE)
    # =========================
    if DeltaTable.isDeltaTable(spark, stage_path):
        logger.info("Tabela Delta encontrada. Executando MERGE incremental.")

        delta_table = DeltaTable.forPath(spark, stage_path)

        (
            delta_table.alias("target")
            .merge(
                df_stage.alias("source"),
                "target.id_endereco = source.id_endereco"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info("Merge incremental concluído com sucesso")

    else:
        logger.info("Tabela Delta não encontrada. Criando tabela Stage.")

        (
            df_stage
            .write
            .format("delta")
            .mode("overwrite")
            .save(stage_path)
        )

        logger.info("Tabela Delta criada para Stage Endereços")

    logger.info("Stage endereços finalizado com sucesso")

    spark.stop()