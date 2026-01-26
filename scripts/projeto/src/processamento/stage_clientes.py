from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType
from delta.tables import DeltaTable

from src.utils.spark_session import criar_spark_session
from src.config.settings import settings
from src.utils.logger import setup_logger


def processar_stage_clientes():
    logger = setup_logger(name="stage_clientes")
    spark = criar_spark_session()

    logger.info("Iniciando processamento Stage - Clientes")

    # =========================
    # Paths (S3A + Delta)
    # =========================
    raw_path = (
        f"s3a://{settings.s3_bucket}/"
        f"{settings.s3_raw_path}/clientes/"
    )

    stage_path = (
        f"s3a://{settings.s3_bucket}/"
        f"{settings.s3_stage_path}/clientes/"
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
        .withColumn("id_cliente", F.col("id_cliente").cast(IntegerType()))
        .withColumn("nome", F.col("nome").cast(StringType()))
        .withColumn("email", F.col("email").cast(StringType()))
        .withColumn("cpf", F.col("cpf").cast(StringType()))
        .withColumn("status", F.col("status").cast(StringType()))
        .withColumn("data_nascimento", F.col("data_nascimento").cast(DateType()))
        .withColumn("data_evento", F.col("data_evento").cast(TimestampType()))
    )

    # =========================
    # Filtro mínimo de integridade
    # =========================
    df_tipado = df_tipado.filter(F.col("id_cliente").isNotNull())

    # =========================
    # Deduplicação (último evento)
    # =========================
    window = (
        Window
        .partitionBy("id_cliente")
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
                "target.id_cliente = source.id_cliente"
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

        logger.info("Tabela Delta criada para Stage Clientes")

    logger.info("Stage clientes finalizado com sucesso")

    spark.stop()