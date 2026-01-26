from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType
from delta.tables import DeltaTable

from src.utils.spark_session import criar_spark_session
from src.config.settings import settings
from src.utils.logger import setup_logger


def processar_analytics_clientes():
    logger = setup_logger(name="analytics_clientes")
    spark = criar_spark_session()

    logger.info("Iniciando processamento da camada Analytics - Clientes")

    # =========================
    # Paths (S3A)
    # =========================
    clientes_stage_path = (
        f"s3a://{settings.s3_bucket}/"
        f"{settings.s3_stage_path}/clientes/"
    )

    enderecos_stage_path = (
        f"s3a://{settings.s3_bucket}/"
        f"{settings.s3_stage_path}/enderecos/"
    )

    clientes_analytics_path = (
        f"s3a://{settings.s3_bucket}/"
        f"{settings.s3_analytics_path}/clientes/"
    )

    # =========================
    # Leitura da Stage (Delta)
    # =========================
    df_clientes = spark.read.format("delta").load(clientes_stage_path)
    df_enderecos = spark.read.format("delta").load(enderecos_stage_path)

    logger.info(
        f"Registros lidos - clientes: {df_clientes.count()} | "
        f"enderecos: {df_enderecos.count()}"
    )

    # =========================
    # Regra de negócio:
    # Apenas clientes ativos
    # =========================
    df_clientes_ativos = df_clientes.filter(F.col("status") == "ativo")

    # =========================
    # LEFT JOIN
    # - Mantém clientes sem endereço
    # - Mantém múltiplos endereços
    # =========================
    df_join = (
        df_clientes_ativos.alias("c")
        .join(
            df_enderecos.alias("e"),
            on="id_cliente",
            how="left"
        )
        .select(
            F.col("c.id_cliente"),
            F.col("e.id_endereco"),
            F.col("c.nome"),
            F.col("c.email"),
            F.col("c.cpf"),
            F.col("c.status"),
            F.col("c.data_nascimento"),  
            F.col("e.logradouro"),
            F.col("e.bairro"),
            F.col("e.cidade"),
            F.col("e.estado"),
            F.col("e.data_atualizacao")
        )
    )

    # =========================
    # Tratamento de clientes sem endereço
    # =========================
    
    df_tratado = (
        df_join
        # =========================
        # Tratamento de id_endereco
        # =========================
        .withColumn(
            "id_endereco",
            F.when(
                F.col("id_endereco").isNull(),
                F.lit(-1)
            ).otherwise(F.col("id_endereco"))
        )

        # =========================
        # Tratamento de campos textuais
        # =========================
        .withColumn(
            "logradouro",
            F.when(
                (F.col("logradouro").isNull()) |
                (F.trim(F.col("logradouro")) == "") |
                (F.lower(F.col("logradouro")) == "nan"),
                F.lit("Sem endereço")
            ).otherwise(F.col("logradouro"))
        )
        .withColumn("bairro", F.coalesce(F.col("bairro"), F.lit("Sem bairro")))
        .withColumn("cidade", F.coalesce(F.col("cidade"), F.lit("Sem cidade")))
        .withColumn("estado", F.coalesce(F.col("estado"), F.lit("Sem estado")))

        # =========================
        # Tratamento data_atualizacao
        # =========================
        .withColumn(
            "data_atualizacao",
            F.coalesce(
                F.col("data_atualizacao"),
                F.current_timestamp()
            )
        )
    )    

    # =========================
    # Coluna calculada: idade
    # =========================
    df_final = (
        df_tratado
        .withColumn(
            "idade",
            F.floor(
                F.months_between(
                    F.current_date(),
                    F.col("data_nascimento")
                ) / 12
            )
        )
    )

    # =========================
    # Escrita da camada Analytics
    # =========================
    (
        df_final
        .coalesce(1)
        .write
        .mode("overwrite")
        .partitionBy("estado")
        .format("parquet")
        .option("compression", "snappy")
        .save(clientes_analytics_path)
    )

    logger.info("Camada Analytics gravada com sucesso")

    spark.stop()