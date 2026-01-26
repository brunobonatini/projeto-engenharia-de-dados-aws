import boto3
from botocore.exceptions import ClientError
from src.config.settings import settings
from src.utils.logger import setup_logger


def criar_data_lake():
    """
    Cria o bucket do Data Lake e estrutura base (raw, stage, analytics).
    Funciona tanto para LocalStack quanto AWS real.
    """
    logger = setup_logger(name="data_lake")

    s3 = boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        endpoint_url=settings.aws_endpoint_url  # None para AWS real
    )

    bucket = settings.s3_bucket

    # =========================
    # Criar bucket (se não existir)
    # =========================
    try:
        s3.head_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' já existe")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code in ["404", "NoSuchBucket"]:
            logger.info(f"Criando bucket '{bucket}'")

            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={
                    "LocationConstraint": settings.aws_region
                }
            )
        else:
            raise

    logger.info("Data Lake inicializado com sucesso")
