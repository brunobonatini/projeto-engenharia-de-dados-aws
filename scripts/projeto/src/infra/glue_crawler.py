import boto3
from src.config.settings import settings


def criar_ou_atualizar_crawler():
    glue = boto3.client(
        "glue",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        endpoint_url=settings.aws_endpoint_url  # LocalStack
    )

    crawler_name = "crawler_clientes_analytics"
    database_name = "clientes"
    s3_target = (
        f"s3://{settings.s3_bucket}/"
        f"{settings.s3_analytics_path}/clientes/"
    )

    try:
        glue.get_crawler(Name=crawler_name)
        print("Crawler já existe. Atualizando...")
        glue.update_crawler(
            Name=crawler_name,
            Role="AWSGlueServiceRoleDefault",
            DatabaseName=database_name,
            Targets={"S3Targets": [{"Path": s3_target}]}
        )
    except glue.exceptions.EntityNotFoundException:
        print("Criando novo crawler...")
        glue.create_crawler(
            Name=crawler_name,
            Role="AWSGlueServiceRoleDefault",
            DatabaseName=database_name,
            Targets={"S3Targets": [{"Path": s3_target}]},
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG"
            }
        )
    
    glue.start_crawler(Name=crawler_name)
    print("Crawler executado com sucesso")
