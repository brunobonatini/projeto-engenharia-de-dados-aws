from dataclasses import dataclass
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


@dataclass
class Settings:
    # Ambiente
    env: str = os.getenv("ENV", "dev")

    # AWS
    aws_region: str = os.getenv("AWS_REGION", "sa-east-1")
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY")

    # Endpoint (LocalStack ou AWS real)
    aws_endpoint_url: str | None = os.getenv("AWS_ENDPOINT_URL")

    # S3 paths (prefixos)
    s3_bucket: str = os.getenv("S3_BUCKET", "data-lake-local")
    s3_raw_path: str = os.getenv("S3_RAW_PATH", "raw")
    s3_stage_path: str = os.getenv("S3_STAGE_PATH", "stage")
    s3_analytics_path: str = os.getenv("S3_ANALYTICS_PATH", "analytics")

    # Processamento
    data_processamento: str = os.getenv(
        "DATA_PROCESSAMENTO",
        datetime.now().strftime("%Y-%m-%d")
    )


settings = Settings()
