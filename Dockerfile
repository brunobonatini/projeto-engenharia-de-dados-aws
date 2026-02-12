# ===============================
# Base oficial Spark
# ===============================
FROM apache/spark:3.5.3

USER root

# ===============================
# Instalar Python + dependências
# ===============================
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Atualizar pip
RUN pip3 install --upgrade pip

# ===============================
# Hadoop AWS (S3A) para LocalStack
# ===============================
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ===============================
# Dependências Python do Projeto
# ===============================
RUN pip3 install --no-cache-dir \
    pyspark==3.5.3 \
    delta-spark==3.2.1 \
    python-dotenv \
    pandas \
    pytest \
    openpyxl \
    boto3 \
    notebook \
    awscli

# ===============================
# Variáveis Spark / Delta
# ===============================
ENV SPARK_SQL_EXTENSIONS=io.delta.sql.DeltaSparkSessionExtension
ENV SPARK_SQL_CATALOG_SPARK_CATALOG=org.apache.spark.sql.delta.catalog.DeltaCatalog

# ===============================
# Diretório do Projeto
# ===============================
WORKDIR /repositorio
COPY scripts /repositorio

# ===============================
# Jupyter como entrada
# ===============================
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token="]
