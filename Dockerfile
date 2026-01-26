# Base de uma distribuição mínima com suporte ao Java
FROM eclipse-temurin:11-jre

# Variáveis de Ambiente
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV DELTA_VERSION=3.2.1
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars/*
ENV SPARK_SQL_EXTENSIONS=io.delta.sql.DeltaSparkSessionExtension
ENV SPARK_SQL_CATALOG_SPARK_CATALOG=org.apache.spark.sql.delta.catalog.DeltaCatalog
ENV IVY_HOME=/root/.ivy2

# Baixar e Instalar o Apache Spark e pacotes do SO
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl python3 python3-pip python3-setuptools procps \
    && curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
	
# Hadoop AWS / S3A Connector
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Permitir que o pip instale pacotes no Python do sistema
ENV PIP_BREAK_SYSTEM_PACKAGES=1

# Instalar Dependências Python e Delta Lake
RUN pip3 install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    delta-spark==${DELTA_VERSION} \
    python-dotenv \
    pandas \
    pytest \
    openpyxl \
    boto3 \
    notebook \
    awscli

# Histórico python
RUN mkdir -p /root/.ipython/profile_default \
    && chmod -R 777 /root/.ipython
ENV IPYTHONDIR=/root/.ipython

# Configurações Adicionais
WORKDIR /repositorio
COPY scripts /repositorio

# Configurar Jupyter Notebook como Entrada Principal
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token="]
