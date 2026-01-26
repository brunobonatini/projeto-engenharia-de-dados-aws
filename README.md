[![author](https://img.shields.io/badge/author-brunobonatini-red.svg)](https://www.linkedin.com/in/bsbonatini)

# Projeto: Engenharia de dados com Docker, Python, Spark, Delta, SQL, LocalStack e AWS

Este projeto demonstra a construção de um pipeline completo de Engenharia de Dados, simulando um ambiente próximo ao produtivo, com ingestão, processamento, validação, governança e disponibilização analítica de dados utilizando tecnologias amplamente adotadas no mercado.

O pipeline foi desenvolvido com Apache Spark, Delta Lake e AWS (simulado via LocalStack), seguindo boas práticas de arquitetura de dados, como separação por camadas (Raw, Stage e Analytics), controle de qualidade de dados, versionamento de tabelas, processamento incremental e observabilidade por meio de logs e testes automatizados.

Os dados de entrada são provenientes de um arquivo Excel contendo informações de clientes e endereços, que passam por regras de validação, enriquecimento e transformação até serem disponibilizados para consumo analítico. Todo o ambiente é containerizado com Docker, garantindo reprodutibilidade e facilidade de execução.

Além do pipeline principal, o projeto inclui:

Criação programática do Data Lake

Processamento incremental com Delta Lake (MERGE)

Testes de qualidade de dados com Pytest e Spark

Validações analíticas via Spark SQL e Jupyter Notebook

Estrutura preparada para integração com catálogo de dados e governança

Este projeto foi pensado como um case técnico, evidenciando decisões arquiteturais, preocupação com qualidade, escalabilidade e organização de código, sendo ideal para portfólio e avaliações técnicas em Engenharia de Dados.

# Como Executar o Projeto

Este guia descreve o passo a passo para executar o projeto localmente utilizando Docker, Spark, Delta Lake e LocalStack, sem necessidade de uma conta AWS real.

# 1. Pré-requisitos

Antes de iniciar, certifique-se de ter instalado:

	* Docker Desktop (versão 20+)

	* Docker Compose (v2+)

	* Git (opcional, para clonar o repositório)

⚠️ Não é necessário instalar Java, Spark ou Python localmente.
Todo o ambiente é provisionado via Docker.

Manual de instalação do Docker Desktop: https://docs.docker.com/desktop/setup/install/windows-install/

Verificar o WSL do Windows: https://docs.docker.com/desktop/setup/install/windows-install/#wsl-verification-and-setup


# 2. Estrutura do Projeto

<img width="808" height="534" alt="image" src="https://github.com/user-attachments/assets/2f1d4fc2-dbdd-4f5d-bf88-0371d290647e" />

# 3. Configuração de Variáveis de Ambiente

Para execução local com LocalStack, crie o arquivo .env na pasta /scripts/projeto/:

<img width="461" height="274" alt="image" src="https://github.com/user-attachments/assets/7205a345-623d-4c3e-8e72-3fdaefaab2a3" />

Para execução em AWS real, basta ajustar as credenciais e remover o endpoint

# 4. Subir o Ambiente com Docker

O Docker ou Docker Desktop precisa estar em execução.

Na raiz do projeto (pasta seu-local/projeto-engenharia-de-dados-aws), abra um terminal ou prompt de comando e execute:

docker compose up -d --build

Esse comando irá:

	* Construir a imagem com Spark + Delta Lake

	* Subir o LocalStack (simulando o S3)

	* Iniciar o Jupyter Notebook

	* Montar o projeto dentro do container
	

# 5. Acessar o Container

Para acessar o terminal do container Spark:

docker exec -it projeto-aws-ed bash

Dentro do container, navegue até o a pasta /repositorio/projeto:

cd /repositorio/projeto


# 6. Criar a Estrutura do Data Lake (opcional)

Caso queira criar manualmente o bucket no LocalStack:

aws --endpoint-url=http://localstack:4566 s3 mb s3://data-lake-local

O pipeline cria essa estrutura automaticamente.


# 7. Executar o Pipeline Completo

Para rodar todo o fluxo (Ingestão → Stage → Analytics):

python3 pipeline.py

Esse processo executa, na ordem:

	* Ingestão Raw a partir do Excel

	* Escrita na camada Raw (Parquet + Snappy + Partições)

	* Processamento Stage com Delta Lake

	* Geração da camada Analytics
	
	* Geração de arquivos de logs


# 8. Validar os Dados no S3 (LocalStack)

Listar o conteúdo do Data Lake:

aws --endpoint-url=http://localstack:4566 s3 ls s3://data-lake-local/

Exemplo de estrutura esperada:

raw/
stage/
analytics/


# 9. Validação via Jupyter Notebook

Acesse o Jupyter Notebook no navegador ou via Docker Desktop:

http://localhost:8888

Na pasta:

/projeto/notebooks/

Você encontrará notebooks para:

	* Validação da ingestão Raw

	* Validação da camada Stage

	* Validação da camada Analytics

	* Simulação de catálogo com Spark SQL
	

# 10. Consultar Dados com Spark SQL

http://localhost:8888

Na pasta /projeto/src/athena contém uma query analítica para Athena:

Esta query pode ser executada no Jupyter Notebook:

/projeto/notebooks/catalogo_spark_sql.ipynb


# 11. Executação dos Testes (adicional)

No terminal ou prompt de comando, na raíz do projeto (pasta /repositorio/projeto/), execute os testes de validação:

pytest

Você deve ver os testes de qualidade passando com sucesso no arquivo de log gerado na pasta projeto/logs/tests.

tests.log

Data Quality tests

	* Log consolidado

	* Visão do estado do dado

	* Ideal para auditoria e monitoramento

Você deve ver os testes de Unit passando com sucesso no arquivo de log gerado na pasta projeto/logs

validacao_clientes.log

validacao_enderecos.log

Unit tests

	* Log por módulo

	* Foco em regra de negócio

	* Arquivos pequenos e específicos


# Resultados Esperados

Ao final das execuções:

	* Dados estarão organizados em Raw, Stage e Analytics

	* Tabelas Delta criadas corretamente

	* Partições reconhecidas
	
	* Banco de dados analítico criado com Spark

	* Query para Athena executada com sucesso no banco de dados analítico

	* Testes realizados com sucesso

	* Pipeline totalmente reproduzível localmente


# 12. Encerrar o Ambiente

Sair do ambiente no Docker:

No terminal ou prompt de comando, em /repositorio/projeto execute:

exit

Para parar os containers:

docker compose down

Para remover as imagens criadas (opcional):

docker rmi projeto-aws-etl-spark

docker rmi localstack/localstack

======================================================================================================================	

# Decisões Técnicas

Esta seção descreve as principais decisões técnicas adotadas no projeto, bem como os motivos por trás de cada escolha, considerando boas práticas de Engenharia de Dados, escalabilidade e clareza arquitetural.


1. Containerização com Docker

O projeto foi containerizado para garantir:

	* Reprodutibilidade do ambiente

	* Padronização das dependências

	* Facilidade de execução em qualquer máquina

O Dockerfile inclui:

	* Python

	* Pandas

	* Python-dotenv

	* Pytest

	* Openpyxl

	* Java + Spark

	* Conector S3A

	* Delta Lake

	* AWS CLI

	* Boto3

	* Jupyter Notebook para validações


2. Uso do Apache Spark como motor de processamento

O Apache Spark foi escolhido como engine principal por oferecer:

	* Processamento distribuído e escalável

	* API madura para transformação de dados estruturados

	* Integração nativa com formatos colunares (Parquet)

	* Suporte avançado a camadas analíticas (SQL, DataFrames)

Mesmo tratando-se de um case com volume reduzido, a escolha do Spark simula um cenário real de produção, onde o crescimento do volume de dados é esperado.


3. Arquitetura em camadas (Raw, Stage e Analytics)

O Data Lake foi estruturado seguindo um padrão amplamente adotado no mercado:

	🔹 Raw

		* Armazena os dados brutos, sem alterações semânticas

		* Mantém rastreabilidade total da origem dos dados

		* Dados particionados por data_processamento

	🔹 Stage

		* Aplica tipagem, deduplicação e regras mínimas de integridade

		* Utiliza Delta Lake para permitir cargas incrementais

		* Representa a camada de dados confiáveis (clean data)

	🔹 Analytics

		* Camada orientada ao consumo analítico

		* Contém regras de negócio (ex: clientes ativos, idade calculada)

		* Dados particionados por estado para otimizar consultas

Essa separação garante governança, auditabilidade e facilidade de manutenção.


4. Adoção do Delta Lake na camada Stage

O Delta Lake foi utilizado na camada Stage para resolver problemas comuns em Data Lakes tradicionais:

	* Falta de controle transacional

	* Dificuldade em cargas incrementais

	* Ausência de versionamento

Com Delta Lake foi possível:

	* Realizar MERGE incremental por chave de negócio

	* Garantir consistência ACID

	* Permitir evolução futura do schema

	* Simular comportamento de tabelas de Data Warehouse


5. Validação de dados antes da ingestão

As regras de validação foram implementadas antes da escrita na camada Raw, utilizando Pandas, por três motivos principais:

	* Simplicidade para validações linha a linha

	* Clareza na geração de logs de rejeição

	* Separação explícita entre dados válidos e inválidos

As validações incluem:

	* Campos obrigatórios

	* Formato de CPF, e-mail e CEP

	* Integridade referencial entre clientes e endereços

	* Validação de datas

Essa abordagem evita a propagação de dados inválidos para camadas posteriores.


6. Testes automatizados para regras de validação

Foram criados testes unitários com pytest para as funções de validação, garantindo que:

	* As regras de negócio sejam reproduzíveis

	* Alterações futuras não quebrem comportamentos esperados

	* O projeto tenha maior confiabilidade e manutenibilidade

	* A decisão de testar validações (e não apenas transformações Spark) reflete práticas reais de projetos maduros.


7. Uso do LocalStack como alternativa ao AWS real

Para permitir desenvolvimento local sem dependência da AWS real, foi utilizado o LocalStack, simulando:

	* Amazon S3

	* Estrutura de Data Lake

	* Permissões e endpoints

Essa escolha permitiu:

	* Execução do pipeline 100% local

	* Redução de custos

	* Facilidade de reprodução do ambiente

Limitações do LocalStack, como a ausência do AWS Glue completo, foram tratadas com soluções alternativas.


8. Simulação de catálogo de dados com Spark SQL

Devido à limitação do LocalStack em emular completamente o AWS Glue, o catálogo de dados foi simulado usando Spark SQL:

	* Criação de tabelas externas via CREATE TABLE USING PARQUET

	* Gerenciamento de partições com MSCK REPAIR TABLE

	* Consultas analíticas diretamente via Spark SQL

Essa abordagem mantém o conceito de Data Catalog, mesmo fora da AWS real.


9. Uso do Parquet como formato de armazenamento

	* O formato Parquet foi escolhido por:

	* Armazenamento colunar

	* Compressão eficiente (Snappy)

	* Leitura seletiva de colunas

	* Ampla compatibilidade com ferramentas analíticas

Esse formato é padrão de mercado para Data Lakes modernos.


10. Orquestração simples via Python com pipeline.py

O pipeline foi orquestrado por um script Python central (pipeline.py), responsável por:

	* Criar a estrutura do Data Lake

	* Executar ingestão, stage e analytics em ordem

	* Centralizar a execução do fluxo completo

Essa abordagem mantém o projeto simples, porém facilmente evolutiva para ferramentas como Airflow ou Step Functions.


11. Uso do Pytest para Testes Automatizados

Para garantir a qualidade dos dados e a confiabilidade do pipeline, foi adotado o Pytest como framework de testes automatizados.

Motivações da escolha:

	* Framework padrão do ecossistema Python, amplamente utilizado na indústria

	* Sintaxe simples e expressiva, facilitando leitura e manutenção dos testes

	* Excelente integração com Spark, DataFrames, fixtures e pipelines de dados

	* Suporte nativo a fixtures reutilizáveis, permitindo compartilhar a SparkSession entre testes

	* Facilidade de execução local, em containers Docker e em pipelines de CI/CD

Estratégia de testes adotada:

	* Testes Unitários (tests/unit)
	Responsáveis por validar regras de negócio puras, sem dependência do Data Lake.

	* Testes de Qualidade de Dados (tests/data_quality)
	Responsáveis por validar os dados reais persistidos no Data Lake, após execução do pipeline.

Uso do conftest.py e Fixtures

Foi criado um arquivo conftest.py para centralizar:

	* Configuração do PYTHONPATH

	* Criação e destruição da SparkSession

	* Compartilhamento eficiente da sessão entre testes


# Considerações finais

As decisões técnicas priorizaram:

	* Boas práticas de Engenharia de Dados

	* Clareza arquitetural

	* Simulação de cenários reais de produção

	* Facilidade de entendimento para fins de avaliação técnica

O projeto foi desenvolvido pensando em escalabilidade, governança e qualidade de dados, mesmo sendo um ambiente local e controlado.
