from src.utils.logger import setup_logger
from src.infra.data_lake import criar_data_lake
from src.ingestao.ingestao_raw import ingestao_raw
from src.processamento.stage_clientes import processar_stage_clientes
from src.processamento.stage_enderecos import processar_stage_enderecos
from src.processamento.analytics_clientes import processar_analytics_clientes
from datetime import datetime

# Função principal de execução do Pipeline
def main():
    
    # Definição do arquivo de log do pipeline
    logger = setup_logger(
        name="pipeline_geral",
        log_dir="logs/pipeline"    
    )

    logger.info("========================================")
    logger.info("INICIANDO PIPELINE DE ENGENHARIA DE DADOS")
    logger.info("========================================")

    try:
        # =========================
        # Data Lake
        # =========================
        logger.info("Etapa 0 - Criação do Data Lake")
        criar_data_lake()


        # =========================
        # RAW
        # =========================
        logger.info("Etapa 1 - Ingestão Raw")
        ingestao_raw()

        # =========================
        # STAGE
        # =========================
        logger.info("Etapa 2 - Stage Clientes")
        processar_stage_clientes()

        logger.info("Etapa 3 - Stage Endereços")
        processar_stage_enderecos()

        # =========================
        # ANALYTICS
        # =========================
        logger.info("Etapa 4 - Analytics Clientes")
        processar_analytics_clientes()

        logger.info("========================================")
        logger.info("PIPELINE FINALIZADO COM SUCESSO")
        logger.info("========================================")

    except Exception as e:
        logger.error("Erro durante a execução do pipeline", exc_info=True)
        raise e

if __name__ == "__main__":
    main()
