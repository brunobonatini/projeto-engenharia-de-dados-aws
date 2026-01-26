import re
import pandas as pd
from typing import Tuple
from src.utils.logger import setup_logger


logger = setup_logger(name="validacao_enderecos")


# =========================
# Regras auxiliares
# =========================

def campo_vazio(valor) -> bool:
    return valor is None or pd.isna(valor) or str(valor).strip() == ""

def validar_cep(cep: str) -> bool:
    if not cep:
        return False
    cep = str(cep).strip()
    return bool(re.fullmatch(r"\d{5}-\d{3}", cep))


def validar_data(data: str) -> bool:
    if not data:
        return False
    try:
        pd.to_datetime(
            data,
            format="%Y-%m-%d %H:%M:%S",
            errors="raise"
        )
        return True
    except Exception:
        return False


# =========================
# Função principal
# =========================

def validar_enderecos(
    df_enderecos: pd.DataFrame,
    ids_clientes_validos: set
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Valida endereços e garante integridade referencial com clientes.
    """

    registros_validos = []
    registros_invalidos = []

    for index, row in df_enderecos.iterrows():
        erros = []

        # =========================
        # id_endereco
        # =========================
        if campo_vazio(row.get("id_endereco")):
            erros.append(("id_endereco", row.get("id_endereco"), "campo obrigatório"))

        # =========================
        # id_cliente (integridade referencial)
        # =========================
        if campo_vazio(row.get("id_cliente")):
            erros.append(("id_cliente", row.get("id_cliente"), "campo obrigatório"))
        elif row.get("id_cliente") not in ids_clientes_validos:
            erros.append(("id_cliente", row.get("id_cliente"), "cliente inexistente"))

        # =========================
        # cep
        # =========================
        if campo_vazio(row.get("cep")) or not validar_cep(row.get("cep")):
            erros.append(("cep", row.get("cep"), "cep inválido"))

        # =========================
        # data_evento
        # =========================
        if campo_vazio(row.get("data_evento")) or not validar_data(row.get("data_evento")):
            erros.append(("data_evento", row.get("data_evento"), "data inválida"))

        # =========================
        # Tratamento de erros
        # =========================
        if erros:
            for campo, valor, motivo in erros:
                logger.warning(
                    f"Linha {index} | Campo: {campo} | Valor: {valor} | Motivo: {motivo}"
                )

            registro = row.to_dict()
            registro["linha"] = index
            registro["motivo_rejeicao"] = "; ".join(
                [f"{campo}: {motivo}" for campo, _, motivo in erros]
            )
            registros_invalidos.append(registro)
        else:
            registros_validos.append(row.to_dict())

    df_validos = pd.DataFrame(registros_validos)
    df_invalidos = pd.DataFrame(registros_invalidos)

    logger.info(
        f"Validação endereços concluída | "
        f"Válidos: {len(df_validos)} | "
        f"Inválidos: {len(df_invalidos)}"
    )

    return df_validos, df_invalidos
