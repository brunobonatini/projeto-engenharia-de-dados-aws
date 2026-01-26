import re
import pandas as pd
from typing import Tuple
from src.utils.logger import setup_logger


logger = setup_logger(name="validacao_clientes")

# =========================
# Regras auxiliares
# =========================

STATUS_VALIDOS = {"ativo", "inativo", "suspenso"}

def campo_vazio(valor) -> bool:
    return valor is None or pd.isna(valor) or str(valor).strip() == ""

def validar_cpf(cpf: str) -> bool:
    if not cpf:
        return False
    cpf = str(cpf).strip()
    return bool(re.fullmatch(r"\d{3}\.\d{3}\.\d{3}-\d{2}", cpf))


def validar_email(email: str) -> bool:
    if not email:
        return True  # e-mail é opcional
    email = str(email).strip()
    return bool(re.fullmatch(r"[^@]+@[^@]+\.[^@]+", email))


def validar_data(data: str) -> bool:
    if not data:
        return False
    try:
        pd.to_datetime(data, format="%Y-%m-%d", errors="raise")
        return True
    except Exception:
        return False


# =========================
# Função principal
# =========================

def validar_clientes(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Valida os dados de clientes conforme regras do case.

    Retorna:
    - DataFrame com registros válidos
    - DataFrame com registros inválidos e motivo da rejeição
    """

    registros_validos = []
    registros_invalidos = []

    for index, row in df.iterrows():
        erros = []

        # =========================
        # id_cliente
        # =========================
        if campo_vazio(row.get("id_cliente")):
            erros.append(("id_cliente", row.get("id_cliente"), "campo obrigatório"))

        # =========================
        # nome
        # =========================
        if campo_vazio(row.get("nome")):
            erros.append(("nome", row.get("nome"), "campo obrigatório"))

        # =========================
        # cpf
        # =========================
        if campo_vazio(row.get("cpf")):
            erros.append(("cpf", row.get("cpf"), "campo obrigatório"))
        elif not validar_cpf(row.get("cpf")):
            erros.append(("cpf", row.get("cpf"), "formato inválido"))

        # =========================
        # data_nascimento
        # =========================
        if campo_vazio(row.get("data_nascimento")):
            erros.append(("data_nascimento", row.get("data_nascimento"), "campo obrigatório"))
        elif not validar_data(row.get("data_nascimento")):
            erros.append(("data_nascimento", row.get("data_nascimento"), "formato inválido"))

        # =========================
        # status
        # =========================
        if campo_vazio(row.get("status")):
            erros.append(("status", row.get("status"), "campo obrigatório"))
        elif row.get("status") not in STATUS_VALIDOS:
            erros.append(("status", row.get("status"), "valor inválido"))


        # =========================
        # email (opcional)
        # =========================
        if not validar_email(row.get("email")):
            erros.append(("email", row.get("email"), "formato inválido"))

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
        f"Validação clientes concluída | "
        f"Válidos: {len(df_validos)} | "
        f"Inválidos: {len(df_invalidos)}"
    )

    return df_validos, df_invalidos
