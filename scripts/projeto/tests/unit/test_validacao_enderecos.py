import pandas as pd
from src.validacao.validacao_enderecos import validar_enderecos

def test_validar_endereco_com_cliente_existente():
    df_enderecos = pd.DataFrame([
        {
            "id_endereco": "10",
            "id_cliente": "1",
            "cep": "12345-678",
            "logradouro": "Rua A",
            "numero": "100",
            "cidade": "São Paulo",
            "estado": "SP",
            "data_evento": "2024-01-15 12:00:00"
        }
    ])

    ids_clientes_validos = {"1"}

    validos, invalidos = validar_enderecos(df_enderecos, ids_clientes_validos)

    assert len(validos) == 1
    assert len(invalidos) == 0


def test_validar_endereco_com_cliente_inexistente():
    df_enderecos = pd.DataFrame([
        {
            "id_endereco": "11",
            "id_cliente": "999",  # cliente inexistente
            "cep": "12345-678",
            "logradouro": "Rua B",
            "numero": "50",
            "cidade": "Rio de Janeiro",
            "estado": "RJ",
            "data_evento": "2024-01-15 12:00:00"
        }
    ])

    ids_clientes_validos = {"1", "2"}

    validos, invalidos = validar_enderecos(df_enderecos, ids_clientes_validos)

    assert len(validos) == 0
    assert len(invalidos) == 1
    assert "cliente inexistente" in invalidos.iloc[0]["motivo_rejeicao"]


def test_validar_endereco_com_cep_invalido():
    df_enderecos = pd.DataFrame([
        {
            "id_endereco": "12",
            "id_cliente": "1",
            "cep": "12345678",  # CEP inválido
            "logradouro": "Rua C",
            "numero": "200",
            "cidade": "Curitiba",
            "estado": "PR",
            "data_evento": "2024-01-15 12:00:00"
        }
    ])

    ids_clientes_validos = {"1"}

    validos, invalidos = validar_enderecos(df_enderecos, ids_clientes_validos)

    assert len(validos) == 0
    assert len(invalidos) == 1
    assert "cep inválido" in invalidos.iloc[0]["motivo_rejeicao"]


def test_validar_endereco_sem_id_endereco():
    df_enderecos = pd.DataFrame([
        {
            "id_endereco": None,  # campo obrigatório ausente
            "id_cliente": "1",
            "cep": "12345-678",
            "logradouro": "Rua D",
            "numero": "10",
            "cidade": "Florianópolis",
            "estado": "SC",
            "data_evento": "2024-01-15 12:00:00"
        }
    ])

    ids_clientes_validos = {"1"}

    validos, invalidos = validar_enderecos(df_enderecos, ids_clientes_validos)

    assert len(validos) == 0
    assert len(invalidos) == 1
    assert "id_endereco: campo obrigatório" in invalidos.iloc[0]["motivo_rejeicao"]
