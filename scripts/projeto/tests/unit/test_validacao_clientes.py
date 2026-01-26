import pandas as pd
from src.validacao.validacao_clientes import validar_clientes

def test_validar_clientes_com_registro_valido():
    df = pd.DataFrame([
        {
            "id_cliente": "1",
            "nome": "Maria Silva",
            "cpf": "123.456.789-10",
            "email": "maria@email.com",
            "data_nascimento": "1990-01-01",
            "status": "ativo",
            "data_evento": "2024-01-10 10:00:00"
        }
    ])

    validos, invalidos = validar_clientes(df)

    assert len(validos) == 1
    assert len(invalidos) == 0


def test_validar_clientes_com_cpf_invalido():
    df = pd.DataFrame([
        {
            "id_cliente": "2",
            "nome": "João Souza",
            "cpf": "12345678910",  # CPF inválido
            "email": "joao@email.com",
            "data_nascimento": "1985-05-20",
            "status": "ativo",
            "data_evento": "2024-01-10 10:00:00"
        }
    ])

    validos, invalidos = validar_clientes(df)

    assert len(validos) == 0
    assert len(invalidos) == 1
    assert "cpf: formato inválido" in invalidos.iloc[0]["motivo_rejeicao"]


def test_validar_clientes_sem_nome():
    df = pd.DataFrame([
        {
            "id_cliente": "3",
            "nome": None,  # Campo obrigatório ausente
            "cpf": "123.456.789-10",
            "email": "teste@email.com",
            "data_nascimento": "1992-03-15",
            "status": "ativo",
            "data_evento": "2024-01-10 10:00:00"
        }
    ])

    validos, invalidos = validar_clientes(df)

    assert len(validos) == 0
    assert len(invalidos) == 1
    assert "nome: campo obrigatório" in invalidos.iloc[0]["motivo_rejeicao"]
