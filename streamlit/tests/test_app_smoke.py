"""Teste de fumaça (achado 6.3 da análise crítica de 30/07/2026): carrega o
painel inteiro (as 4 abas) com `AppTest` e garante que nenhuma exceção Python
é levantada, sem depender de um Trino real — mocka só a camada de conexão
(`db.get_connection`), não `db.run_query` em si, pra funcionar
independentemente de qual módulo já importou `run_query` antes (evita a
armadilha de `from db import run_query` copiar a referência antiga).

Não simula o clique em "Gerar novo relatório com IA" (aba Resumo) de
propósito — isso dispara uma chamada real à OpenAI, que este teste não deve
fazer nem mockar (mockar o LLM não pegaria o tipo de regressão que interessa
aqui: erro de Python no carregamento inicial das 4 abas, que já pegou pelo
menos uma regressão real nesta mesma aplicação — o upgrade do Streamlit que
quebrou o CSS dos filtros, achado que motivou este teste)."""

import sys
from pathlib import Path

import pandas as pd
import pytest

STREAMLIT_DIR = Path(__file__).resolve().parent.parent
if str(STREAMLIT_DIR) not in sys.path:
    sys.path.insert(0, str(STREAMLIT_DIR))

import db  # noqa: E402

_ORGAOS = ["SECRETARIA TESTE A", "SECRETARIA TESTE B", "SECRETARIA TESTE C"]


def _fixture_para(sql: str) -> pd.DataFrame:
    """Devolve um DataFrame com o formato esperado pra cada query real do
    painel, identificada por um trecho de SQL exclusivo dela. Checagens mais
    específicas vêm primeiro pra não colidir com queries mais genéricas que
    compartilham parte do texto."""
    if "SELECT DISTINCT ano FROM iceberg.gold.fato_contrato" in sql:
        return pd.DataFrame({"ano": [2025, 2026]})
    if "SELECT DISTINCT nome, sigla" in sql:
        return pd.DataFrame({"nome": _ORGAOS, "sigla": ["STA", "STB", "STC"]})
    if "approx_percentile(score_anomalia" in sql:
        return pd.DataFrame({"cutoff": [0.7]})
    if "data_mais_recente" in sql:
        return pd.DataFrame({"data_mais_recente": [pd.Timestamp("2026-06-03")]})

    if "n_registros" in sql:
        return pd.DataFrame(
            {
                "ano": [2026, 2026, 2026],
                "mes": [1, 2, 3],
                "n_registros": [500, 520, 480],
                "valor_pago": [1_000_000.0, 1_100_000.0, 900_000.0],
                "valor_empenhado": [1_500_000.0, 1_400_000.0, 1_300_000.0],
            }
        )
    if "GROUP BY dorg.nome, dorg.sigla" in sql:
        return pd.DataFrame(
            {
                "nome_orgao": _ORGAOS,
                "sigla_orgao": ["STA", "STB", "STC"],
                "total_empenhado": [1_000_000.0, 2_000_000.0, 500_000.0],
                "total_pago": [800_000.0, 1_000_000.0, 500_000.0],
            }
        )
    if "SUM(fc.valor_pago) AS total_pago" in sql:
        return pd.DataFrame({"total_empenhado": [3_500_000.0], "total_pago": [2_300_000.0]})

    if "COUNT(DISTINCT o.codigo) AS total_orgaos" in sql:
        return pd.DataFrame({"total_orgaos": [3]})
    if "SELECT DISTINCT ano_previsto" in sql:
        return pd.DataFrame({"ano_previsto": [2026]})
    if "SUM(valor_previsto_p10) AS p10" in sql:
        return pd.DataFrame(
            {
                "trimestre_previsto": [1, 3],
                "p10": [80.0, 90.0],
                "p50": [100.0, 110.0],
                "p90": [120.0, 130.0],
                "n_orgaos": [2, 3],
            }
        )
    if "GROUP BY dt.ano, dt.trimestre" in sql:
        return pd.DataFrame({"ano": [2025, 2026], "trimestre": [4, 1], "valor": [1_000_000.0, 1_100_000.0]})
    if "GROUP BY dt.trimestre" in sql:
        return pd.DataFrame({"trimestre": [1, 2], "valor": [900_000.0, 950_000.0]})
    if "SELECT nome_orgao, valor_previsto_p10, valor_previsto_p50, valor_previsto_p90" in sql:
        return pd.DataFrame(
            {
                "nome_orgao": _ORGAOS[:2],
                "valor_previsto_p10": [80.0, 70.0],
                "valor_previsto_p50": [100.0, 90.0],
                "valor_previsto_p90": [120.0, 110.0],
            }
        )

    if "sac.flag_anomalia" in sql:
        return pd.DataFrame(
            {
                "id_contrato_origem": ["C1", "C2"],
                "ano": [2026, 2026],
                "num_spu": ["S1", "S2"],
                "valor_contrato": [500_000.0, 300_000.0],
                "valor_empenhado": [500_000.0, 300_000.0],
                "valor_pago": [400_000.0, 200_000.0],
                "status": ["Vigente", "Vigente"],
                "flag_emergency": [False, True],
                "score_anomalia": [0.90, 0.75],
                "flag_anomalia": [True, True],
                "model_version": ["v1", "v1"],
                "nome_orgao": [_ORGAOS[0], _ORGAOS[1]],
                "sigla_orgao": ["STA", "STB"],
                "nome_municipio": ["Fortaleza", "Fortaleza"],
                "nome_credor": ["CREDOR A", "CREDOR B"],
                "cnpj_cpf": ["00000000000100", "00000000000200"],
                "tipo_credor": ["PJ", "PJ"],
                "historico_infringement": [False, False],
                "descricao_modalidade": ["Pregão", "Dispensa"],
            }
        )
    if "fc.score_anomalia >= 0.49" in sql:
        return pd.DataFrame(
            {
                "id_contrato_origem": ["C1", "C2"],
                "ano": [2026, 2026],
                "valor_contrato": [500_000.0, 300_000.0],
                "valor_pago": [400_000.0, 200_000.0],
                "status": ["Vigente", "Vigente"],
                "flag_emergency": [False, True],
                "score_anomalia": [0.90, 0.55],
                "nome_orgao": [_ORGAOS[0], _ORGAOS[1]],
                "nome_credor": ["CREDOR A", "CREDOR B"],
                "tipo_credor": ["PJ", "PJ"],
                "descricao_modalidade": ["Pregão", "Dispensa"],
            }
        )
    if "COUNT(DISTINCT dorg.nome) AS total_orgaos" in sql:
        return pd.DataFrame({"valor_total": [5_000_000.0], "total_orgaos": [3]})
    if "n_anomalos" in sql:
        return pd.DataFrame({"ano": [2025, 2026], "total_contratos": [1000, 900], "n_anomalos": [10, 8]})
    if "COUNT(*) AS total_contratos" in sql:
        return pd.DataFrame({"nome_orgao": _ORGAOS, "total_contratos": [100, 80, 50]})

    if "SELECT COUNT(*) AS n" in sql:
        return pd.DataFrame({"n": [2]})
    if "SUM(valor_previsto_p50) AS total" in sql:
        return pd.DataFrame({"total": [500_000.0]})

    raise AssertionError(f"Nenhum fixture cadastrado em test_app_smoke.py pra esta query — trecho: {sql[:200]!r}")


class _FakeCursor:
    def __init__(self):
        self.description = []
        self._rows = []

    def execute(self, sql, *args, **kwargs):
        df = _fixture_para(sql)
        self.description = [(c,) for c in df.columns]
        self._rows = list(df.itertuples(index=False, name=None))

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def cursor(self):
        return _FakeCursor()


@pytest.fixture(autouse=True)
def _mock_trino(monkeypatch):
    monkeypatch.setattr(db, "get_connection", lambda: _FakeConnection())
    db.run_query.clear()  # cache de execuções anteriores (outros testes/sessões locais)


def test_app_carrega_sem_excecao_com_dado_mockado():
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_file(str(STREAMLIT_DIR / "app.py"), default_timeout=60)
    at.run()

    assert not at.exception, f"Exceção ao carregar o painel: {at.exception}"
    assert len(at.tabs) == 4
