"""Testes de `src/report_validation.py` — verificação pós-geração do relatório
narrativo (auditoria de rigor científico de 30/07/2026)."""

from src import report_validation as rv


class TestExtrairValoresReais:
    def test_extracts_value_with_thousands_and_cents(self):
        assert rv.extrair_valores_reais("o valor foi de R$ 1.234.567,89 no total") == [1234567.89]

    def test_extracts_value_without_thousands_separator(self):
        assert rv.extrair_valores_reais("custou R$ 500,00") == [500.0]

    def test_extracts_multiple_values(self):
        texto = "entre R$ 100,00 e R$ 300,50, mediana R$ 200,25"
        assert rv.extrair_valores_reais(texto) == [100.0, 300.50, 200.25]

    def test_no_monetary_values_returns_empty_list(self):
        assert rv.extrair_valores_reais("nenhum valor monetário aqui, só texto.") == []


class TestValoresNaoVerificados:
    def test_value_matching_provided_set_is_not_flagged(self):
        texto = "o contrato soma R$ 1.234.567,89."
        suspeitos = rv.valores_nao_verificados(texto, valores_permitidos={1234567.89})
        assert suspeitos == []

    def test_value_within_rounding_tolerance_is_not_flagged(self):
        # LLM arredondou 1234567.894 pra 1.234.567,89 na exibição — dentro da tolerância.
        texto = "totalizando R$ 1.234.567,89"
        suspeitos = rv.valores_nao_verificados(texto, valores_permitidos={1234567.894})
        assert suspeitos == []

    def test_value_absent_from_provided_set_is_flagged(self):
        texto = "o modelo previu R$ 999.999,99 pro próximo trimestre"
        suspeitos = rv.valores_nao_verificados(texto, valores_permitidos={100.0, 200.0})
        assert suspeitos == [999999.99]

    def test_empty_text_or_no_permitted_values_handled_gracefully(self):
        assert rv.valores_nao_verificados("sem número nenhum", valores_permitidos=set()) == []
        assert rv.valores_nao_verificados("", valores_permitidos={100.0}) == []
