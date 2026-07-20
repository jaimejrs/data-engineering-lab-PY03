"""Testes das funções puras do carregador do DW (tarefas 15/16)."""

import math

import pandas as pd

from src.loaders.dw_loader import _clean, _to_year


class TestClean:
    def test_nan_becomes_none(self):
        assert _clean(float("nan")) is None

    def test_none_stays_none(self):
        assert _clean(None) is None

    def test_regular_value_passes_through(self):
        assert _clean("abc") == "abc"
        assert _clean(42) == 42
        assert _clean(0.0) == 0.0


class TestToYear:
    def test_extracts_year_from_iso_date(self):
        result = _to_year(pd.Series(["2026-07-19", "2022-01-10"]))
        assert list(result) == [2026, 2022]

    def test_blank_value_becomes_nan(self):
        result = _to_year(pd.Series(["", "2026-07-19"]))
        assert math.isnan(result.iloc[0])
        assert result.iloc[1] == 2026
