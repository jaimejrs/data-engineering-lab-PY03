"""Configuração compartilhada do MLflow tracking — usada pelos dois modelos
(`models/anomaly_detection.py`, `models/payment_forecast.py`, tarefa 29).

Sem servidor MLflow dedicado no stack (não fazia parte do escopo original) —
usa o backend de arquivo local por padrão, gravado em `models/artifacts/mlruns/`
(já coberto por `models/artifacts/` no `.gitignore`, mesmo padrão dos `.joblib`
dos modelos). `MLFLOW_TRACKING_URI` sobrescreve para apontar a um servidor real
(ex: `http://mlflow:5000`) sem mudar código, se o time decidir subir um depois.
"""

import os

import mlflow

_ARTIFACTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models", "artifacts")

DEFAULT_TRACKING_URI = "file:" + os.path.join(_ARTIFACTS_DIR, "mlruns")


def configure_mlflow(experiment_name: str) -> None:
    """Aponta o MLflow para o tracking URI configurado e seleciona/cria o experimento.

    Chamar uma vez no início de `run()` de cada modelo, antes de `mlflow.start_run()`.
    """
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_TRACKING_URI)
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
