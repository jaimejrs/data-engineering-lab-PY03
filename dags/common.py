"""Constantes compartilhadas entre `dag_bronze_extract.py` e `dag_silver_transform.py`.

Isoladas num módulo à parte, sem nenhum `@dag`/`DAG(...)`: o `DagBag` do Airflow
executa qualquer módulo importado por um arquivo de DAG e recolhe objetos DAG
encontrados nele também — importar `dag_bronze_extract.py` diretamente fazia o
scheduler registrar o dag_id `bronze_extract` duas vezes (uma vindo do próprio
arquivo, outra "encontrada" via import em `dag_silver_transform.py`), gerando
`AirflowDagDuplicatedIdException` (confirmado rodando no Airflow real do
Datalab em 19/07/2026).
"""

from airflow.datasets import Dataset

WATERMARK_VARIABLE = "bronze_last_data_extracao"

# Emitido quando a Bronze de uma data_extracao é validada — a DAG 2
# (dag_silver_transform) usa isso como schedule em vez de horário fixo.
BRONZE_VALIDATED_DATASET = Dataset("bronze://validated")
