"""Guarda contra deriva de configuração entre os dois docker-compose do
lakehouse (`docker-compose.yml` na raiz — stack autônomo — e
`deploy/server-lakehouse/docker-compose.yml` — overlay do servidor). Item "Dois
composes" de docs/03-pendencias-e-melhorias.md: os dois arquivos existem por
motivo real (topologias diferentes — o servidor não tem HDFS próprio), mas os
limites de recurso dos 4 serviços do overlay lakehouse (hive-metastore,
spark-master, spark-worker, trino) deveriam ser sempre iguais nos dois. Antes
disso, garantir isso era "disciplina manual"; agora é um teste — muda um lado
e esquece o outro, o CI fica vermelho.
"""

import os

import pytest

yaml = pytest.importorskip("yaml")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_COMPOSE = os.path.join(ROOT, "docker-compose.yml")
SERVER_COMPOSE = os.path.join(ROOT, "deploy", "server-lakehouse", "docker-compose.yml")

# Serviços do overlay lakehouse presentes (com o mesmo papel) nos dois
# arquivos — nomes de container diferem de propósito (datalab_* vs
# lakehouse_*), não comparados aqui.
SHARED_SERVICES = ["hive-metastore", "spark-master", "spark-worker", "trino", "superset"]


def _load(path):
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@pytest.fixture(scope="module")
def compose_files():
    return _load(ROOT_COMPOSE)["services"], _load(SERVER_COMPOSE)["services"]


class TestResourceLimitsInSync:
    @pytest.mark.parametrize("service", SHARED_SERVICES)
    def test_mem_limit_matches(self, compose_files, service):
        root, server = compose_files
        assert root[service].get("mem_limit") == server[service].get("mem_limit"), (
            f"'{service}': mem_limit divergiu entre docker-compose.yml e " f"deploy/server-lakehouse/docker-compose.yml"
        )

    @pytest.mark.parametrize("service", SHARED_SERVICES)
    def test_cpus_matches(self, compose_files, service):
        root, server = compose_files
        assert root[service].get("cpus") == server[service].get("cpus"), (
            f"'{service}': cpus divergiu entre docker-compose.yml e " f"deploy/server-lakehouse/docker-compose.yml"
        )


class TestHealthcheckInSync:
    def test_hive_metastore_healthcheck_matches(self, compose_files):
        root, server = compose_files
        assert root["hive-metastore"].get("healthcheck") == server["hive-metastore"].get("healthcheck")


class TestSharedServicesPresent:
    @pytest.mark.parametrize("service", SHARED_SERVICES)
    def test_service_exists_in_both_composes(self, compose_files, service):
        root, server = compose_files
        assert service in root, f"'{service}' sumiu de docker-compose.yml"
        assert service in server, f"'{service}' sumiu de deploy/server-lakehouse/docker-compose.yml"
