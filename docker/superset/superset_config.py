"""Config mínima do Superset — lida via PYTHONPATH (ver Dockerfile). Segue o
padrão oficial de variáveis de ambiente do próprio projeto Superset
(DATABASE_DIALECT/USER/PASSWORD/HOST/PORT/DB), sem Redis/Celery — não
precisamos de cache assíncrono pesado pra um painel interno pequeno.
"""

import os

DATABASE_DIALECT = os.environ.get("DATABASE_DIALECT", "postgresql")
DATABASE_USER = os.environ.get("DATABASE_USER", "superset")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD", "superset")
DATABASE_HOST = os.environ.get("DATABASE_HOST", "postgres")
DATABASE_PORT = os.environ.get("DATABASE_PORT", "5432")
DATABASE_DB = os.environ.get("DATABASE_DB", "superset")

SQLALCHEMY_DATABASE_URI = (
    f"{DATABASE_DIALECT}://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_DB}"
)

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "changeme-dev-only-secret-key")

# Permite que o dashboard seja embutido (ex: iframe interno) se algum dia
# precisar — não expõe nada publicamente sozinho, o acesso já é restrito pela
# rede privada (Tailscale), mesma lógica de segurança do resto do projeto.
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True,
}
