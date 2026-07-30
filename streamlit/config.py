"""Configuração do painel — conexão Trino e constantes de estilo/paleta.

Lida das mesmas variáveis de ambiente já padronizadas no resto do projeto
(TRINO_HOST/PORT/USER/HTTP_SCHEME/CATALOG, ver src/trino_io.py e
.env.example na raiz) — carregadas antes deste import por app.py via
`load_dotenv()` (raiz do projeto e/ou streamlit/.env local).
"""

import os

# --- Conexão com o Trino ---
TRINO_HOST = os.getenv("TRINO_HOST", "100.69.31.14")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))
TRINO_USER = os.getenv("TRINO_USER", "arthur")
TRINO_HTTP_SCHEME = os.getenv("TRINO_HTTP_SCHEME", "http")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")

# --- Paleta institucional ---
COR_PAGO = "#1B8A3D"
COR_EMPENHADO = "#F5B301"
COR_PREVISTO = "#E74C3C"

# --- Faixas de risco (score de anomalia do Modelo 1) ---
FAIXA_BAIXO = "Baixo (< 0,70)"
FAIXA_MEDIO = "Médio (0,70 a 0,85)"
FAIXA_ALTO = "Alto (> 0,85)"
CORES_FAIXA_RISCO = {
    FAIXA_BAIXO: "#2ecc71",
    FAIXA_MEDIO: "#f1c40f",
    FAIXA_ALTO: "#e74c3c",
}
