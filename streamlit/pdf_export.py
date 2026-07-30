"""Exporta o relatório narrativo (Markdown) da IA generativa em PDF —
conversão pura Python (Markdown -> HTML -> PDF via xhtml2pdf), sem depender de
binário externo (wkhtmltopdf/Chromium) — mantém o Dockerfile do painel
simples (só pip install, sem apt-get de sistema).
"""

import io

import markdown
from xhtml2pdf import pisa

_ESTILO_PDF = """
<style>
    body { font-family: Helvetica, Arial, sans-serif; font-size: 11pt; color: #16281F; }
    h1 { color: #00693E; font-size: 18pt; margin-bottom: 4px; }
    h2 { color: #00693E; font-size: 13pt; margin-top: 16px; }
    p, li { line-height: 1.4; }
    .meta { color: #5A6B63; font-size: 9pt; margin-bottom: 16px; }
    .alerta { color: #92400E; background-color: #FEF3C7; padding: 8px; font-size: 9pt; margin-bottom: 16px; }
</style>
"""


def gerar_pdf_relatorio(texto_markdown: str, titulo: str, meta: str = "", alerta: str = "") -> bytes:
    """Converte o relatório (Markdown) em PDF (bytes). `meta` (referência/
    filtros usados) e `alerta` (valores suspeitos, ver ai_report.py) — quando
    presente, viaja junto no PDF, não só na tela, já que o documento pode ser
    salvo/impartilhado separado da sessão do painel."""
    corpo_html = markdown.markdown(texto_markdown, extensions=["extra"])
    meta_html = f'<p class="meta">{meta}</p>' if meta else ""
    alerta_html = f'<p class="alerta">⚠ {alerta}</p>' if alerta else ""
    html = f"""
    <html>
    <head><meta charset="utf-8">{_ESTILO_PDF}</head>
    <body>
        <h1>{titulo}</h1>
        {meta_html}
        {alerta_html}
        {corpo_html}
    </body>
    </html>
    """
    buffer = io.BytesIO()
    pisa.CreatePDF(html, dest=buffer)
    return buffer.getvalue()
