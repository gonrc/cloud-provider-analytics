"""Genera el PDF de un documento Markdown de docs/, con los diagramas Mermaid renderizados.

Uso: python scripts/build_pdf.py docs/diseno_v1.md

Convierte el Markdown a HTML, reemplaza los bloques ```mermaid por diagramas
(mermaid.js desde jsDelivr, así que necesita conexión) e imprime a PDF con
Chrome headless. Los links relativos al repo pasan a apuntar a GitHub.
"""
import html
import re
import subprocess
import sys
from pathlib import Path

import markdown

REPO_URL = "https://github.com/gonrc/cloud-provider-analytics/blob/main"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

CSS = """
@page { size: A4; margin: 16mm 14mm; }
body { font-family: -apple-system, "Helvetica Neue", Arial, sans-serif; font-size: 10pt; line-height: 1.45; color: #1a1a1a; }
h1 { font-size: 19pt; margin: 0 0 6pt; }
h2 { font-size: 14pt; margin: 18pt 0 6pt; padding-bottom: 3pt; border-bottom: 1px solid #ccc; break-after: avoid; }
h3 { font-size: 11.5pt; margin: 12pt 0 4pt; break-after: avoid; }
p, li { margin: 4pt 0; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0 10pt; font-size: 8.6pt; break-inside: auto; }
tr { break-inside: avoid; }
th, td { border: 1px solid #cfcfcf; padding: 3pt 5pt; vertical-align: top; text-align: left; }
th { background: #f1f3f5; }
code { font-family: Menlo, Consolas, monospace; font-size: 8.4pt; background: #f4f4f4; padding: 0 2pt; border-radius: 2pt; }
pre { background: #f6f8fa; border: 1px solid #e1e4e8; padding: 6pt 8pt; font-size: 8.2pt; line-height: 1.35; overflow: hidden; white-space: pre-wrap; break-inside: avoid; }
pre code { background: none; padding: 0; }
pre.mermaid { background: none; border: none; text-align: center; white-space: normal; }
img { max-width: 100%; }
a { color: #0b5cad; text-decoration: none; }
hr { border: none; border-top: 1px solid #ddd; margin: 12pt 0; }
"""


def github_slug(value: str, separator: str = "-") -> str:
    # Mismo criterio que GitHub para las anclas, así los links internos funcionan igual.
    value = re.sub(r"<[^>]+>", "", value).strip().lower()
    value = re.sub(r"[^\w\- ]", "", value)
    return value.replace(" ", separator)


def to_html(md_path: Path) -> str:
    text = md_path.read_text()
    text = re.sub(r"\]\(\.\./([^)#]+)\)", lambda m: f"]({REPO_URL}/{m.group(1)})", text)
    body = markdown.markdown(
        text,
        extensions=["tables", "fenced_code", "toc"],
        extension_configs={"toc": {"slugify": github_slug}},
    )
    body = re.sub(
        r'<pre><code class="language-mermaid">(.*?)</code></pre>',
        lambda m: f'<pre class="mermaid">{html.unescape(m.group(1))}</pre>',
        body,
        flags=re.S,
    )
    return f"""<!doctype html><html lang="es"><head><meta charset="utf-8">
<title>{md_path.stem}</title><style>{CSS}</style></head><body>{body}
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{startOnLoad: true, theme: "neutral", flowchart: {{htmlLabels: true}}}});</script>
</body></html>"""


def main() -> int:
    md_path = Path(sys.argv[1])
    pdf_path = md_path.with_suffix(".pdf")
    # El HTML se escribe junto al .md para que las imágenes con ruta relativa (img/...) resuelvan.
    html_path = md_path.resolve().parent / f".{md_path.stem}.tmp.html"
    try:
        html_path.write_text(to_html(md_path))
        subprocess.run(
            [CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
             "--virtual-time-budget=15000", f"--print-to-pdf={pdf_path.resolve()}", html_path.as_uri()],
            check=True, capture_output=True,
        )
    finally:
        html_path.unlink(missing_ok=True)
    print(f"PDF: {pdf_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
