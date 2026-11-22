#!/usr/bin/env python3
"""Build the M2 report PDF from docs/report/m2/*.md.

  python3 tools/report/build.py            -> docs/report/m2/IF4031_M2_OrangSuksesAamiin.pdf

Markdown -> HTML (python-markdown: tables, fenced code, toc) -> PDF with headless Chrome/Edge. Mermaid blocks are
rendered in the page by mermaid.js before printing. Needs: pip install markdown; Chrome or Edge.
"""
import os
import pathlib
import re
import shutil
import subprocess
import sys

import markdown

ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = ROOT / "docs" / "report" / "m2"
ORDER = ["cover.md", "00-umum.md", "01-p1.md", "02-p2.md", "03-p3.md", "04-p4.md", "05-p5.md", "lampiran.md"]
OUT = SRC / "IF4031_M2_OrangSuksesAamiin.pdf"

CSS = """
@page { size: A4; margin: 18mm 16mm 18mm 16mm; }
body { font-family: "Segoe UI", "Noto Sans", Arial, sans-serif; font-size: 10pt; line-height: 1.42; color: #111; }
h1 { font-size: 18pt; color: #0b4f6c; border-bottom: 2px solid #0b4f6c; padding-bottom: 3px; page-break-before: always; }
h1.first, .cover h1 { page-break-before: avoid; }
h2 { font-size: 13pt; color: #0b4f6c; margin-top: 16px; }
h3 { font-size: 11pt; margin-top: 12px; }
table { border-collapse: collapse; width: 100%; margin: 6px 0 10px; font-size: 8.6pt; page-break-inside: auto; }
th, td { border: 1px solid #b8c4cc; padding: 3px 5px; vertical-align: top; }
th { background: #e8f0f4; }
tr { page-break-inside: avoid; }
code { font-family: Consolas, "Cascadia Mono", monospace; font-size: 8.6pt; background: #f3f5f7; padding: 0 2px; }
pre { background: #f3f5f7; border: 1px solid #dde3e8; padding: 6px 8px; font-size: 8pt; white-space: pre-wrap;
      word-break: break-all; page-break-inside: avoid; }
pre code { background: none; padding: 0; }
blockquote { border-left: 3px solid #0b4f6c; margin: 6px 0; padding: 2px 10px; color: #333; background: #f7fafb; }
img { max-width: 100%; }
.mermaid { text-align: center; margin: 8px 0; page-break-inside: avoid; }
.cover { text-align: left; padding-top: 40mm; page-break-after: always; }
.cover .title { font-size: 34pt; font-weight: 700; color: #0b4f6c; margin: 10px 0 0; }
.cover .sub { font-size: 14pt; font-weight: 600; }
.cover table { width: auto; font-size: 11pt; margin-top: 30mm; }
.cover td { border: none; padding: 3px 14px 3px 0; }
.toc { page-break-after: always; }
.toc ul { list-style: none; padding-left: 14px; }
"""


def mermaid_blocks(text):
    # ```mermaid ... ``` -> <div class="mermaid">...</div> (kept raw for mermaid.js)
    return re.sub(r"```mermaid\n(.*?)```", lambda m: '<div class="mermaid">\n' + m.group(1) + "</div>",
                  text, flags=re.S)


def main():
    parts = []
    for name in ORDER:
        p = SRC / name
        if not p.exists():
            sys.exit(f"missing {p}")
        parts.append(mermaid_blocks(p.read_text(encoding="utf-8")))
    md = markdown.Markdown(extensions=["tables", "fenced_code", "toc", "sane_lists", "md_in_html"],
                           extension_configs={"toc": {"toc_depth": "1-2"}})
    body = md.convert("\n\n".join(parts))
    body = body.replace("<h1", '<h1 class="first"', 1) if body.lstrip().startswith("<h1") else body
    toc = f'<div class="toc"><h1 class="first">Daftar isi</h1>{md.toc}</div>'
    body = body.replace("<!--TOC-->", toc)
    html = f"""<!doctype html><html lang="id"><head><meta charset="utf-8"><title>IF4031 M2 OrangSuksesAamiin</title>
<style>{CSS}</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{startOnLoad: true, theme: "neutral", flowchart: {{useMaxWidth: true}}}});</script>
</head><body>{body}</body></html>"""
    out_html = SRC / "report.html"
    out_html.write_text(html, encoding="utf-8")
    browser = next((b for b in [os.environ.get("CHROME", ""), shutil.which("chrome") or "", shutil.which("msedge") or "",
                                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                                r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                                "/usr/bin/google-chrome", "/usr/bin/chromium"] if b and os.path.exists(b)), None)
    if not browser:
        sys.exit("no Chrome/Edge found; open docs/report/m2/report.html and print to PDF")
    subprocess.run([browser, "--headless=new", "--disable-gpu", "--no-pdf-header-footer", "--virtual-time-budget=20000",
                    f"--print-to-pdf={OUT}", out_html.as_uri()], check=True)
    print("wrote", OUT)


if __name__ == "__main__":
    main()
