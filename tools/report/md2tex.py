#!/usr/bin/env python3
"""Convert docs/report/m2/*.md into the LaTeX layout of the M1 report and compile it.

  python3 tools/report/md2tex.py   -> docs/report/m2/IF4031_M2_OrangSuksesAamiin.pdf

Handles the Markdown subset used by the report: headings, paragraphs, bullet and numbered lists (nested), pipe
tables, fenced code, Mermaid blocks (rendered to PDF figures by headless Chrome), **bold**, *italic*, `code`, links.
The preamble is the M1 one (docs/report/latex/preamble.tex) with the M2 header. Needs xelatex and Chrome/Edge.
"""
import os
import pathlib
import re
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = ROOT / "docs" / "report" / "m2"
OUT_DIR = ROOT / "docs" / "report" / "latex-m2"
FIG = OUT_DIR / "fig"
PARTS = ["00-umum.md", "01-p1.md", "02-p2.md", "03-p3.md", "04-p4.md", "05-p5.md", "lampiran.md"]
PDF = SRC / "IF4031_M2_OrangSuksesAamiin.pdf"
SIGNED = SRC / "IF4031_Surat-Pernyataan-AI_M2_OrangSuksesAamiin.pdf"
# the team uses the signed declaration of M1 unless an M2 one is added
SIGNED_M1 = ROOT / "docs" / "report" / "latex" / "IF4031_Surat-Pernyataan-AI_OrangSuksesAamiin.pdf"

STATUS = {"✅": "lulus", "🟡": "sebagian", "❌": "belum", "➡️": "di video", "⬜": "belum"}
SPECIAL = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#", "_": r"\_",
           "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}
UNICODE = {"→": r"$\rightarrow$", "←": r"$\leftarrow$", "≤": r"$\le$", "≥": r"$\ge$", "×": r"$\times$",
           "≈": r"$\approx$", "–": "--", "—": "---", "…": r"\ldots{}", "±": r"$\pm$", "·": r"$\cdot$",
           "−": "--", "✓": "", "“": "``", "”": "''", "’": "'"}


def esc(s):
    out = "".join(SPECIAL.get(c, c) for c in s)
    for k, v in UNICODE.items():
        out = out.replace(k, v)
    return out


def code_inline(s):
    t = esc(s)
    # allow line breaks after separators inside long identifiers and paths
    t = re.sub(r"(/|\\_|\.|-|,|=|\|)", r"\1\\allowbreak{}", t)
    # and between the words of long CamelCase names (TestPendingChangesResumeAfterPartialApply)
    t = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", r"\\allowbreak{}", t)
    return r"\texttt{" + t + "}"


def inline(s):
    for k, v in STATUS.items():
        s = s.replace(k, v)
    s = re.sub(r"\*\*(PASS|Lulus)\*\*", "lulus", s)
    s = re.sub(r"\*\*Sebagian\*\*", "sebagian", s)
    parts = re.split(r"(`[^`]+`)", s)
    out = []
    for p in parts:
        if p.startswith("`") and p.endswith("`") and len(p) > 1:
            out.append(code_inline(p[1:-1]))
            continue
        p = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", p)  # links: keep the text
        t = esc(p)
        t = re.sub(r'"([^"]+)"', r"``\1''", t)  # typographic quotes like the M1 report
        t = re.sub(r"\*\*(.+?)\*\*", r"\\textbf{\1}", t)
        t = re.sub(r"(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])", r"\\emph{\1}", t)
        out.append(t)
    return "".join(out)


def heading_text(s):
    s = re.sub(r"^\d+(\.\d+)*\.?\s+", "", s.strip())  # LaTeX numbers sections itself
    return inline(s)


def render_mermaid(src, idx, browser):
    FIG.mkdir(parents=True, exist_ok=True)
    html = FIG / f"m{idx}.html"
    pdf = FIG / f"m{idx}.pdf"
    html.write_text(f"""<!doctype html><html><head><meta charset="utf-8">
<style>body{{margin:0;font-family:Arial,sans-serif}} #d{{display:inline-block;padding:6px}}</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script></head><body>
<div id="d" class="mermaid">{src}</div>
<script>
mermaid.initialize({{startOnLoad:false, theme:"neutral", flowchart:{{useMaxWidth:false, htmlLabels:true}}}});
mermaid.run({{nodes:[document.getElementById("d")]}}).then(() => {{
  const b = document.getElementById("d").getBoundingClientRect();
  const st = document.createElement("style");
  st.textContent = "@page{{size:" + Math.ceil(b.width + 4) + "px " + Math.ceil(b.height + 4) + "px;margin:0}}";
  document.head.appendChild(st);
}});
</script></body></html>""", encoding="utf-8")
    subprocess.run([browser, "--headless=new", "--disable-gpu", "--no-pdf-header-footer", "--virtual-time-budget=15000",
                    f"--print-to-pdf={pdf}", html.as_uri()], check=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    return pdf.name


def table(rows):
    cells = [[c.strip() for c in r.strip().strip("|").split("|")] for r in rows]
    head, body = cells[0], [r for r in cells[2:]]
    n = len(head)
    body = [r + [""] * (n - len(r)) for r in body]
    lens = []
    for i in range(n):
        m = max([len(head[i])] + [len(r[i]) for r in body])
        lens.append(min(max(m, 6), 90))
    total = sum(lens)
    widths = [max(0.14 if n > 2 else 0.2, l / total) for l in lens]
    s = sum(widths)
    widths = [w / s for w in widths]
    spec = "".join(r">{\raggedright\arraybackslash}p{%.3f\linewidth}" % (w * 0.97 - 0.012) for w in widths)
    out = [r"{\small", r"\begin{longtable}{@{}" + spec + "@{}}", r"\toprule"]
    out.append(" & ".join(r"\textbf{" + inline(h) + "}" for h in head) + r" \\")
    out += [r"\midrule", r"\endhead"]
    for r in body:
        out.append(" & ".join(inline(c) for c in r[:n]) + r" \\")
    out += [r"\bottomrule", r"\end{longtable}", "}"]
    return "\n".join(out)


def convert(md, browser, counter):
    lines = md.split("\n")
    out = []
    i = 0
    list_stack = []  # (indent, env)

    def close_lists(to_indent=-1):
        while list_stack and list_stack[-1][0] > to_indent:
            out.append(r"\end{%s}" % list_stack.pop()[1])

    while i < len(lines):
        line = lines[i]
        if line.strip().startswith("<!--") or line.strip().startswith("<div") or line.strip().startswith("</div"):
            i += 1
            continue
        if line.startswith("```"):
            close_lists()
            lang = line[3:].strip()
            j = i + 1
            block = []
            while j < len(lines) and not lines[j].startswith("```"):
                block.append(lines[j])
                j += 1
            if lang == "mermaid":
                counter[0] += 1
                name = render_mermaid("\n".join(block), counter[0], browser)
                from pypdf import PdfReader
                box = PdfReader(str(FIG / name)).pages[0].mediabox
                if float(box.width) / float(box.height) > 99:  # rotation disabled: diagrams are drawn compact
                    # very wide diagram: rotate it onto its own page so the labels stay readable
                    out.append(r"\begin{figure}[p]\centering\includegraphics[angle=90,width=0.9\textheight,height=\linewidth,keepaspectratio]{fig/%s}\end{figure}" % name)
                    i = j + 1
                    continue
                out.append(r"\begin{figure}[H]\centering\includegraphics[width=\linewidth,height=0.82\textheight,keepaspectratio]{fig/%s}\end{figure}" % name)
            else:
                out.append(r"\begin{lstlisting}")
                out += block
                out.append(r"\end{lstlisting}")
            i = j + 1
            continue
        m = re.match(r"^(#{1,3})\s+(.*)$", line)
        if m:
            close_lists()
            level = len(m.group(1))
            cmd = {1: "chapter", 2: "section", 3: "subsection"}[level]
            out.append("\\%s{%s}" % (cmd, heading_text(m.group(2))))
            i += 1
            continue
        if line.startswith("|"):
            close_lists()
            j = i
            rows = []
            while j < len(lines) and lines[j].startswith("|"):
                rows.append(lines[j])
                j += 1
            if len(rows) >= 2 and re.match(r"^\|[\s:|-]+\|?$", rows[1].strip()):
                out.append(table(rows))
            i = j
            continue
        m = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", line)
        if m:
            indent = len(m.group(1))
            env = "enumerate" if m.group(2)[0].isdigit() else "itemize"
            if not list_stack or indent > list_stack[-1][0]:
                list_stack.append((indent, env))
                out.append(r"\begin{%s}" % env)
            else:
                close_lists(indent)
                if not list_stack:
                    list_stack.append((indent, env))
                    out.append(r"\begin{%s}" % env)
            text = m.group(3)
            j = i + 1
            while j < len(lines) and lines[j].strip() and not re.match(r"^(\s*)([-*]|\d+\.)\s+", lines[j]) \
                    and lines[j].startswith(" ") and not lines[j].strip().startswith("|"):
                text += " " + lines[j].strip()
                j += 1
            out.append(r"\item " + inline(text))
            i = j
            continue
        if not line.strip():
            if list_stack:
                # a blank line ends a list unless the next line continues it
                nxt = lines[i + 1] if i + 1 < len(lines) else ""
                if not re.match(r"^(\s*)([-*]|\d+\.)\s+", nxt) and not nxt.startswith(" "):
                    close_lists()
            out.append("")
            i += 1
            continue
        if line.startswith(" ") and list_stack:
            out.append(inline(line.strip()))
            i += 1
            continue
        close_lists()
        para = [line.strip()]
        j = i + 1
        while j < len(lines) and lines[j].strip() and not lines[j].startswith(("#", "|", "```", "- ", "* ")) \
                and not re.match(r"^\d+\.\s", lines[j]) and not lines[j].strip().startswith("<!--"):
            para.append(lines[j].strip())
            j += 1
        out.append(inline(" ".join(para)))
        out.append("")
        i = j
    close_lists()
    return "\n".join(out)


MAIN = r"""%% Laporan IF4031 Milestone 2 -- dibangkitkan dari docs/report/m2/*.md oleh tools/report/md2tex.py
\documentclass[11pt,a4paper,oneside]{report}
\input{preamble}
\usepackage{graphicx}
\usepackage{float}
\usepackage{pdfpages}
\begin{document}
\begin{titlepage}
\thispagestyle{empty}
\sffamily
\vspace*{1.2cm}
{\color{accent}\rule{\textwidth}{1.6pt}}\\[1.2em]
{\Large IF4031 Arsitektur Aplikasi Terdistribusi}\\[0.4em]
{\large Tugas Besar -- Milestone 2: Sistem Terdistribusi Skalabel}\\[2.2em]
{\fontsize{40}{44}\selectfont\bfseries\color{accent} ARGUS}\\[0.5em]
{\Large\bfseries Aggregated Real-time Geohazard Unified System}\\[0.8em]
{\large Gateway kebencanaan BNPB di atas Kubernetes}\\[1.2em]
{\color{accent}\rule{\textwidth}{0.6pt}}

\vspace{2.2cm}
\begin{tabular}{@{}L{3.6cm}L{10cm}@{}}
\textbf{Nama kelompok} & OrangSuksesAamiin\\[0.8em]
\textbf{Anggota} & 13523128 - Andi Farhan Hidayat\\
 & 13523164 - Muhammad Rizain Firdaus\\
 & 13523152 - Muhammad Kinan Arkansyaddad\\[0.8em]
\textbf{Mata kuliah} & IF4031 Arsitektur Aplikasi Terdistribusi\\
\textbf{Institusi} & Institut Teknologi Bandung\\
\textbf{Semester} & 7, 2026/2027
\end{tabular}

\vfill
{\small\color{gridgray}Berkas ini dikumpulkan sebagai \texttt{IF4031\_M2\_OrangSuksesAamiin.pdf}.}
\end{titlepage}
\pagenumbering{roman}
\tableofcontents
\clearpage
\pagenumbering{arabic}
%(body)s
%(signed)s
\end{document}
"""


def main():
    browser = next((b for b in [os.environ.get("CHROME", ""), r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                                r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                                shutil.which("google-chrome") or "", shutil.which("chromium") or ""]
                    if b and os.path.exists(b)), None)
    if not browser:
        sys.exit("Chrome/Edge is needed to render the Mermaid figures")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pre = (ROOT / "docs" / "report" / "latex" / "preamble.tex").read_text(encoding="utf-8")
    pre = pre.replace("Laporan Milestone 1", "Laporan Milestone 2")
    (OUT_DIR / "preamble.tex").write_text(pre, encoding="utf-8")
    counter = [0]
    body = []
    for name in PARTS:
        tex = convert((SRC / name).read_text(encoding="utf-8"), browser, counter)
        (OUT_DIR / name.replace(".md", ".tex")).write_text(tex, encoding="utf-8")
        body.append(r"\input{%s}" % name.replace(".md", ""))
    signed = ""
    src_signed = SIGNED if SIGNED.exists() else SIGNED_M1
    if src_signed.exists():
        shutil.copy(src_signed, OUT_DIR / "signed.pdf")
        signed = r"\chapter{Lampiran Surat Pernyataan Penggunaan AI}" + "\n" + \
                 r"\includepdf[pages=-,pagecommand={\thispagestyle{empty}}]{signed.pdf}"
    (OUT_DIR / "main.tex").write_text(MAIN % {"body": "\n".join(body), "signed": signed}, encoding="utf-8")
    for _ in range(2):
        r = subprocess.run(["xelatex", "-interaction=nonstopmode", "-halt-on-error", "main.tex"], cwd=OUT_DIR,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace")
        if r.returncode != 0:
            print(r.stdout[-3000:])
            sys.exit("xelatex failed")
    shutil.copy(OUT_DIR / "main.pdf", PDF)
    print("wrote", PDF, "figures:", counter[0], "signed:", bool(signed))


if __name__ == "__main__":
    main()
