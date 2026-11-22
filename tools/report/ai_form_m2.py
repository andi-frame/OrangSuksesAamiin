#!/usr/bin/env python3
"""Make the M2 AI-use declaration from the signed M1 one: only the task title (Milestone 1 -> 2) and the date change.

The M1 PDF is a Google Docs export, so its text cannot be edited in place. A white box is drawn over the two old
strings and the new text is written at the same baseline in Times-Roman (base font, same size as the original).
  python3 tools/report/ai_form_m2.py -> docs/report/m2/IF4031_Surat-Pernyataan-AI_M2_OrangSuksesAamiin.pdf
"""
import pathlib

from pypdf import PdfReader, PdfWriter
from pypdf.generic import (ArrayObject, DecodedStreamObject, DictionaryObject, FloatObject, NameObject,
                           RectangleObject)

ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = ROOT / "docs" / "report" / "latex" / "IF4031_Surat-Pernyataan-AI_OrangSuksesAamiin.pdf"
OUT = ROOT / "docs" / "report" / "m2" / "IF4031_Surat-Pernyataan-AI_M2_OrangSuksesAamiin.pdf"
S = 0.75  # text matrix scale of the Google Docs export

# (page, cm_e, cm_f, tm_x_start, tm_x_end, tm_y, font_size_in_text_space, new_text)
EDITS = [
    (0, 113.040001, 582.4028, 277.52533, 288.0, 16.18229107, 17.333334, "2"),               # "Milestone 1"
    (1, 404.03998, 679.995056, 78.067482, 172.0, 12.447917, 13.333333, "22 November 2026"),  # "10 Oktober 2026"
]


def overlay(writer, edits):
    ops = []
    for _, e, f, x0, x1, ty, fs, text in edits:
        x, xe = e + S * x0, e + S * x1
        base = f - S * ty
        size = fs * S
        ops.append(f"1 1 1 rg {x - 0.5:.2f} {base - size * 0.28:.2f} {xe - x + 1:.2f} {size * 1.15:.2f} re f")
        ops.append(f"0 0 0 rg BT /FT {size:.3f} Tf {x:.2f} {base:.2f} Td ({text}) Tj ET")
    stream = DecodedStreamObject()
    stream.set_data("\n".join(ops).encode("latin-1"))
    return writer._add_object(stream)


def main():
    reader = PdfReader(str(SRC))
    writer = PdfWriter()
    writer.append(reader)
    font = DictionaryObject({NameObject("/Type"): NameObject("/Font"), NameObject("/Subtype"): NameObject("/Type1"),
                             NameObject("/BaseFont"): NameObject("/Times-Roman"),
                             NameObject("/Encoding"): NameObject("/WinAnsiEncoding")})
    font_ref = writer._add_object(font)
    for i, page in enumerate(writer.pages):
        edits = [e for e in EDITS if e[0] == i]
        if not edits:
            continue
        ref = overlay(writer, edits)
        contents = page.get("/Contents")
        arr = ArrayObject(list(contents.get_object()) if isinstance(contents.get_object(), ArrayObject) else [contents])
        # the original stream leaves its (flipped) transform active: isolate it with q ... Q
        save, restore = DecodedStreamObject(), DecodedStreamObject()
        save.set_data(b"q ")
        restore.set_data(b" Q ")
        arr.insert(0, writer._add_object(save))
        arr.append(writer._add_object(restore))
        arr.append(ref)
        page[NameObject("/Contents")] = arr
        res = page["/Resources"].get_object()
        fonts = res.get("/Font")
        if fonts is None:
            res[NameObject("/Font")] = DictionaryObject()
            fonts = res["/Font"]
        fonts.get_object()[NameObject("/FT")] = font_ref
    writer.add_metadata({"/Title": "IF4031_M2_OrangSuksesAamiin"})
    with open(OUT, "wb") as fh:
        writer.write(fh)
    print("wrote", OUT)


if __name__ == "__main__":
    main()
