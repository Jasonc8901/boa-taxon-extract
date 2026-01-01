#!/usr/bin/env python3
"""
Zero-third-party scraper for Butterflies of America US/Canada images page.

XLSX output (stdlib only) with columns:
  Species      -> "Genus species"
  Subspecies   -> last word of trinomial; blank if none
  Common Name  -> from <i id="e">...</i> on the species row; carried to its subspecies rows
"""

from __future__ import annotations

import re
import sys
import urllib.request
import zipfile
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import List, Optional, Tuple
from xml.sax.saxutils import escape as xml_escape

DEFAULT_URL = "https://www.butterfliesofamerica.com/US-Can-images.htm"

# Accepts "Genus species" or "Genus species subspecies"
TAXON_RE = re.compile(r"^([A-Z][a-z]+)\s+([a-z][a-z\-]*)(?:\s+([a-z][a-z\-]*))?$")


@dataclass(frozen=True)
class TaxonItem:
    kind: str   # "species" | "subspecies"
    taxon: str  # e.g. "Microtia elvira" or "Eurytides phaon phaon"


class BOAHTMLParser(HTMLParser):
    """
    Robust extraction:
      - While inside <p id="p9"> (species) or <p id="p9s"> (subspecies), capture ANY <a> text
        that matches TAXON_RE. This avoids relying on the <a title="..."> attribute.
      - Common name is <i id="e">...</i> inside the species paragraph <p id="p9">.
    """
    def __init__(self) -> None:
        super().__init__()
        self.items: List[TaxonItem] = []
        self.species_common: dict[str, str] = {}

        # Context flags
        self._in_p9: bool = False
        self._in_p9s: bool = False

        # Anchor capture (only within p9/p9s)
        self._capture_a: bool = False
        self._a_buf: List[str] = []

        # Common name capture (only within p9)
        self._capture_common: bool = False
        self._common_buf: List[str] = []

        # Track last species taxon seen within the current p9 to map the common name
        self._last_species_in_p9: Optional[str] = None

    def handle_starttag(self, tag: str, attrs) -> None:
        t = tag.lower()
        attrs_dict = {k.lower(): (v or "") for k, v in attrs}

        if t == "p":
            pid = attrs_dict.get("id", "")
            if pid == "p9":
                self._in_p9 = True
                self._in_p9s = False
                self._last_species_in_p9 = None
            elif pid == "p9s":
                self._in_p9s = True
                self._in_p9 = False

        # Capture any <a> text inside the relevant <p>
        if t == "a" and (self._in_p9 or self._in_p9s):
            self._capture_a = True
            self._a_buf = []

        # Capture common name: <i id="e">Common Name</i> inside p9
        if t == "i" and self._in_p9 and attrs_dict.get("id", "") == "e":
            self._capture_common = True
            self._common_buf = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        if t == "a" and self._capture_a:
            text = " ".join("".join(self._a_buf).split())
            self._capture_a = False
            self._a_buf = []

            m = TAXON_RE.match(text)
            if m:
                genus, sp, ssp = m.group(1), m.group(2), m.group(3)
                norm_species = f"{genus} {sp}"
                if self._in_p9s or (ssp is not None and self._in_p9):
                    # Subspecies row (usually p9s; also handle rare trinomial in p9)
                    self.items.append(TaxonItem("subspecies", text))
                else:
                    # Species row
                    self.items.append(TaxonItem("species", norm_species))
                    if self._in_p9:
                        self._last_species_in_p9 = norm_species

        if t == "i" and self._capture_common:
            common = " ".join("".join(self._common_buf).split())
            self._capture_common = False
            self._common_buf = []

            if common and self._last_species_in_p9:
                self.species_common[self._last_species_in_p9] = common

        if t == "p":
            # Leaving paragraph blocks
            if self._in_p9:
                self._in_p9 = False
                self._last_species_in_p9 = None
            if self._in_p9s:
                self._in_p9s = False

    def handle_data(self, data: str) -> None:
        if self._capture_a and data:
            self._a_buf.append(data)
        if self._capture_common and data:
            self._common_buf.append(data)


def fetch_html(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; boa-zero-deps-scraper/1.0)",
            "Accept": "text/html,application/xhtml+xml",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def normalize_species(taxon_text: str) -> str:
    parts = taxon_text.split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}"
    return taxon_text.strip()


def subspecies_epithet_only(trinomial_text: str) -> str:
    parts = trinomial_text.split()
    return parts[-1] if parts else ""


def build_rows(items: List[TaxonItem], species_common: dict[str, str]) -> List[Tuple[str, str, str]]:
    """
    No species-only header rows.
    If a species has no subspecies, output a single row with subspecies blank.
    """
    rows: List[Tuple[str, str, str]] = []

    current_species: str = ""
    current_common: str = ""
    current_species_had_subspecies: bool = False

    def flush_blank_if_needed() -> None:
        nonlocal current_species, current_species_had_subspecies, current_common, rows
        if current_species and not current_species_had_subspecies:
            rows.append((current_species, "", current_common))

    for it in items:
        if it.kind == "species":
            flush_blank_if_needed()
            current_species = normalize_species(it.taxon)
            current_common = species_common.get(current_species, "")
            current_species_had_subspecies = False

        elif it.kind == "subspecies":
            # subspecies item carries full trinomial text; infer species from first two words
            inferred_species = normalize_species(it.taxon)
            if not current_species:
                current_species = inferred_species
                current_common = species_common.get(current_species, "")

            current_species_had_subspecies = True
            rows.append((current_species, subspecies_epithet_only(it.taxon), current_common))

    flush_blank_if_needed()

    # De-duplicate exact repeats while preserving order
    seen = set()
    deduped: List[Tuple[str, str, str]] = []
    for r in rows:
        if r in seen:
            continue
        seen.add(r)
        deduped.append(r)

    return deduped


def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def cell_ref(col: int, row: int) -> str:
    return f"{col_letter(col)}{row}"


def xlsx_cell_inline_str(r: str, value: str, style_idx: int) -> str:
    v = xml_escape(value)
    return f'<c r="{r}" t="inlineStr" s="{style_idx}"><is><t>{v}</t></is></c>'


def write_xlsx(rows: List[Tuple[str, str, str]], out_path: str) -> None:
    # styles: 0 normal, 1 bold (header)
    sheet_rows_xml: List[str] = []

    # Header row
    header = [
        xlsx_cell_inline_str(cell_ref(1, 1), "Species", 1),
        xlsx_cell_inline_str(cell_ref(2, 1), "Subspecies", 1),
        xlsx_cell_inline_str(cell_ref(3, 1), "Common Name", 1),
    ]
    sheet_rows_xml.append(f'<row r="1">{"".join(header)}</row>')

    # Data rows
    for i, (species, subspp, common) in enumerate(rows, start=2):
        cells = [
            xlsx_cell_inline_str(cell_ref(1, i), species, 0),
            xlsx_cell_inline_str(cell_ref(2, i), subspp, 0),
            xlsx_cell_inline_str(cell_ref(3, i), common, 0),
        ]
        sheet_rows_xml.append(f'<row r="{i}">{"".join(cells)}</row>')

    sheet_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
           xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheetData>
    {''.join(sheet_rows_xml)}
  </sheetData>
</worksheet>
'''

    workbook_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Taxa" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
'''

    styles_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="2">
    <font>
      <sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/>
    </font>
    <font>
      <b/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/>
    </font>
  </fonts>
  <fills count="1">
    <fill><patternFill patternType="none"/></fill>
  </fills>
  <borders count="1">
    <border><left/><right/><top/><bottom/><diagonal/></border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="2">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyFont="1"/>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>
  </cellXfs>
</styleSheet>
'''

    content_types_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml"
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml"
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml"
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>
'''

    rels_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1"
                Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
                Target="xl/workbook.xml"/>
</Relationships>
'''

    workbook_rels_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1"
                Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
                Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2"
                Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles"
                Target="styles.xml"/>
</Relationships>
'''

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types_xml)
        z.writestr("_rels/.rels", rels_xml)
        z.writestr("xl/workbook.xml", workbook_xml)
        z.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        z.writestr("xl/styles.xml", styles_xml)
        z.writestr("xl/worksheets/sheet1.xml", sheet_xml)


def main() -> int:
    out_path = "us_can_taxa.xlsx"
    url = DEFAULT_URL

    if len(sys.argv) >= 2:
        out_path = sys.argv[1]
    if len(sys.argv) >= 3:
        url = sys.argv[2]

    html = fetch_html(url)

    parser = BOAHTMLParser()
    parser.feed(html)

    if not parser.items:
        print("No taxa found. The page markup/pattern may have changed.", file=sys.stderr)
        return 2

    rows = build_rows(parser.items, parser.species_common)
    if not rows:
        print("No rows produced after processing taxa.", file=sys.stderr)
        return 2

    write_xlsx(rows, out_path)
    print(f"Wrote {len(rows)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
