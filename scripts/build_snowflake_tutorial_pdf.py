#!/usr/bin/env python3
"""Build the ABC Retail Snowflake tutorial PDF from its Markdown source."""

from __future__ import annotations

import argparse
import html
import re
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    HRFlowable,
    KeepTogether,
    ListFlowable,
    ListItem,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    XPreformatted,
)
from reportlab.platypus.tableofcontents import TableOfContents


NAVY = colors.HexColor("#102A43")
BLUE = colors.HexColor("#1677FF")
CYAN = colors.HexColor("#13C2C2")
ICE = colors.HexColor("#EAF4FF")
LIGHT = colors.HexColor("#F5F8FC")
MID = colors.HexColor("#D7E2EE")
TEXT = colors.HexColor("#243B53")
MUTED = colors.HexColor("#627D98")
GREEN = colors.HexColor("#0F9D7A")
CODE_BG = colors.HexColor("#101820")
CODE_TEXT = colors.HexColor("#E8F1F8")
WHITE = colors.white


def register_fonts() -> None:
    base = Path("/System/Library/Fonts/Supplemental")
    pdfmetrics.registerFont(TTFont("Arial", str(base / "Arial.ttf")))
    pdfmetrics.registerFont(TTFont("Arial-Bold", str(base / "Arial Bold.ttf")))
    pdfmetrics.registerFont(TTFont("Arial-Italic", str(base / "Arial Italic.ttf")))
    pdfmetrics.registerFont(
        TTFont("Arial-BoldItalic", str(base / "Arial Bold Italic.ttf"))
    )
    pdfmetrics.registerFont(TTFont("CourierNew", str(base / "Courier New.ttf")))
    pdfmetrics.registerFont(
        TTFont("CourierNew-Bold", str(base / "Courier New Bold.ttf"))
    )
    pdfmetrics.registerFontFamily(
        "Arial",
        normal="Arial",
        bold="Arial-Bold",
        italic="Arial-Italic",
        boldItalic="Arial-BoldItalic",
    )


class TutorialDocTemplate(BaseDocTemplate):
    def __init__(self, filename: str, **kwargs):
        super().__init__(filename, **kwargs)
        self._heading_seq = 0

    def beforeDocument(self):
        self._heading_seq = 0

    def afterFlowable(self, flowable):
        if isinstance(flowable, Paragraph):
            level = getattr(flowable, "_toc_level", None)
            if level is not None:
                text = flowable.getPlainText()
                self._heading_seq += 1
                key = f"heading-{self._heading_seq}"
                self.canv.bookmarkPage(key)
                self.canv.addOutlineEntry(text, key, level=level, closed=False)
                self.notify("TOCEntry", (level, text, self.page, key))


def make_styles():
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="BodyVN",
            fontName="Arial",
            fontSize=9.4,
            leading=14.2,
            textColor=TEXT,
            spaceAfter=5.5,
            allowWidows=0,
            allowOrphans=0,
        )
    )
    styles.add(
        ParagraphStyle(
            name="H1VN",
            fontName="Arial-Bold",
            fontSize=18,
            leading=22,
            textColor=NAVY,
            spaceBefore=12,
            spaceAfter=9,
            keepWithNext=True,
        )
    )
    styles.add(
        ParagraphStyle(
            name="H2VN",
            fontName="Arial-Bold",
            fontSize=13.5,
            leading=17,
            textColor=BLUE,
            spaceBefore=10,
            spaceAfter=6,
            keepWithNext=True,
        )
    )
    styles.add(
        ParagraphStyle(
            name="H3VN",
            fontName="Arial-Bold",
            fontSize=10.8,
            leading=14,
            textColor=NAVY,
            spaceBefore=7,
            spaceAfter=4,
            keepWithNext=True,
        )
    )
    styles.add(
        ParagraphStyle(
            name="BulletVN",
            parent=styles["BodyVN"],
            leftIndent=13,
            firstLineIndent=-7,
            bulletIndent=4,
            spaceAfter=2.5,
        )
    )
    styles.add(
        ParagraphStyle(
            name="QuoteVN",
            parent=styles["BodyVN"],
            leftIndent=11,
            rightIndent=7,
            borderColor=CYAN,
            borderWidth=2,
            borderPadding=(6, 8, 6, 10),
            backColor=colors.HexColor("#ECFBFB"),
            textColor=colors.HexColor("#275D5D"),
            spaceBefore=5,
            spaceAfter=8,
        )
    )
    styles.add(
        ParagraphStyle(
            name="CodeVN",
            fontName="CourierNew",
            fontSize=6.3,
            leading=8.1,
            textColor=CODE_TEXT,
            backColor=CODE_BG,
            borderColor=colors.HexColor("#243746"),
            borderWidth=0.5,
            borderPadding=7,
            leftIndent=0,
            rightIndent=0,
            spaceBefore=4,
            spaceAfter=8,
            splitLongWords=True,
        )
    )
    styles.add(
        ParagraphStyle(
            name="CaptionVN",
            fontName="Arial-Italic",
            fontSize=7.5,
            leading=10,
            textColor=MUTED,
            alignment=TA_CENTER,
            spaceAfter=6,
        )
    )
    styles.add(
        ParagraphStyle(
            name="TOCTitle",
            fontName="Arial-Bold",
            fontSize=24,
            leading=29,
            textColor=NAVY,
            spaceAfter=12,
        )
    )
    styles.add(
        ParagraphStyle(
            name="TOC0",
            fontName="Arial-Bold",
            fontSize=10,
            leading=15,
            textColor=NAVY,
            leftIndent=0,
            firstLineIndent=0,
            spaceBefore=3,
        )
    )
    styles.add(
        ParagraphStyle(
            name="TOC1",
            fontName="Arial",
            fontSize=8.7,
            leading=13,
            textColor=TEXT,
            leftIndent=12,
            firstLineIndent=0,
        )
    )
    return styles


def inline_markup(text: str) -> str:
    text = html.escape(text, quote=False)
    text = re.sub(r"`([^`]+)`", r'<font name="CourierNew" color="#0B5CAB">\1</font>', text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"\*([^*]+)\*", r"<i>\1</i>", text)
    text = re.sub(
        r"(https?://[^\s<]+)",
        r'<link href="\1" color="#1677FF">\1</link>',
        text,
    )
    return text


def normalize_code(code: str, width: int = 112) -> str:
    """Wrap only very long code lines to prevent clipping."""
    out = []
    for line in code.expandtabs(2).splitlines():
        if len(line) <= width:
            out.append(line)
            continue
        indent = re.match(r"\s*", line).group(0)
        remaining = line
        first = True
        while len(remaining) > width:
            cut = remaining.rfind(" ", 0, width)
            if cut < max(20, len(indent) + 5):
                cut = width
            out.append(remaining[:cut])
            remaining = indent + "  " + remaining[cut:].lstrip()
            first = False
        if remaining or first:
            out.append(remaining)
    return "\n".join(out)


def paragraph(text: str, style):
    return Paragraph(inline_markup(text.strip()), style)


def parse_table(lines: list[str], styles, available_width: float):
    rows = []
    for line in lines:
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if all(re.fullmatch(r":?-{3,}:?", c or "") for c in cells):
            continue
        rows.append(
            [
                Paragraph(
                    inline_markup(c),
                    ParagraphStyle(
                        "TableCell",
                        parent=styles["BodyVN"],
                        fontSize=7.5,
                        leading=10,
                        spaceAfter=0,
                    ),
                )
                for c in cells
            ]
        )
    if not rows:
        return Spacer(1, 1)
    columns = max(len(r) for r in rows)
    widths = [available_width / columns] * columns
    table = Table(rows, colWidths=widths, repeatRows=1, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
                ("FONTNAME", (0, 0), (-1, 0), "Arial-Bold"),
                ("BACKGROUND", (0, 1), (-1, -1), WHITE),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT]),
                ("GRID", (0, 0), (-1, -1), 0.4, MID),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return table


def parse_markdown(text: str, styles, available_width: float):
    flowables = []
    lines = text.splitlines()
    i = 0
    in_code = False
    code_lines: list[str] = []
    paragraph_lines: list[str] = []

    def flush_paragraph():
        nonlocal paragraph_lines
        if paragraph_lines:
            joined = " ".join(x.strip() for x in paragraph_lines).strip()
            if joined:
                flowables.append(paragraph(joined, styles["BodyVN"]))
            paragraph_lines = []

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if stripped.startswith("```"):
            flush_paragraph()
            if in_code:
                code = normalize_code("\n".join(code_lines))
                flowables.append(XPreformatted(html.escape(code), styles["CodeVN"]))
                code_lines = []
                in_code = False
            else:
                in_code = True
            i += 1
            continue

        if in_code:
            code_lines.append(line)
            i += 1
            continue

        if stripped == r"\pagebreak":
            flush_paragraph()
            flowables.append(PageBreak())
            i += 1
            continue

        if stripped.startswith("|") and i + 1 < len(lines):
            flush_paragraph()
            table_lines = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                table_lines.append(lines[i])
                i += 1
            flowables.append(parse_table(table_lines, styles, available_width))
            flowables.append(Spacer(1, 6))
            continue

        heading = re.match(r"^(#{1,3})\s+(.+)$", stripped)
        if heading:
            flush_paragraph()
            level = len(heading.group(1))
            style = styles[{1: "H1VN", 2: "H2VN", 3: "H3VN"}[level]]
            p = paragraph(heading.group(2), style)
            p._toc_level = level - 1
            flowables.append(p)
            if level == 1:
                flowables.append(HRFlowable(width="100%", thickness=1, color=MID))
                flowables.append(Spacer(1, 4))
            i += 1
            continue

        if stripped.startswith(">"):
            flush_paragraph()
            quote_lines = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                quote_lines.append(lines[i].strip()[1:].strip())
                i += 1
            flowables.append(paragraph(" ".join(quote_lines), styles["QuoteVN"]))
            continue

        bullet = re.match(r"^(\s*)[-*]\s+(.+)$", line)
        number = re.match(r"^(\s*)\d+\.\s+(.+)$", line)
        if bullet or number:
            flush_paragraph()
            match = bullet or number
            indent_level = min(len(match.group(1)) // 2, 3)
            text_value = match.group(2)
            number_match = re.match(r"^\s*(\d+)", line)
            bullet_text = "•" if bullet else f"{number_match.group(1)}."
            style = ParagraphStyle(
                f"List{indent_level}",
                parent=styles["BulletVN"],
                leftIndent=13 + indent_level * 10,
                bulletIndent=4 + indent_level * 10,
            )
            flowables.append(
                Paragraph(inline_markup(text_value), style, bulletText=bullet_text)
            )
            i += 1
            continue

        if not stripped:
            flush_paragraph()
            i += 1
            continue

        paragraph_lines.append(line)
        i += 1

    flush_paragraph()
    if code_lines:
        flowables.append(
            XPreformatted(html.escape(normalize_code("\n".join(code_lines))), styles["CodeVN"])
        )
    return flowables


def draw_cover(canvas, doc):
    width, height = A4
    canvas.saveState()
    canvas.setFillColor(NAVY)
    canvas.rect(0, 0, width, height, fill=1, stroke=0)
    canvas.setFillColor(BLUE)
    canvas.circle(width - 28 * mm, height - 32 * mm, 42 * mm, fill=1, stroke=0)
    canvas.setFillColor(CYAN)
    canvas.circle(width - 4 * mm, height - 9 * mm, 19 * mm, fill=1, stroke=0)
    canvas.setFillColor(colors.HexColor("#0B1F33"))
    canvas.rect(0, 0, width, 40 * mm, fill=1, stroke=0)

    canvas.setFillColor(WHITE)
    canvas.setFont("Arial-Bold", 13)
    canvas.drawString(22 * mm, height - 31 * mm, "ABC RETAIL DATA PLATFORM")
    canvas.setFont("Arial-Bold", 31)
    canvas.drawString(22 * mm, height - 64 * mm, "SNOWFLAKE NATIVE")
    canvas.drawString(22 * mm, height - 78 * mm, "END-TO-END")
    canvas.setFont("Arial", 15)
    canvas.setFillColor(colors.HexColor("#D8E8F6"))
    canvas.drawString(22 * mm, height - 94 * mm, "Tutorial triển khai chuyên sâu")

    canvas.setFillColor(ICE)
    canvas.roundRect(22 * mm, height - 146 * mm, 164 * mm, 34 * mm, 4 * mm, fill=1, stroke=0)
    canvas.setFillColor(NAVY)
    canvas.setFont("Arial-Bold", 10)
    canvas.drawString(29 * mm, height - 124 * mm, "TỪ FILE ĐẾN DATA MART")
    canvas.setFont("Arial", 8.7)
    canvas.drawString(29 * mm, height - 132 * mm, "Medallion  |  CDC  |  SCD2  |  Dimensional Model")
    canvas.drawString(29 * mm, height - 139 * mm, "Workspaces  |  dbt Projects  |  Cortex AI")

    canvas.setFillColor(WHITE)
    canvas.setFont("Arial-Bold", 10)
    canvas.drawString(22 * mm, 26 * mm, "HANDBOOK THỰC HÀNH")
    canvas.setFont("Arial", 8.5)
    canvas.setFillColor(colors.HexColor("#AFC7DB"))
    canvas.drawString(22 * mm, 18 * mm, "Phiên bản 4.0  •  Cập nhật 11/08/2026")
    canvas.restoreState()


def draw_body(canvas, doc):
    width, height = A4
    canvas.saveState()
    canvas.setStrokeColor(MID)
    canvas.setLineWidth(0.5)
    canvas.line(18 * mm, height - 17 * mm, width - 18 * mm, height - 17 * mm)
    canvas.setFont("Arial-Bold", 7.2)
    canvas.setFillColor(NAVY)
    canvas.drawString(18 * mm, height - 12.5 * mm, "ABC RETAIL • SNOWFLAKE NATIVE")
    canvas.setFont("Arial", 7)
    canvas.setFillColor(MUTED)
    canvas.drawRightString(width - 18 * mm, height - 12.5 * mm, "Tutorial triển khai")
    canvas.line(18 * mm, 15 * mm, width - 18 * mm, 15 * mm)
    canvas.setFont("Arial", 7)
    canvas.drawString(18 * mm, 9.5 * mm, "Snowflake Workspace learning project")
    canvas.drawRightString(width - 18 * mm, 9.5 * mm, f"Trang {doc.page}")
    canvas.restoreState()


def build_pdf(source: Path, output: Path) -> None:
    register_fonts()
    styles = make_styles()
    output.parent.mkdir(parents=True, exist_ok=True)

    page_width, page_height = A4
    body_frame = Frame(
        18 * mm,
        19 * mm,
        page_width - 36 * mm,
        page_height - 39 * mm,
        id="body",
        leftPadding=0,
        rightPadding=0,
        topPadding=2 * mm,
        bottomPadding=2 * mm,
    )
    cover_frame = Frame(0, 0, page_width, page_height, id="cover")

    doc = TutorialDocTemplate(
        str(output),
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=20 * mm,
        bottomMargin=19 * mm,
        title="Snowflake-native dbt Projects Tutorial - ABC Retail",
        author="Codex",
        subject="Workspaces, dbt Projects, Medallion, CDC, SCD and Snowflake",
        creator="Codex PDF workflow",
    )
    doc.addPageTemplates(
        [
            PageTemplate(id="Cover", frames=[cover_frame], onPage=draw_cover),
            PageTemplate(id="Body", frames=[body_frame], onPage=draw_body),
        ]
    )

    story = [Spacer(1, 1)]
    story += [NextPageTemplate("Body"), PageBreak()]

    story.append(Paragraph("MỤC LỤC", styles["TOCTitle"]))
    story.append(
        Paragraph(
            "Cấu trúc tutorial được thiết kế theo đúng thứ tự triển khai.",
            styles["BodyVN"],
        )
    )
    toc = TableOfContents()
    toc.levelStyles = [styles["TOC0"], styles["TOC1"], styles["TOC1"]]
    story.append(toc)
    story.append(PageBreak())

    markdown = source.read_text(encoding="utf-8")
    # Metadata/title are represented on the cover; start at the execution roadmap.
    first_section = markdown.find("# 0.")
    if first_section < 0:
        first_section = markdown.find("# 1.")
    if first_section >= 0:
        markdown = markdown[first_section:]
    story.extend(parse_markdown(markdown, styles, body_frame._width))

    doc.multiBuild(story)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    build_pdf(args.source.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
