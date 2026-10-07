"""Pure data transformations shared by the report and drill-through pages."""
from datetime import date, timedelta
from pathlib import Path
import posixpath
import re
import zipfile
import xml.etree.ElementTree as ET

import pandas as pd

METRICS = ["sale_qty", "sale_value", "available_qty", "available_value", "inward_qty", "inward_value"]
DATE_PRESETS = ["Today", "Yesterday", "Last 7 days", "Last 30 days", "Week to date", "Month to date", "Quarter to date", "Year to date", "Custom range"]


def date_bounds(preset, today):
    if preset == "Yesterday":
        return today - timedelta(days=1), today - timedelta(days=1)
    starts = {
        "Today": today,
        "Last 7 days": today - timedelta(days=6),
        "Last 30 days": today - timedelta(days=29),
        "Week to date": today - timedelta(days=today.weekday()),
        "Month to date": today.replace(day=1),
        "Quarter to date": date(today.year, ((today.month - 1) // 3) * 3 + 1, 1),
        "Year to date": date(today.year, 1, 1),
    }
    return starts[preset], today


def category_key(value):
    return re.sub(r"\s*/\s*", " / ", re.sub(r"\s+", " ", str(value).strip())).casefold()


def read_category_mapping(path):
    """Read the two mapping columns from OOXML without an Excel dependency.

    Text and cached cell values only; workbook contents are treated as data.
    Conflicting parent mappings fail instead of multiplying report totals.
    """
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(Path(path)) as workbook:
        strings = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
            strings = ["".join(t.text or "" for t in item.findall(".//s:t", ns)) for item in root.findall("s:si", ns)]
        sheet = ET.fromstring(workbook.read("xl/workbook.xml")).find("s:sheets/s:sheet", ns)
        rel_id = sheet.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        relationships = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
        target = next(rel.get("Target") for rel in relationships if rel.get("Id") == rel_id)
        sheet_path = target.lstrip("/") if target.startswith("/") else posixpath.normpath("xl/" + target)
        rows = ET.fromstring(workbook.read(sheet_path)).findall(".//s:sheetData/s:row", ns)
        records = []
        for row in rows:
            values = {}
            for cell in row:
                column = re.sub(r"\d", "", cell.get("r", ""))
                value = cell.find("s:v", ns)
                text = value.text if value is not None else ""
                if cell.get("t") == "s":
                    text = strings[int(text)] if text else ""
                elif cell.get("t") == "inlineStr":
                    text = "".join(t.text or "" for t in cell.findall(".//s:t", ns))
                values[column] = (text or "").strip()
            records.append(values)
    if not records:
        raise ValueError("The category workbook is empty.")
    headers = {value.casefold(): column for column, value in records[0].items()}
    if not {"category", "display name"}.issubset(headers):
        raise ValueError("The workbook must contain CATEGORY and Display Name columns.")
    mapping = {}
    for row in records[1:]:
        parent = row.get(headers["category"], "")
        child = row.get(headers["display name"], "")
        if not parent or not child:
            continue
        key = category_key(child)
        if key in mapping and mapping[key] != parent:
            raise ValueError(f"Conflicting master categories for {child}: {mapping[key]} and {parent}")
        mapping[key] = parent
    return mapping


def add_master_category(df, mapping, column):
    result = df.copy()
    result["master_category"] = result[column].map(lambda value: mapping.get(category_key(value), "Unmapped"))
    return result


def aggregate_report(df, keys):
    return df.groupby(keys, dropna=False, as_index=False)[METRICS].sum()


def consolidate_products(df, kind):
    """Sum facts, retain dimension descriptions, never deduplicate by picture."""
    if df.empty:
        return df.copy()
    quantities = ["sale_qty", "sale_value"] if kind == "sales" else ["available_inventory", "available_selling_price"]
    def labels(series):
        return ", ".join(sorted(set(series.dropna().astype(str))))
    rules = {column: "sum" for column in quantities}
    for column in df.columns:
        if column == "product_id" or column in rules:
            continue
        if column in {"company", "location", "lot_number"}:
            rules[column] = labels
        elif column in {"overall_age", "sale_age"}:
            rules[column] = "max"
        elif column == "sold_date":
            rules[column] = "max"
        elif column == "stock_move_date":
            rules[column] = "min"
        else:
            rules[column] = "first"
    result = df.groupby("product_id", dropna=False, as_index=False).agg(rules)
    # The consolidated sale age must correspond to its latest sale date.
    if kind == "sales" and "sold_date" in result:
        reference = df.attrs.get("age_reference")
        if reference is not None:
            result["sale_age"] = (pd.Timestamp(reference) - pd.to_datetime(result["sold_date"])).dt.days
    return result


def scope_predicate(column, values, null_label=None):
    """Build a parameterized scope; empty selection means all values."""
    if not values:
        return "1 = 1", []
    nonnull = [value for value in values if value != null_label]
    clauses = []
    if nonnull:
        clauses.append(f"{column} IN ({', '.join('?' for _ in nonnull)})")
    if null_label is not None and null_label in values:
        clauses.append(f"{column} IS NULL")
    return "(" + " OR ".join(clauses) + ")", nonnull
