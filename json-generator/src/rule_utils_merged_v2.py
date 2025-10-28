# rule_utils_merged_v1_1.py — full version (based on v6, 100% preserved + 2 safe helpers)
import re
from typing import List, Tuple, Optional
import datetime
from pathlib import Path

# Full debug flags (preserved behavior)
DEBUG_BUSINESS_RULES = True  # ensure business rule debug is on

# Path for debug log output and internal line counter
DEBUG_LOG_PATH = Path("debug_outputs/sql_flow_debug.log")
_debug_line_counter = 0

def _debug_log(section: str, content: str):
    """Write numbered, timestamped blocks to debug_outputs/sql_flow_debug.log"""
    global _debug_line_counter
    _debug_line_counter += 1
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"\n[{_debug_line_counter}] 🧩 {section} — {ts}\n")
        f.write("-" * 80 + "\n")
        for line in (content or "").strip().splitlines():
            f.write(line + "\n")
        f.write("\n")

# ============================================================
# [PATCH v1.1 additions for build_sql_job_merged_v1_1.py]
# ============================================================

_JOIN_RX = re.compile(
    r"(?i)^\s*(left|inner|right|full)\s+join\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?\s+on\s+(.+)$"
)

def _ensure_unique_join_aliases(joins: list, base_alias: str = "mas") -> list:
    """
    Ensure each LEFT JOIN alias is unique and deduplicated.
    Matches the aliasing logic used inside build_sql_job_merged_v1_1.py
    """
    used = {base_alias.lower()}
    out, seen = [], set()
    for j in joins:
        if not j or not isinstance(j, str):
            continue
        m = _JOIN_RX.match(j.strip())
        if not m:
            sig = re.sub(r"\s+", " ", j.strip().lower())
            if sig not in seen:
                seen.add(sig)
                out.append(j)
            continue
        _, table, alias, on = m.groups()
        if not alias or alias.lower() in used:
            alias_base = "ref" if table.lower().startswith("gls") else table.lower()[:3]
            k = 1
            alias2 = alias_base
            while alias2.lower() in used:
                alias2 = f"{alias_base}{k}"
                k += 1
            alias = alias2
        used.add(alias.lower())
        on_norm = re.sub(r"\s+", " ", on).strip()
        js = f"LEFT JOIN {table} {alias} ON {on_norm}"
        sig = re.sub(r"\s+", " ", js.lower())
        if sig not in seen:
            seen.add(sig)
            out.append(js)
    return out


_SUSPICIOUS = re.compile(r"(?i)string_agg\s*\(\s*format\s*\(\s*ascii\s*\(", re.DOTALL)
def _guard_suspicious(expr: str) -> str:
    """
    Guard unresolved function fragments (like STRING_AGG(FORMAT(ASCII...)) 
    so SQL compiles cleanly.
    """
    if not isinstance(expr, str):
        return expr
    if _SUSPICIOUS.search(expr or ""):
        short = (expr[:120] + "...") if len(expr) > 120 else expr
        short = short.replace("/*", "/ *").replace("*/", "* /")
        return f"NULL /* unresolved expression guarded: {short} */"
    return expr

# ---------- Debug hooks (kept light; file-writer lives in build script) ----------
DEBUG_JOINS = True
DEBUG_TRANSFORMATIONS = True


# ---------- Small utils ----------

def squash(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def clean_free_text(s: str) -> str:
    if not isinstance(s, str) or not s.strip():
        return ""
    # keep SQL-ish text; drop trailing “log an exception …” noise commonly found
    s = re.sub(r"(?i)\blog an exception.*", "", s)
    return s.strip()

# ---------- Business Rules → WHERE ----------

def _extract_predicates_from_lines(lines: List[str]) -> Tuple[List[str], List[str]]:
    preds, notes = [], []
    for ln in lines:
        l = (ln or "").strip()
        if not l:
            continue

        # “duplicate” hint → ROW_NUMBER TODO note (kept as comment)
        if re.search(r"(?i)\bduplicate\b.*\bossbr_2_1\.SRSECCODE\b", l):
            notes.append("-- TODO: Duplicates: enforce ROW_NUMBER() OVER (PARTITION BY mas.SRSECCODE ORDER BY <choose>) = 1")

        # “all spaces” on SRSECCODE
        if re.search(r"(?i)ossbr_2_1\.SRSECCODE.*all\s+spaces", l):
            preds.append("TRIM(mas.SRSECCODE) <> ''")

        # SRSTATUS <> 'A' → keep only active
        if re.search(r"(?i)ossbr_2_1\.SRSTATUS\s*<>?\s*'A'", l) or re.search(r"(?i)not\s+active", l):
            preds.append("mas.SRSTATUS = 'A'")

        # Mutual fund / SBB already extracted rule
        if re.search(r"(?i)GLSXREF", l) and re.search(r"(?i)SBB", l) and re.search(r"(?i)MFSPRIC", l):
            preds.append(
                "NOT (ref.WASTE_SECURITY_CODE = mas.SRSECCODE "
                "AND LEFT(ref.FUND_COMPANY,3) = 'SBB' "
                "AND ref.FUND_NUMBER = mf.DTL_SEND_NUM)"
            )

        # “exclude” / “reject” notes preserved
        if re.search(r"(?i)\breject the record\b", l):
            notes.append(f"-- NOTE: Evaluate rule -> {l}")
        if re.search(r"(?i)\bexclude the record\b", l):
            notes.append(f"-- NOTE: Exclusion rule -> {l}")

    return preds, notes

def business_rules_to_where(biz_text: str) -> str:
    """Convert enumerated/paragraph business rules into a WHERE block with audit comments."""
    if not biz_text:
        return ""
    text = clean_free_text(biz_text)
    # split by bullets like "1)" and by newlines; keep content
    items = re.split(r"(?:^\s*\d+\)\s*|\n)+", text)
    items = [i for i in items if i and i.strip()]
    preds, notes = _extract_predicates_from_lines(items)
    body = []
    body.extend(notes)
    if preds:
        body.append(" AND ".join(preds))
    return "\n  ".join(body).strip()

# ---------- Transformation parsing (CASE preservation) ----------

def parse_literal_set(trans: str) -> Optional[str]:
    """Detect 'Set to <X>' patterns → return a SQL literal."""
    if not trans:
        return None
    m = re.search(r"(?i)\bset\s+to\s+(.+?)(?:\s*$|\.)", trans.strip())
    if not m:
        return None
    val = m.group(1).strip()
    # strip trailing parenthetical notes like (DB)
    val = re.sub(r"\s*\(.*?\)\s*$", "", val).strip()
    # numeric?
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", val):
        return val
    # +01342-like codes
    if re.match(r"^\+\d+", val):
        return f"'{val}'"
    # as text literal
    return "'" + val.strip("'\"") + "'"

_CASE_START = re.compile(r"(?is)^\s*case\b")
_FROM_SPLIT   = re.compile(r"(?is)\bfrom\b")

def extract_case_core(trans_text: str) -> Tuple[str, str]:
    """
    If text starts with CASE…END and later has FROM/JOINs, split:
      - return (case_block_without_trailing_from, trailing_from_comment)
    Otherwise, return (original_text, "")
    """
    # 🧩 Guard against empty or non-string text
    if not trans_text or not isinstance(trans_text, str):
        return ("", "")

    txt = trans_text.strip()
    if not txt:
        return ("", "")

    # 🧩 Guard: skip splitting if no FROM/JOIN keywords exist at all
    if "from" not in txt.lower() and "join" not in txt.lower():
        return (txt, "")

    if not _CASE_START.match(txt):
        return (txt, "")

    # Find the first FROM after the CASE block begins.
    # We keep everything up to (but not including) FROM in "core",
    # and emit the remainder as a commented context.
    parts = _FROM_SPLIT.split(txt, maxsplit=1)
    if len(parts) == 2:
        core = parts[0].rstrip()
        trailing = "FROM " + parts[1].strip()
        # 🧩 Strip redundant trailing FROM fragments (ossbr_2_1 etc.)
        trailing = re.sub(r"\bFROM\s+ossbr_2_1.*", "", trailing, flags=re.I)
        return (core, f"-- Source context preserved: {squash(trailing)}")
    return (txt, "")

# ---------- Smart datatype helpers (for casting) ----------

def _infer_datatype_from_value(value: str, explicit_type: Optional[str]) -> str:
    """Prefer explicit CSV type; else infer: numeric -> BIGINT, decimalx -> DECIMAL, quoted -> STRING."""
    if explicit_type and explicit_type.strip():
        return explicit_type.strip()
    if value is None:
        return "STRING"
    v = str(value).strip()

    # numeric (integer-like)
    if re.fullmatch(r"[-+]?\d+", v):
        return "BIGINT"
    # decimal
    if re.fullmatch(r"[-+]?\d+\.\d+", v):
        return "DECIMAL(17,2)"
    # date-ish keywords
    if "to_date(" in v.lower() or re.search(r"\d{4}-\d{2}-\d{2}", v):
        return "DATE"
    return "STRING"

def _cast_to_datatype(expr: str, target_datatype: str, default_val: Optional[str] = None) -> str:
    """
    Return expr casted to the given type. Handles NULL specially and keeps function calls unquoted.
    Enhanced for COALESCE-aware casting.
    """
    if not target_datatype:
        return expr
    dt = target_datatype.strip().upper()
    e = expr.strip()

    # Apply COALESCE default if provided
    if default_val and default_val.strip() and default_val.strip().upper() != "NULL":
        e = f"COALESCE({e}, {default_val.strip()})"

    # Handle NULL directly
    if e.upper() == "NULL":
        return f"CAST(NULL AS {dt})"

    # Avoid re-casting structured expressions (CASE, TO_DATE, CAST)
    if re.match(r"(?i)^(CASE|CAST|TO_DATE|COALESCE|CURRENT_TIMESTAMP)\b", e):
        return e

    # Numeric literals
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", e.strip("'")):
        val = e.strip("'")
        return f"CAST({val} AS {dt})"


    # Quoted string
    if re.fullmatch(r"'[^']*'", e):
        inner = e.strip("'")
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", inner):
            return f"CAST({inner} AS {dt})"
        return f"CAST({e} AS {dt})"

    return f"CAST({e} AS {dt})"

# ---------- Utility: determine when to CAST ----------
def _needs_cast(expr: str) -> bool:
    """
    Decide whether the expression should be casted to a target datatype.
    Returns True for pure literals (numbers, quoted strings, NULL),
    and False if expression already contains CAST, COALESCE, TO_DATE, etc.
    """
    if not expr or not isinstance(expr, str):
        return False
    ex = expr.strip().upper()

    # Already a structured SQL expression — skip casting
    if any(keyword in ex for keyword in ["CAST(", "COALESCE(", "TO_DATE(", "CURRENT_TIMESTAMP", "CASE "]):
        return False

    # Simple numeric literal or quoted string literal
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", expr.strip().strip("'")):
        return True
    if re.fullmatch(r"'[^']*'", expr.strip()):
        return True
    if expr.strip().upper() == "NULL":
        return True

    return False

# ---------- Main transformation expression ----------

def parse_set_rule(rule_text: str) -> Optional[str]:
    """Detects and converts free-form 'Set to ...' or 'Straight move' rules into valid SQL expressions."""
    if not rule_text or not isinstance(rule_text, str):
        return None

    original = rule_text.strip()
    text = original.lower()

    # NULL
    if "set to null" in text:
        return "NULL"

    # current timestamp (function, not string)
    if "current_timestamp" in text:
        return "CURRENT_TIMESTAMP()"

    # ETL effective date parameter
    if "etl.effective.start.date" in text:
        return "TO_DATE('\"\"\"${etl.effective.start.date}\"\"\"', 'yyyyMMddHHmmss')"

    # Conditional NULL patterns
    if re.search(r"set\s+to\s+null\s+if", text):
        return "CASE WHEN {source_column} IS NULL OR TRIM({source_column})='' THEN NULL ELSE {source_column} END"

    # Default rules: if blank/empty then 0
    m_default = re.search(
        r"(if|when)\s+(blank|empty|null|missing)[^a-z0-9]+(then|use|set|assign|pass)\s+(['\"]?[-\w\.]+['\"]?)",
        text,
        re.I,
    )
    if m_default:
        val = m_default.group(4).strip("'\" ")
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", val):
            return f"COALESCE({{source_column}}, {val})"
        if val.lower() == "null":
            return "COALESCE({source_column}, NULL)"
        return f"COALESCE({{source_column}}, '{val}')"

    # Dates like 9999-12-31
    mdate = re.search(r"(\d{4}-\d{2}-\d{2})", original)
    if mdate:
        d = mdate.group(1)
        if "cast" in text:
            return f"CAST('{d}' AS DATE)"
        return f"'{d}'"

    # “Set X to Y” or “Set to Y”
    m = re.search(r"(?i)set(?:\s+\w+)?\s+to\s+(.+)$", original)
    if m:
        val = m.group(1).strip().rstrip(".")
        val = re.sub(r"--.*", "", val).strip()
        val = re.sub(r"\s*\(.*?\)\s*$", "", val).strip()

        # numeric normalization
        if re.fullmatch(r"[+]?0*\d+", val):
            num = re.sub(r"^[+]?0*", "", val) or "0"
            return num
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", val):
            return val
        if re.fullmatch(r"'[^']*'", val):
            return val
        val_clean = val.strip().strip("'").strip('"')
        return f"'{val_clean}'"

    # Straight move
    if re.search(r"straight\s*move", text, re.I):
        if re.search(r"yyyy[-/]mm[-/]dd", text, re.I) or re.search(r"date\s*field", text, re.I):
            return "TO_DATE({source_column}, 'YYYY-MM-DD')"
        else:
            return "{source_column}"

    # Fallback
    cleaned = re.sub(r"--.*", "", original)
    cleaned = re.sub(r"(?i)\bset\s+to\b", "", cleaned)
    cleaned = re.sub(r"(?i)\bset\b", "", cleaned).strip(" :\"'")
    if re.search(r"\b(case|when|select|join|from)\b", cleaned, re.I):
        return cleaned
    if cleaned.upper() == "NULL":
        return "NULL"
    return f"'{cleaned}'" if cleaned else None

def transformation_expression(trans: str, target_col: str, src_col: str, target_datatype: Optional[str] = None) -> Tuple[str, Optional[str]]:
    """
    Build the actual SQL expression for the SELECT list.
    Returns:
      (expression_sql, trailing_comment_or_None)
    """
    trans = clean_free_text(trans)
    if not trans:
        expr = src_col or "NULL"
        return (expr, None)

    # Skip smart parser for CASE blocks
    if trans.strip().lower().startswith("case"):
        core, trailing_comment = extract_case_core(trans)
        return (core, trailing_comment or None)

    rule_expr = parse_set_rule(trans)
    if rule_expr:
        return (rule_expr, None)

    lit = parse_literal_set(trans)
    if lit is not None:
        return (lit, None)

    return (trans.strip(), None)

# ---------- Joins ----------

def normalize_join(join_text: str) -> str:
    """
    Normalize JOIN statements and enforce consistent LEFT JOIN behavior.
    """
    # 🧩 Guard: skip empty, NaN, or malformed text
    if not join_text or str(join_text).strip().lower() in ("nan", "none", ""):
        return ""

    s = clean_free_text(join_text).strip()
    s = re.sub(r"(?i)\bwith\b", " ", s)
    s = re.sub(r"(?i)\binner\s+join\b", "JOIN", s)
    s = re.sub(r"(?i)\bjoin\b", "JOIN", s)
    s = re.sub(r"[;\n]+", " ", s)
    s = re.sub(r"\s+FROM\s+[A-Za-z0-9_\. ]+(?=(\s+(LEFT|INNER|RIGHT|FULL)\s+JOIN\b|\s*$))", "", s, flags=re.I)
    s = re.sub(r"(?i)\bjoin\s+\S+\s+with\s+([A-Za-z0-9_]+)\s+([A-Za-z0-9_]+)", r"LEFT JOIN \1 \2", s)

    m = re.search(r"LEFT JOIN\s+([A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)*)\s+ON\s+(.*)", s, re.I)
    if m:
        table_block = m.group(1)
        cond = m.group(2)
        parts = table_block.split()
        if len(parts) > 2:
            table_block = " ".join(parts[-2:])
        s = f"LEFT JOIN {table_block} ON {cond}"

    if not re.search(r"(?i)\b(left|right|full)\s+join\b", s):
        s = re.sub(r"(?i)\bjoin\b", "LEFT JOIN", s)

    lookup_pattern = r"(?i)\b(_ref|_lkp|_xref|_map|_dim)\b"
    if re.search(lookup_pattern, s):
        s = re.sub(r"(?i)\b(inner|right|full)\s+join\b", "LEFT JOIN", s)

    m = re.search(r"LEFT JOIN\s+([A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)*)\s+ON\s+(.*)", s, re.I)
    if m:
        table_block = m.group(1)
        cond = m.group(2)
        parts = table_block.split()
        if len(parts) > 2:
            table_block = " ".join(parts[-2:])
        s = f"LEFT JOIN {table_block} ON {cond}"

    s_final = squash(s)

    # 🧩 Remove trailing FROM ossbr_2_1 artifacts
    s_final = re.sub(r"\bFROM\s+ossbr_2_1.*", "", s_final, flags=re.I)

    if DEBUG_JOINS:
        _debug_log("JOIN NORMALIZATION", s_final)
    return s_final

# ---------- Lookup detection ----------

def detect_lookup(text_blocks: List[str]) -> bool:
    """
    Lightweight detector: return True if combined text mentions lookup/reference tables.
    """
    if not text_blocks:
        return False
    joined = " ".join(str(t).lower() for t in text_blocks)
    patterns = ["lookup", "_lkp", "_xref", "_map", "_ref", "code_mapping"]
    return any(p in joined for p in patterns)
