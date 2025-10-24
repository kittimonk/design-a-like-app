# rule_utils.py (v5) — preserves multi-line CASE logic; builds auditable WHERE; normalizes joins; smart "Set" parsing
import re
from typing import List, Tuple, Optional

# -------------------------------------------------------------------
# GLOBAL DEBUG SWITCHES
# -------------------------------------------------------------------
DEBUG_JOINS = False           # Print normalized JOINs during processing
DEBUG_BUSINESS_RULES = False  # Print extracted predicates & notes for business rules


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
    """Detect 'Set to <X>' patterns → return a SQL literal (legacy simple handler)."""
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
    txt = trans_text.strip()
    if not _CASE_START.match(txt):
        return (txt, "")

    parts = _FROM_SPLIT.split(txt, maxsplit=1)
    if len(parts) == 2:
        core = parts[0].rstrip()
        trailing = "FROM " + parts[1].strip()
        return (core, f"-- Source context preserved: {squash(trailing)}")
    return (txt, "")

# ---------- Smart Parser for 'Set' Rules (enhanced) ----------

def parse_set_rule(rule_text: str) -> Optional[str]:
    """Detects and converts free-form 'Set to ...' rules into valid SQL expressions."""
    if not rule_text or not isinstance(rule_text, str):
        return None

    # keep original for picking exact tokens (like quoted dates), but also a lower variant
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
        # use triple double quotes around the variable
        return "TO_DATE('\"\"\"${etl.effective.start.date}\"\"\"', 'yyyyMMddHHmmss')"

    # Dates like 9999-12-31 (optionally with cast directions)
    mdate = re.search(r"(\d{4}-\d{2}-\d{2})", original)
    if mdate:
        d = mdate.group(1)
        if "cast" in text:
            return f"CAST('{d}' AS DATE)"
        return f"'{d}'"

    # “Set X to Y” or “Set to Y”
    m = re.search(r"(?i)set(?:\s+\w+)?\s+to\s+(['\"]?)([A-Za-z0-9_]+)\1", original)
    if m:
        val = m.group(2)
        return f"'{val}'"

    # fallback: strip comment markers and boilerplate
    cleaned = re.sub(r"--.*", "", original)
    cleaned = re.sub(r"(?i)\bset\s+to\b", "", cleaned)
    cleaned = re.sub(r"(?i)\bset\b", "", cleaned).strip(" :\"'")
    if cleaned.upper() == "NULL":
        return "NULL"
    return f"'{cleaned}'" if cleaned else None

def transformation_expression(trans: str, target_col: str, src_col: str) -> Tuple[str, Optional[str]]:
    """
    Build the actual SQL expression for the SELECT list.

    Returns:
      (expression_sql, trailing_comment_or_None)
    """
    trans = clean_free_text(trans)
    if not trans:
        # no transformation text; use source column if present
        return (src_col or "NULL", None)

    # Smart "Set ..." handler first
    if re.match(r"(?i)^\s*set\b", trans):
        lit = parse_set_rule(trans)
        if lit is not None:
            return (lit, None)

    # Simple literal "set to ..." (legacy)
    lit = parse_literal_set(trans)
    if lit is not None:
        return (lit, None)

    # Preserve CASE body and move trailing FROM/JOIN into comment
    core, trailing_comment = extract_case_core(trans)
    return (core, trailing_comment or None)

# ---------- Joins ----------

# -------------------------------------------------------------------
# DEBUG CONTROL — set to True to print every JOIN normalization
# -------------------------------------------------------------------
DEBUG_JOINS = False


def normalize_join(join_text: str) -> str:
    """
    Normalize JOIN statements and enforce consistent LEFT JOIN behavior.

    Enhancements:
    - Converts ambiguous JOINs to LEFT JOIN (safe default)
    - Detects lookup/reference tables (_ref, _lkp, _xref, _map, _dim)
      and forces LEFT JOIN even if INNER JOIN is mentioned
    - Cleans malformed multi-table fragments (e.g., 'ossbr_2_1 mas GLSXREF ref')
    - Deduplicates whitespace and enforces SQL-safe formatting
    - Optional debug mode to print every normalized JOIN (set DEBUG_JOINS=True)

    Modification guide:
    --------------------
    1️⃣  If you prefer INNER JOIN by default → comment out the LEFT JOIN substitution block.
    2️⃣  If you want to allow all join types (inner/right/full) → comment out the LEFT JOIN enforcement section.
    3️⃣  If you don’t want auto LEFT JOIN for lookup tables → comment out the lookup_pattern section.
    4️⃣  If debugging JOIN parsing → set DEBUG_JOINS = True above.
    """

    if not join_text:
        return ""

    # Basic cleanup
    s = clean_free_text(join_text).strip()
    s = re.sub(r"(?i)\bwith\b", " ", s)
    s = re.sub(r"(?i)\binner\s+join\b", "JOIN", s)
    s = re.sub(r"(?i)\bjoin\b", "JOIN", s)
    s = re.sub(r"[;\n]+", " ", s)

    # -------------------------------------------------------------------
    # 1️⃣  Default JOIN type enforcement — use LEFT JOIN unless specified
    # -------------------------------------------------------------------
    if not re.search(r"(?i)\b(left|right|full)\s+join\b", s):
        s = re.sub(r"(?i)\bjoin\b", "LEFT JOIN", s)

    # -------------------------------------------------------------------
    # 2️⃣  Auto-detect lookup/reference tables and force LEFT JOIN
    # -------------------------------------------------------------------
    # This ensures all lookup-style joins are non-restrictive.
    lookup_pattern = r"(?i)\b(_ref|_lkp|_xref|_map|_dim)\b"
    if re.search(lookup_pattern, s):
        # Force LEFT JOIN regardless of user input
        s = re.sub(r"(?i)\b(inner|right|full)\s+join\b", "LEFT JOIN", s)

    # -------------------------------------------------------------------
    # 3️⃣  Fix malformed fragments like "LEFT JOIN ossbr_2_1 mas GLSXREF ref ON ..."
    #      → retain only last two tokens before ON (GLSXREF ref)
    # -------------------------------------------------------------------
    m = re.search(r"LEFT JOIN\s+([A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)*)\s+ON\s+(.*)", s, re.I)
    if m:
        table_block = m.group(1)
        cond = m.group(2)
        parts = table_block.split()
        # Keep only last two tokens: table + alias
        if len(parts) > 2:
            table_block = " ".join(parts[-2:])
        s = f"LEFT JOIN {table_block} ON {cond}"

    # Final cleanup and spacing normalization
    s_final = squash(s)

    if DEBUG_JOINS:
        print(f"[DEBUG] Normalized JOIN → {s_final}")

    return s_final

# ---------- Lookup detector ----------

def detect_lookup(texts: List[str]) -> bool:
    blob = " ".join([t for t in texts if t])
    return bool(re.search(r"(?i)\blookup\b|\bref(erence)?\b|standard[_\s-]*code", blob))
