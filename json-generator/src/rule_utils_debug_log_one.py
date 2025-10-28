# rule_utils.py (v5) — preserves multi-line CASE logic; builds auditable WHERE; normalizes joins; smart "Set" parsing
import re
from typing import List, Tuple, Optional
import datetime
from pathlib import Path

# -------------------------------------------------------------------
# GLOBAL DEBUG SETTINGS
# -------------------------------------------------------------------
DEBUG_JOINS = True
DEBUG_BUSINESS_RULES = True
DEBUG_TRANSFORMATIONS = True

# Path for debug log output
DEBUG_LOG_PATH = Path("debug_outputs/sql_flow_debug.log")

# Internal line counter for log numbering
_debug_line_counter = 0

def _debug_log(section: str, content: str):
    """
    Append a structured, line-numbered block to the debug log file.
    Each block is separated by blank lines for readability.
    """
    global _debug_line_counter
    _debug_line_counter += 1
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"\n[{_debug_line_counter}] 🧩 {section} — {timestamp}\n")
        f.write("-" * 80 + "\n")
        for line in content.strip().splitlines():
            f.write(line + "\n")
        f.write("\n")  # blank line for readability

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
    """
    Extracts predicate expressions (WHERE filters) and notes from business rule lines.
    This function detects common phrases like 'duplicate', 'all spaces', 'not active', etc.
    """
    preds, notes = [], []
    for ln in lines:
        l = (ln or "").strip()
        if not l:
            continue

        # “duplicate” hint → ROW_NUMBER TODO note
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

    # Debug mode output
    if DEBUG_BUSINESS_RULES:
        _debug_log(
            "BUSINESS RULE EXTRACTION",
            "\n".join([
                "Predicates Detected:" if preds else "No predicates.",
                *[f"  - {p}" for p in preds],
                "",
                "Notes Detected:" if notes else "No notes.",
                *[f"  - {n}" for n in notes]
            ])
        )

    return preds, notes


def business_rules_to_where(biz_text: str) -> str:
    """
    Convert enumerated/paragraph business rules into a WHERE block with audit comments.
    Each rule produces:
    - Inline SQL predicates (TRIM, equality, etc.)
    - Notes for exceptions, duplicates, or TODOs

    Debug:
    ------
    If DEBUG_BUSINESS_RULES=True, prints both extracted predicates and notes for each rule.
    """
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

    block = "\n  ".join(body).strip()

    if DEBUG_BUSINESS_RULES:
        _debug_log("BUSINESS RULE WHERE BLOCK", block or "(empty)")

    return block

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
    """Detects and converts free-form 'Set to ...' or 'Straight move' rules into valid SQL expressions."""
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

    # 🟢 Handle "Straight move" patterns
    if re.search(r"straight\s*move", text, re.I):
        # If it mentions a date field or date formatting
        if re.search(r"yyyy[-/]mm[-/]dd", text, re.I) or re.search(r"date\s*field", text, re.I):
            # Format source as YYYY-MM-DD
            return "TO_DATE({source_column}, 'YYYY-MM-DD')"
        else:
            # Straight passthrough
            return "{source_column}"

    # fallback: strip comment markers and boilerplate
    cleaned = re.sub(r"--.*", "", original)
    cleaned = re.sub(r"(?i)\bset\s+to\b", "", cleaned)
    cleaned = re.sub(r"(?i)\bset\b", "", cleaned).strip(" :\"'")

    # 🩹 Fix: prevent wrapping CASE or SQL fragments in quotes
    if re.search(r"\b(case|when|select|join|from)\b", cleaned, re.I):
        return cleaned

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
        expr = src_col or "NULL"
        if DEBUG_TRANSFORMATIONS:
            _debug_log("TRANSFORMATION (default passthrough)", f"Target: {target_col}\nExpr: {expr}")
        return (expr, None)

    # 🔹 Step 1: Handle custom "Set to ..." rules (smart parser)
    # 🔹 Step 1A: Skip smart parser for CASE blocks (they already contain SQL)
    if not trans.strip().lower().startswith("case"):
        rule_expr = parse_set_rule(trans)
        if rule_expr:
            if DEBUG_TRANSFORMATIONS:
                _debug_log("TRANSFORMATION (smart rule)", f"Target: {target_col}\nRaw: {trans}\nParsed: {rule_expr}")
            return (rule_expr, None)
        
    # 🔹 Step 2: Handle generic literal "Set to ..." cases
    lit = parse_literal_set(trans)
    if lit is not None:
        if DEBUG_TRANSFORMATIONS:
            _debug_log("TRANSFORMATION (literal rule)", f"Target: {target_col}\nRaw: {trans}\nParsed: {lit}")
        return (lit, None)

    # 🔹 Step 3: Handle CASE blocks with possible FROM clauses
    core, trailing_comment = extract_case_core(trans)

    if DEBUG_TRANSFORMATIONS:
        _debug_log(
            "TRANSFORMATION (complex rule)",
            f"Target: {target_col}\nSource: {src_col}\nRaw:\n{trans}\n\nParsed Core:\n{core}\nTrailing:\n{trailing_comment or '(none)'}"
        )

    return (core, trailing_comment or None)

# ---------- Joins ----------

def normalize_join(join_text: str) -> str:
    """
    Normalize JOIN statements and enforce consistent LEFT JOIN behavior.

    Enhancements:
    - Converts ambiguous JOINs to LEFT JOIN (safe default)
    - Detects lookup/reference tables (_ref, _lkp, _xref, _map, _dim)
      and forces LEFT JOIN even if INNER JOIN is mentioned
    - Cleans malformed multi-table fragments (e.g., 'ossbr_2_1 mas GLSXREF ref')
    - Deduplicates whitespace and enforces SQL-safe formatting
    - Removes duplicated or concatenated JOIN fragments
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
    # 🩹 Fix: Remove concatenated or duplicate JOIN fragments (e.g. two JOINs stuck together)
    # -------------------------------------------------------------------
    s = re.sub(r"(LEFT\s+JOIN\s+[A-Za-z0-9_]+\s+[A-Za-z0-9_]+\s+)+", " ", s, flags=re.I)

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
        _debug_log("JOIN NORMALIZATION", s_final)

    return s_final

# ---------- Lookup detector ----------

def detect_lookup(texts: List[str]) -> bool:
    blob = " ".join([t for t in texts if t])
    return bool(re.search(r"(?i)\blookup\b|\bref(erence)?\b|standard[_\s-]*code", blob))