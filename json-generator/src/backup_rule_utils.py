# rule_utils.py (v6) — preserves multi-line CASE logic; builds auditable WHERE; normalizes joins
import re
from typing import List, Tuple, Optional

# ---------- Debug hooks (kept light; file-writer lives in build script) ----------
DEBUG_JOINS = False
DEBUG_TRANSFORMATIONS = False

def _debug_log(title: str, block: str):
    # build_sql_job.py writes to file; here we keep it minimal for import safety
    if DEBUG_JOINS or DEBUG_TRANSFORMATIONS:
        print(f"[DEBUG] {title}\n{block}\n")

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
    txt = trans_text.strip()
    if not _CASE_START.match(txt):
        return (txt, "")

    # Find the first FROM after the CASE block begins.
    # We keep everything up to (but not including) FROM in "core",
    # and emit the remainder as a commented context.
    parts = _FROM_SPLIT.split(txt, maxsplit=1)
    if len(parts) == 2:
        core = parts[0].rstrip()
        trailing = "FROM " + parts[1].strip()
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
    """Return expr casted to the given type. Handles NULL specially and keeps function calls unquoted.
       Patch 7 extension: optional COALESCE(expr, default_val) for default-aware casting.
    """
    if not target_datatype:
        return expr
    dt = target_datatype.strip().upper()

    # 🟢 NEW: apply COALESCE default if provided
    if default_val and default_val.strip() and default_val.strip().upper() != "NULL":
        expr = f"COALESCE({expr}, {default_val})"

    # NULL casting
    if expr.strip().upper() == "NULL":
        if "DECIMAL" in dt or "NUMERIC" in dt:
            return f"CAST(NULL AS {dt})"
        if any(t in dt for t in ["BIGINT", "INT", "INTEGER", "SMALLINT"]):
            return f"CAST(NULL AS {dt})"
        if any(t in dt for t in ["DATE", "TIMESTAMP"]):
            return f"CAST(NULL AS {dt})"
        return f"CAST(NULL AS {dt})"

    # Already function-like (TO_DATE, CURRENT_TIMESTAMP, CASE ...)
    if re.match(r"(?i)^\s*(TO_DATE|DATE|CURRENT_TIMESTAMP|CASE|COALESCE|CAST)\b", expr.strip(), re.I):
        return f"CAST({expr} AS {dt})" if ("DECIMAL" in dt or "NUMERIC" in dt or "BIGINT" in dt or "INT" in dt) else expr

    # simple numeric literals
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", expr.strip().strip("'")):
        lit = expr.strip().strip("'")
        if "DECIMAL" in dt or "NUMERIC" in dt:
            return f"CAST({lit} AS {dt})"
        if any(t in dt for t in ["BIGINT", "INT", "INTEGER", "SMALLINT"]):
            return f"CAST({lit} AS {dt})"
        # fall back to string for other types
        return f"CAST('{lit}' AS {dt})"

    # quoted text literal
    if re.fullmatch(r"'[^']*'", expr.strip()):
        if "DECIMAL" in dt or "NUMERIC" in dt or "BIGINT" in dt or "INT" in dt:
            # try to parse number inside quotes
            inner = expr.strip().strip("'")
            if re.fullmatch(r"[-+]?\d+(\.\d+)?", inner):
                return f"CAST({inner} AS {dt})"
        return f"CAST({expr} AS {dt})"

    # default
    return f"CAST({expr} AS {dt})" if ("DECIMAL" in dt or "NUMERIC" in dt or "BIGINT" in dt or "INT" in dt) else expr

# ---------- Utility: determine when to CAST ----------
def _needs_cast(expr: str) -> bool:
    """
    Decide whether the expression should be casted to a target datatype.
    Returns True for pure literals (numbers, quoted strings, NULL),
    and False if expression already contains CAST, COALESCE, TO_DATE, etc.
    
    Safe defaults:
    - Skips CASE, TO_DATE, CURRENT_TIMESTAMP, COALESCE, and existing CAST()
    - Triggers CAST for:
        * Plain numerics: 123, -2, 3.14
        * Quoted text: 'A'
        * NULL
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
    """Detects and converts free-form 'Set to ...' or 'Straight move' rules into valid SQL expressions.
       Enhanced (Patch 9): 
       - Smart detection for conditional defaults like 'if blank then 0', 'if empty pass N'
       - Proper cleanup of trailing developer markers like '--1A'
       - Maintains all previous behaviors (date handling, straight move, etc.)
    """
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

    # Conditional NULL patterns (simple heuristic)
    if re.search(r"set\s+to\s+null\s+if", text):
        # we return a placeholder; build layer can expand with src_col
        return "CASE WHEN {source_column} IS NULL OR TRIM({source_column})='' THEN NULL ELSE {source_column} END"

    # 🟢 NEW: Smart default detection for conditional rules
    # Matches: "if blank then 0", "if empty pass 0", "when null assign 1", etc.
    m_default = re.search(
        r"(if|when)\s+(blank|empty|null|missing)[^a-z0-9]+(then|use|set|assign|pass)\s+(['\"]?[-\w\.]+['\"]?)",
        text,
        re.I,
    )
    if m_default:
        val = m_default.group(4).strip("'\" ")
        # numeric or decimal
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", val):
            return f"COALESCE({{source_column}}, {val})"
        # NULL explicitly
        if val.lower() == "null":
            return "COALESCE({source_column}, NULL)"
        # otherwise treat as string
        return f"COALESCE({{source_column}}, '{val}')"

    # Dates like 9999-12-31 (optionally with cast directions)
    mdate = re.search(r"(\d{4}-\d{2}-\d{2})", original)
    if mdate:
        d = mdate.group(1)
        if "cast" in text:
            return f"CAST('{d}' AS DATE)"
        return f"'{d}'"

    # “Set X to Y” or “Set to Y” (strip developer notes in parentheses)
    m = re.search(r"(?i)set(?:\s+\w+)?\s+to\s+(.+)$", original)
    if m:
        val = m.group(1).strip().rstrip(".")
        # 🩹 FIX: remove inline comment markers like "--1A" first
        val = re.sub(r"--.*", "", val).strip()
        # remove trailing parenthetical commentary
        val = re.sub(r"\s*\(.*?\)\s*$", "", val).strip()

        # +00331 → 331 (strip leading plus zeros if numeric)
        if re.fullmatch(r"[+]?0*\d+", val):
            num = re.sub(r"^[+]?0*", "", val) or "0"
            return num
        # plain quoted or bare tokens
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", val):
            return val
        if re.fullmatch(r"'[^']*'", val):
            return val
        val_clean = val.strip().strip("'").strip('"')
        return f"'{val_clean}'"

    # 🟢 Handle "Straight move" patterns
    if re.search(r"straight\s*move", text, re.I):
        if re.search(r"yyyy[-/]mm[-/]dd", text, re.I) or re.search(r"date\s*field", text, re.I):
            return "TO_DATE({source_column}, 'YYYY-MM-DD')"
        else:
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

    # 🔹 Skip smart parser for CASE blocks (they already contain SQL)
    if trans.strip().lower().startswith("case"):
        core, trailing_comment = extract_case_core(trans)
        return (core, trailing_comment or None)

    # 🔹 Smart rule / literal handlers
    rule_expr = parse_set_rule(trans)
    if rule_expr:
        return (rule_expr, None)

    lit = parse_literal_set(trans)
    if lit is not None:
        return (lit, None)

    # default: return cleaned text (may be a bare column or function)
    return (trans.strip(), None)

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

    # 🩹 Fix: Remove concatenated or duplicate JOIN fragments (e.g. two JOINs stuck together)
    s = re.sub(r"(LEFT\s+JOIN\s+[A-Za-z0-9_]+\s+[A-Za-z0-9_]+\s+)+", " ", s, flags=re.I)

    # 1️⃣  Default JOIN type enforcement — use LEFT JOIN unless specified
    if not re.search(r"(?i)\b(left|right|full)\s+join\b", s):
        s = re.sub(r"(?i)\bjoin\b", "LEFT JOIN", s)

    # 2️⃣  Auto-detect lookup/reference tables and force LEFT JOIN
    lookup_pattern = r"(?i)\b(_ref|_lkp|_xref|_map|_dim)\b"
    if re.search(lookup_pattern, s):
        s = re.sub(r"(?i)\b(inner|right|full)\s+join\b", "LEFT JOIN", s)

    # 3️⃣  Fix malformed fragments like "LEFT JOIN ossbr_2_1 mas GLSXREF ref ON ..."
    #      → retain only last two tokens before ON (GLSXREF ref)
    m = re.search(r"LEFT JOIN\s+([A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)*)\s+ON\s+(.*)", s, re.I)
    if m:
        table_block = m.group(1)
        cond = m.group(2)
        parts = table_block.split()
        if len(parts) > 2:  # keep only last two tokens: table + alias
            table_block = " ".join(parts[-2:])
        s = f"LEFT JOIN {table_block} ON {cond}"

    s_final = squash(s)

    if DEBUG_JOINS:
        _debug_log("JOIN NORMALIZATION", s_final)

    return s_final

# ---------- Lookup detection (for job JSON) ----------

def detect_lookup(text_blocks: List[str]) -> bool:
    """
    Lightweight detector: return True if the combined text mentions lookup/reference tables.
    Used by build_sql_job to decide whether to include lookup_cd module.
    """
    if not text_blocks:
        return False
    joined = " ".join(str(t).lower() for t in text_blocks)
    patterns = ["lookup", "_lkp", "_xref", "_map", "_ref", "code_mapping"]
    return any(p in joined for p in patterns)
