# rule_utils.py (v4) — preserves multi-line CASE logic; builds auditable WHERE; normalizes joins
import re
from typing import List, Tuple, Optional

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

    # Handle simple "Set to ..." cases
    lit = parse_literal_set(trans)
    if lit is not None:
        return (lit, None)

    # If full fragment includes "AS <target>", we'll keep the alias if present;
    # but we'll still strip trailing FROM context if it exists.
    core, trailing_comment = extract_case_core(trans)
    # If user included "AS <target_col>" in the fragment, leave as-is; else add alias later in caller.
    return (core, trailing_comment or None)

# ---------- Joins ----------

def normalize_join(join_text: str) -> str:
    """Normalize JOIN notes like 'JOIN A WITH B ON A.id=B.id' → 'JOIN A B ON ...'"""
    if not join_text:
        return ""
    s = clean_free_text(join_text)
    s = re.sub(r"(?i)\bwith\b", " ", s)
    s = re.sub(r"(?i)\bjoin\b", "JOIN", s)
    s = re.sub(r"[;\n]+", " ", s)
    return squash(s)

# ---------- Lookup detector ----------

def detect_lookup(texts: List[str]) -> bool:
    blob = " ".join([t for t in texts if t])
    return bool(re.search(r"(?i)\blookup\b|\bref(erence)?\b|standard[_\s-]*code", blob))


# ---------- Parse rule smart function ----------

def parse_set_rule(rule_text: str) -> str:
    """Detects and converts free-form 'Set to ...' rules into valid SQL expressions."""
    if not rule_text or not isinstance(rule_text, str):
        return None

    text = rule_text.strip().lower()

    # Handle NULLs
    if "set to null" in text:
        return "NULL"

    # Handle current_timestamp()
    if "current_timestamp" in text:
        return "CURRENT_TIMESTAMP()"

    # Handle etl effective date patterns
    if "etl.effective.start.date" in text:
        return "TO_DATE('${etl.effective.start.date}', 'yyyyMMddHHmmss')"

    # Handle literal date values
    if re.search(r"\d{4}-\d{2}-\d{2}", text):
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
        if "cast" in text:
            return f"CAST('{date_match.group(1)}' AS DATE)"
        return f"'{date_match.group(1)}'"

    # Handle single char/literal rules like 'Set A to A --1A'
    if re.search(r"set\s+[a-z0-9_]+\s+to\s+([a-z0-9]+)", rule_text, re.I):
        val = re.findall(r"set\s+[a-z0-9_]+\s+to\s+([a-z0-9]+)", rule_text, re.I)[0]
        return f"'{val}'"

    # Fallback: clean comment markers and return sanitized text
    cleaned = re.sub(r"--.*", "", rule_text)
    cleaned = cleaned.replace("set to", "").replace("set", "").strip()
    return f"'{cleaned}'" if cleaned else "NULL"
