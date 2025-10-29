
#!/usr/bin/env python3
import argparse, json
from typing import Dict, Set
import pandas as pd
from common_utils import load_csv, learn_source_columns

def extract_sources(csv_path: str, outdir: str) -> None:
    df = load_csv(csv_path)
    sources = [s for s in df["src_table"].astype(str).str.strip().unique().tolist() if s]
    src_cols_map: Dict[str, Set[str]] = learn_source_columns(df)

    res = {"sources": sources, "columns_by_source": {k: sorted(list(v)) for k,v in src_cols_map.items()}}

    from pathlib import Path
    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    (out / "sources_columns.json").write_text(json.dumps(res, indent=2))

    # human log
    log = ["# Sources & Columns Used\n"]
    log.append("- Sources: " + ", ".join(sources))
    log.append("")
    for s in sources:
        cols = res["columns_by_source"].get(s.lower(), [])
        log.append(f"## {s}")
        for c in cols:
            log.append(f"- {c}")
        log.append("")
    (out / "sources_columns.md").write_text("\n".join(log))

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("csv")
    p.add_argument("--outdir", required=True)
    args = p.parse_args()
    extract_sources(args.csv, args.outdir)
