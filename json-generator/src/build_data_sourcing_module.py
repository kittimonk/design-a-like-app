
#!/usr/bin/env python3
import argparse, json
from pathlib import Path
from common_utils import load_csv

def build_data_sourcing(csv_path: str, outdir: str, malcode: str) -> None:
    df = load_csv(csv_path)
    sources = [s for s in df["src_table"].astype(str).str.strip().unique().tolist() if s]

    module = {
        "options": {"module": "data_sourcing_process", "method": "process"},
        "loggable": True,
        "sourcelist": sources
    }
    for sname in sources:
        module[sname] = {"type":"sz_zone","table.name":sname,"read-format":"view","path":f"${{adls.source.root}}/{sname}"}

    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    (out / "data_sourcing_module.json").write_text(json.dumps(module, indent=2))
    (out / "data_sourcing_module.md").write_text(
        "# Data Sourcing Module\n\n- Sources: " + ", ".join(sources) + "\n"
    )

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("csv")
    p.add_argument("--outdir", required=True)
    p.add_argument("--malcode", required=True)
    args = p.parse_args()
    build_data_sourcing(args.csv, args.outdir, args.malcode)
