
#!/usr/bin/env python3
import argparse, json
from pathlib import Path

def build_load_enrich(outdir: str, target: str, malcode: str) -> None:
    final_view = f"dt_{target}_{malcode.lower()}"
    module = {
        "options": {"module": "load_enrich_process", "method": "process"},
        "loggable": True,
        "sql": f"SELECT * FROM {final_view}",
        "target-path": "${adls.stage.root}/" + malcode,
        "mode-of-write": "replace_partition",
        "keys": "",
        "cdc-flag": False,
        "scd2-flag": False,
        "partition-by": "effective_dt",
        "target-format": "delta",
        "target-table": f"/{target}",
        "name": f"{target}_daily"
    }
    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    (out / "load_enrich_module.json").write_text(json.dumps(module, indent=2))
    (out / "load_enrich_module.md").write_text("# Load Enrich Module\n\n- Final view: " + final_view + "\n")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--outdir", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--malcode", required=True)
    args = p.parse_args()
    build_load_enrich(args.outdir, args.target, args.malcode)
