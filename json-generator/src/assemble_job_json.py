
#!/usr/bin/env python3
import argparse, json
from pathlib import Path

def assemble(outdir: str, target: str, malcode: str) -> None:
    od = Path(outdir)
    job_dir = od / f"{target}_job"
    job_dir.mkdir(parents=True, exist_ok=True)

    # read partials
    data_sourcing = json.loads((od / "data_sourcing_module.json").read_text())
    dt_modules    = json.loads((od / "data_transformation_modules.json").read_text())
    load_enrich   = json.loads((od / "load_enrich_module.json").read_text())

    # merge
    modules = {
        "data_sourcing_process": data_sourcing,
        **dt_modules,
        f"dt_{target}_{malcode.lower()}": {
            "sql": f'"""SELECT *\nFROM dt_{target}_{malcode.lower()}_ossbr_2_1 base"""',
            "loggable": True,
            "options": {"module":"data_transformation","method":"process"},
            "name": f"dt_{target}_{malcode.lower()}"
        },
        "load_enrich_process": load_enrich
    }

    job_json = {
        "source malcode": malcode,
        "source basepath": malcode,
        "comment": f"Automated job for {target} from {malcode}",
        "modules": modules
    }
    path = job_dir / f"ew_123_{target}_{malcode.lower()}.json"
    path.write_text(json.dumps(job_json, indent=2))

    # simple log
    (job_dir / "assemble_log.md").write_text("# Assemble Log\n\n- Included data_sourcing, transformation modules, final dt view stub, and load_enrich.\n")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--outdir", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--malcode", required=True)
    args = p.parse_args()
    assemble(args.outdir, args.target, args.malcode)
