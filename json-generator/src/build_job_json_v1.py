# build_job_json_v1.py
import json, argparse, os

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_job_block(model, sql_obj, malcode):
    """
    Builds the hybrid JSON/YAML job block.

    sql_obj is the JSON produced by the SQL generator:
    {
      "sql": "SELECT ...",
      "name": "dt_mcb_ossbr_2_1",
      ...
    }
    """
    base = model["base_entity"]
    sql_text = sql_obj["sql"].strip()

    job_name = f"dt_{malcode}_{base}"

    block = []
    block.append(f"{job_name}:")
    block.append("  {")
    block.append(f'    sql: """{sql_text}"""')
    block.append("    loggable: true,")
    block.append("    options: {")
    block.append("      module: data_transformation")
    block.append("      method: process")
    block.append("    },")
    block.append(f'    "name": "{job_name}"')
    block.append("  }")

    return "\n".join(block)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True, help="path to model_v1.json")
    ap.add_argument("--sqljson", required=True, help="path to dt_mcb_ossbr_2_1.json")
    ap.add_argument("--malcode", required=True, help="mcb, aaw, etc.")
    ap.add_argument("--output", required=True, help="output job block")
    args = ap.parse_args()

    model = read_json(args.model)
    sql_obj = read_json(args.sqljson)

    block = build_job_block(model, sql_obj, args.malcode)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(block)

    print(f"✅ Wrote: {args.output}")
    print(f"Job name → dt_{args.malcode}_{model['base_entity']}")

if __name__ == "__main__":
    main()
