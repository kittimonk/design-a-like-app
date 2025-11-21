import argparse
import os
import re

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def trim_data_sourcing_to_keep_modules_open(ds_text: str) -> str:
    lines = ds_text.rstrip("\n").splitlines()

    brace_indices = [
        i for i, line in enumerate(lines)
        if line.strip() in ("}", "},")
    ]

    if len(brace_indices) < 3:
        raise RuntimeError(
            "data_sourcing file does not have at least three closing braces "
        )

    cutoff = brace_indices[-3]
    kept_lines = lines[:cutoff + 1]
    return "\n".join(kept_lines)

def indent_block(text: str, indent: str = "        ") -> str:
    lines = text.rstrip("\n").splitlines()
    out = []
    for line in lines:
        if line.strip() == "":
            out.append("")
        else:
            out.append(indent + line)
    return "\n".join(out)

def infer_source_malcode(ds_text: str) -> str:
    m = re.search(r'source\.malcode:\s*"([^"]+)"', ds_text)
    return m.group(1) if m else "unknown"

def infer_target_table(le_text: str) -> str:
    m = re.search(r'target_table:\s*"/?([^"]+)"', le_text)
    return m.group(1) if m else "unknown"

def main():
    ap = argparse.ArgumentParser(description="Combine all modules into final job")
    ap.add_argument("--data-sourcing", required=True)
    ap.add_argument("--lookup", required=True)
    ap.add_argument("--dt-view", required=True)
    ap.add_argument("--load-enrich", required=True)
    ap.add_argument("--job-id", required=True)
    ap.add_argument("--output-dir", required=True)
    args = ap.parse_args()

    ds_text = read_text(args.data_sourcing)
    lookup_text = read_text(args.lookup)
    dt_text = read_text(args.dt_view)
    le_text = read_text(args.load_enrich)

    ds_prefix = trim_data_sourcing_to_keep_modules_open(ds_text)

    lookup_block = indent_block(lookup_text, "        ")
    dt_block = indent_block(dt_text, "        ")
    le_block = indent_block(le_text, "        ")

    malcode = infer_source_malcode(ds_text)
    target_table = infer_target_table(le_text)

    combined_parts = [
        ds_prefix,
        "",
        lookup_block,
        "",
        dt_block,
        "",
        le_block,
        "    }",
        "}",
    ]

    final_text = "\n".join(combined_parts) + "\n"
    os.makedirs(args.output_dir, exist_ok=True)

    outfile = (
        f"{args.output_dir}/cw_{args.job_id}_{target_table}_{malcode}.txt"
    )

    with open(outfile, "w", encoding="utf-8") as f:
        f.write(final_text)

    print("✅ Combined job written:", outfile)

if __name__ == "__main__":
    main()
