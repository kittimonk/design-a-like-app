import argparse
import os
import re


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def trim_data_sourcing_to_keep_modules_open(ds_text: str) -> str:
    """
    Keep everything up to and including the closing brace of data_sourcing_process,
    but drop the final two closing braces that close modules and the root object.

    We assume the data_sourcing file ends like:

            }
        }
    }

    and we want to keep only up to the first of those three '}' lines.
    """
    lines = ds_text.rstrip("\n").splitlines()
    brace_indices = [i for i, line in enumerate(lines) if line.strip() == "}"]

    if len(brace_indices) < 3:
        raise RuntimeError(
            "data_sourcing file does not have at least three standalone '}' lines "
            "(expected closing data_sourcing_process, modules, and root)."
        )

    # Index of the '}' that closes data_sourcing_process (3rd from the end)
    cutoff = brace_indices[-3] + 1  # +1 to include that line
    kept_lines = lines[:cutoff + 1]
    return "\n".join(kept_lines)


def indent_block(text: str, indent: str = "        ") -> str:
    """Indent every non-empty line by the given indent (for nesting under modules)."""
    lines = text.rstrip("\n").splitlines()
    indented = []
    for line in lines:
        if line.strip() == "":
            indented.append("")
        else:
            indented.append(indent + line)
    return "\n".join(indented)


def infer_source_malcode(ds_text: str) -> str:
    m = re.search(r'source\.malcode:\s*"([^"]+)"', ds_text)
    return m.group(1) if m else "unknown"


def infer_target_table(le_text: str) -> str:
    # target_table: "/tantrum"
    m = re.search(r'target_table:\s*"/?([^"]+)"', le_text)
    return m.group(1) if m else "unknown"


def main():
    ap = argparse.ArgumentParser(
        description="Combine data sourcing, data transformation, and load_enrich "
                    "outputs into a single hybrid job file."
    )
    ap.add_argument("--data-sourcing", required=True,
                    help="Path to data_sourcing_module_v11a.txt")
    ap.add_argument("--dt-view", required=True,
                    help="Path to dt_*_*.job.txt (data transformation view)")
    ap.add_argument("--load-enrich", required=True,
                    help="Path to load_enrich_*_*.job.txt")
    ap.add_argument("--job-id", required=True,
                    help="Job id to use in final file name, e.g. 11000")
    ap.add_argument("--output-dir", required=True,
                    help="Directory to write final combined job file")
    args = ap.parse_args()

    ds_text = read_text(args.data_sourcing)
    dt_text = read_text(args.dt_view)
    le_text = read_text(args.load_enrich)

    # 1) Trim data_sourcing so that modules remain open
    ds_prefix = trim_data_sourcing_to_keep_modules_open(ds_text)

    # 2) Indent dt & load_enrich blocks so they live under modules:
    dt_block = indent_block(dt_text, "        ")
    le_block = indent_block(le_text, "        ")

    # 3) Infer malcode & target table name for filename (no content changes)
    source_malcode = infer_source_malcode(ds_text)
    target_table = infer_target_table(le_text)

    # 4) Stitch together:
    #    [data_sourcing header + data_sourcing_process block]
    #    + blank line
    #    + dt_* block
    #    + blank line
    #    + load_enrich block
    #    + closing } for modules and root
    combined_parts = [
        ds_prefix,
        "",
        dt_block,
        "",
        le_block,
        "    }",
        "}",
    ]
    final_text = "\n".join(combined_parts) + "\n"

    os.makedirs(args.output_dir, exist_ok=True)
    outfile_name = f"cw_{args.job_id}_{target_table}_{source_malcode}.txt"
    outfile_path = os.path.join(args.output_dir, outfile_name)

    with open(outfile_path, "w", encoding="utf-8") as f:
        f.write(final_text)

    print(f"✅ Combined job written to: {outfile_path}")
    print(f"   source.malcode    = {source_malcode}")
    print(f"   target_table      = {target_table}")
    print(f"   job id            = {args.job_id}")
    print("   Structure: data_sourcing_process + dt_view + load_enrich under modules{}.")


if __name__ == "__main__":
    main()
