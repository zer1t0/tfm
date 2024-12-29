#!/usr/bin/env python3

import argparse
import csv
import sys
import tfmutils as utils

csv.field_size_limit(sys.maxsize)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "file",
        help="CSV files to join. If none then stdin will be used",
        nargs="*",
    )

    parser.add_argument(
        "-o", "--out",
        required=True,
        help="Output result file",
    )

    parser.add_argument(
        "-a", "--append",
        help="Append to output file",
        action="store_true"
    )

    return parser.parse_args()

def main():
    args = parse_args()

    output_file = args.out
    append = args.append
    mode = "a" if append else "w"

    with open(output_file, mode) as fo:
        writer = csv.DictWriter(fo, fieldnames=utils.DATASET_FIELDS)
        if not append:
            writer.writeheader()

        for filepath in utils.read_text_targets(args.file, try_read_file=False):
            with open(filepath, "r") as fi:
                reader = csv.DictReader(fi)
                
                rows = [r for r in reader]
                writer.writerows(rows)

    print(output_file, flush=True)

if __name__ == "__main__":
    exit(main())
