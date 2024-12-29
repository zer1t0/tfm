#!/usr/bin/env python3
import argparse
from warcio import ArchiveIterator
from multiprocessing import Pool
import tfmutils as utils

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "file",
        help="files to process. "
        "If none then stdin will be use",
        nargs="*",
    )

    parser.add_argument(
        "-m", "--min",
        help="Minimum number of samples to show",
        default=1,
        type=int
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    verbose = args.verbose
    min_samples = args.min

    status_codes = {}
    for filepath in utils.read_text_targets(args.file, try_read_file=False):
        if verbose:
            utils.eprint("Parsing {}".format(filepath))
        with open(filepath, "rb") as fi:
            for record in ArchiveIterator(fi):
                if record.rec_type != "response":
                    continue
                try:
                    status_code = utils.get_record_status_code(record)
                except AttributeError:
                    continue

                try:
                    status_codes[status_code] += 1
                except KeyError:
                    status_codes[status_code] = 1


                    
    for k,v in sorted(status_codes.items(), key=lambda x: x[1], reverse=True):
        if v < min_samples:
            continue
        print("{}: {}".format(k, v))

if __name__ == "__main__":
    exit(main())
