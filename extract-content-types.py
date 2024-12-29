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

    content_types = {}
    for filepath in utils.read_text_targets(args.file, try_read_file=False):
        if verbose:
            utils.eprint("Parsing {}".format(filepath))
        with open(filepath, "rb") as fi:
            for record in ArchiveIterator(fi):
                if record.rec_type != "response":
                    continue
                status_code = utils.get_record_status_code(record)
                if status_code not in ["404", "200"]:
                    continue
                content_type = utils.get_record_content_type(record)
                try:
                    content_types[content_type] += 1
                except KeyError:
                    content_types[content_type] = 1


                    
    for k,v in sorted(content_types.items(), key=lambda x: x[1], reverse=True):
        if v < min_samples:
            continue
            
        if k == "/":
            k = "<None>"
        print("{}: {}".format(k, v))

if __name__ == "__main__":
    exit(main())
