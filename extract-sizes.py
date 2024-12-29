#!/usr/bin/env python3
import argparse
from warcio import ArchiveIterator
from multiprocessing import Pool
import pandas as pd
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

    sizes = []
    for filepath in utils.read_text_targets(args.file, try_read_file=False):
        if verbose:
            utils.eprint("Parsing {}".format(filepath))
        with open(filepath, "rb") as fi:
            for record in ArchiveIterator(fi):
                if not is_valid_record(record):
                    continue
                payload_size = utils.get_record_payload_len(record)
                sizes.append(payload_size)

    print(pd.Series(sizes).describe())

def is_valid_record(record):
    if record.rec_type != "response":
        return False

    if record.payload_length == 0:
        return False

    status_code = utils.get_record_status_code(record)
    if status_code not in utils.VALID_STATUS_CODES:
        return False

    content_type = utils.get_record_content_type(record)
    if not utils.is_text_content_type(content_type):
        return False

    if utils.get_record_tld(record) not in utils.ALLOWED_TLDS:
        return False

    return True


if __name__ == "__main__":
    exit(main())
