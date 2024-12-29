#!/usr/bin/env python3
import argparse
from warcio import ArchiveIterator
from warcio.warcwriter import WARCWriter
from urllib.parse import urlparse
import os
from multiprocessing import Pool
import tfmutils as utils
import logging

ALLOWED_TLDS = utils.DEFAULT_ALLOWED_TLDS
MAX_ALLOWED_PAYLOAD_SIZE = utils.DEFAULT_MAX_ALLOWED_PAYLOAD_SIZE
MAX_RECORDS_PER_FILE = 6000
VALID_STATUS_CODES = utils.DEFAULT_VALID_STATUS_CODES

logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "file",
        help="files to process. "
        "If none then stdin will be use",
        nargs="*",
    )

    parser.add_argument(
        "-s", "--min-file-size",
        help="Minimum size of file for splitting (in bytes)",
        type=int,
        default= 1024 * 1024 * 100 # 100 MB
    )

    parser.add_argument(
        "-M", "--max-record-size",
        help="Max allowed size for responses",
        type=int,
        default=MAX_ALLOWED_PAYLOAD_SIZE,
    )

    parser.add_argument(
        "--all-tlds",
        action="store_true",
        help="Do not filter by TLD",
    )

    parser.add_argument(
        "--max-records-per-file",
        default=MAX_RECORDS_PER_FILE,
    )

    parser.add_argument(
        "--status-code",
        nargs="+",
        type=size_range,
        help="Valid status codes (e.g. 200 404 500-599)"
    )

    return parser.parse_args()

def size_range(v):
    parts = v.split("-")

    if len(parts) == 1:
        min_size = int(parts[0])
        max_size = min_size
        
    elif len(parts) == 2:
        min_size = int(parts[0])
        max_size = int(parts[1])
    else:
        raise ValueError(
            "Too many parts in size {}, just specify min and max".format(v)
        )

    return (min_size, max_size)


def main():
    global MAX_ALLOWED_PAYLOAD_SIZE
    global MAX_RECORDS_PER_FILE
    global ALLOWED_TLDS
    global VALID_STATUS_CODES
    
    args = parse_args()
    init_log(1)

    min_file_size = args.min_file_size
    MAX_ALLOWED_PAYLOAD_SIZE = args.max_record_size
    if args.all_tlds:
        ALLOWED_TLDS = []
        
    MAX_RECORDS_PER_FILE = args.max_records_per_file

    args_status_codes = []
    if args.status_code:
        for min_size, max_size in args.status_code:
            args_status_codes.extend(list(range(min_size, max_size + 1)))

    if args_status_codes:
        VALID_STATUS_CODES = [str(s) for s in args_status_codes]

    process_pool = Pool()
    results = []
    for filepath in utils.read_text_targets(args.file, try_read_file=False):
        if os.path.getsize(filepath) < min_file_size:
            print(filepath, flush=True)
            continue
        result = process_pool.apply_async(split_file, (filepath,))
        results.append(result)

    for result in results:
        result.get()

def split_file(filepath):
    part_n = 0
    record_count = 0

    logger.info("Splitting {}".format(filepath))
    with open(filepath, "rb") as fi:
        try:
            part_file = "{}.{}part.gz".format(filepath, part_n) 
            fo = open(part_file, "wb")
            writer = WARCWriter(fo)
            for record in ArchiveIterator(fi):
                if not utils.is_valid_record(
                        record,
                        max_payload_size=MAX_ALLOWED_PAYLOAD_SIZE,
                        allowed_tlds=ALLOWED_TLDS,
                        valid_status_codes=VALID_STATUS_CODES,
                ):
                    continue
                try:
                    writer.write_record(record)
                except UnicodeEncodeError:
                    continue
                    
                record_count += 1
                if record_count == MAX_RECORDS_PER_FILE:
                    logger.info(
                        "Write {} records into {}".format(record_count, part_file)
                    )
                    print(part_file, flush=True)
                    record_count = 0
                    part_n += 1
                    fo.close()
                    part_file = "{}.{}part.gz".format(filepath, part_n) 
                    fo = open(part_file, "wb")
                    writer = WARCWriter(fo)
        finally:
            logger.info("Write {} records into {}".format(record_count, part_file))
            print(part_file, flush=True)
            fo.close()

def init_log(verbosity=0):

    if verbosity >= 1:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=logging.ERROR,
        format="{}:%(levelname)s:%(message)s".format(os.path.basename(__file__))
    )
    logger.level = level

            
if __name__ == "__main__":
    exit(main())
