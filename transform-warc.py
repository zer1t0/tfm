#!/usr/bin/env python3

from warcio.archiveiterator import ArchiveIterator
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
import tfmutils as utils
import argparse
import os
import logging
import io
import re
import json
from collections import namedtuple
from urllib.parse import urlparse
import collections
from multiprocessing import Pool
import random, string
from pathlib import Path
import csv
import sys
csv.field_size_limit(sys.maxsize)

logger = logging.getLogger(__name__)

MAX_ALLOWED_PAYLOAD_SIZE = utils.DEFAULT_MAX_ALLOWED_PAYLOAD_SIZE
ALLOWED_TLDS = utils.DEFAULT_ALLOWED_TLDS
VALID_STATUS_CODES = utils.DEFAULT_VALID_STATUS_CODES

MapInput = namedtuple("MapInput", [
    "in_filepath",
    "out_dir"
])

ParsingResult = namedtuple("ParsingResult", [
    "filepath",
    "result_filepath",
    "content_types",
    "valid_records_count"
])

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "file",
        help="files to process. "
        "If none then stdin will be use",
        nargs="*",
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
        "--status-code",
        nargs="+",
        type=size_range,
        help="Valid status codes (e.g. 200 404 500-599)"
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        help="Verbosity",
        default=0
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
    global ALLOWED_TLDS
    global VALID_STATUS_CODES

    args = parse_args()
    init_log(args.verbose)

    MAX_ALLOWED_PAYLOAD_SIZE = args.max_record_size
    if args.all_tlds:
        ALLOWED_TLDS = []

    args_status_codes = []
    if args.status_code:
        for min_size, max_size in args.status_code:
            args_status_codes.extend(list(range(min_size, max_size + 1)))

    if args_status_codes:
        VALID_STATUS_CODES = [str(s) for s in args_status_codes]
    
    files_list = extract_warc_filepaths(args.file)

    # out_dir = args.out
    # VALID_STATUS_CODE = args.status

    # Path(out_dir).mkdir(parents=True, exist_ok=True)

    process_pool = Pool()
    results = []
    for filepath in utils.read_text_targets(args.file, try_read_file=False):
        result = process_pool.apply_async(parse_file, (filepath,))
        results.append(result)

    for result in results:
        result.get()

    # reduce_results(args.out, results, append=args.append)

def parse_file(filepath):

    result_filepath = filepath + ".csv"
    # "/tmp/filtered-{}.csv".format(
    #     os.path.basename(filepath)
    # )
    logger.info("Parsing {}".format(filepath))
    with open(result_filepath, "w") as fo:
        writer = csv.DictWriter(fo, fieldnames=utils.DATASET_FIELDS)
        writer.writeheader()
        with open(filepath, "rb") as fi:
            process_file_records(fi, writer, filepath, result_filepath)

    print(result_filepath, flush=True)

def process_file_records(fi, writer, filepath, result_filepath):
    valid_records_count = 0
    content_types = {}
    i = 0

    for record in ArchiveIterator(fi):
        i += 1
        if i % 2000 == 0:
            logger.info("{}: {} records parsed".format(filepath, i))
        try:
            record_info = utils.parse_record_header(
                record,
                max_payload_size=MAX_ALLOWED_PAYLOAD_SIZE,
                allowed_tlds=ALLOWED_TLDS,
                valid_status_codes=VALID_STATUS_CODES,
            )

            # to avoid modify the record contents
            # we replace the raw_stream with bytes stream
            # that we can seek and consume as many times
            # as we want
            # raw_body = record.raw_stream.read()
            # record.raw_stream = io.BytesIO(raw_body)

            title, text = parse_record_body(record, record_info)

            content_type = record_info.content_type
            try:
                content_types[content_type] += 1
            except KeyError:
                content_types[content_type] = 1

            # record.raw_stream.seek(0)
            # writer.write_record(record)

            label = "content" if record_info.status_code == "200" else "error"
            writer.writerow({
                utils.DATASET_FIELD_LABEL: label,
                utils.DATASET_FIELD_STATUS_CODE: record_info.status_code,
                utils.DATASET_FIELD_CONTENT_TYPE: record_info.content_type,
                utils.DATASET_FIELD_URL: record_info.url,
                utils.DATASET_FIELD_SIZE: record_info.size,
                utils.DATASET_FIELD_TITLE: title,
                utils.DATASET_FIELD_TEXT: text,
            })

            valid_records_count += 1

            # raise Exception("break")
        except utils.InvalidRecordError:
            pass

    logger.info("{} finished: {} records parsed".format(filepath, i))
    return ParsingResult(
        filepath=filepath,
        result_filepath=result_filepath,
        content_types=content_types,
        valid_records_count=valid_records_count
    )

# We can read the body here since it will be previously converted to a byte
# stream
def parse_record_body(record, record_info):
    content_type = record_info.content_type
    body = record.content_stream().read()

    title, text = utils.extract_title_and_text(content_type, body)
    if not text:
        raise utils.InvalidRecordError("No text in record")

    return title, text
    

def gen_random_file_id():
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(10))

def extract_warc_filepaths(filepaths):
    files_list = []
    for fp in filepaths:
        if os.path.isdir(fp):
            files_list.extend([
                os.path.join(fp, f)
                for f in os.listdir(fp)
                if f.endswith(".warc")
            ])
        else:
            files_list.append(fp)
    return sorted(files_list)

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


def reduce_results(output_file, results, append):
    content_types = {}
    valid_records_count = 0

    mode = "a" if append else "w"

    with open(output_file, mode) as fo:
        writer = csv.DictWriter(fo, fieldnames=OUTPUT_FIELDS)
        if not append:
            writer.writeheader()

        for result in results:
            pr = result.get()
            valid_records_count += pr.valid_records_count

            file_content_types = pr.content_types
            for k in file_content_types:
                try:
                    content_types[k] += file_content_types[k]
                except KeyError:
                    content_types[k] = file_content_types[k]

            with open(pr.result_filepath, "r") as fi:
                reader = csv.DictReader(fi)
                rows = [r for r in reader]
                writer.writerows(rows)
            # os.remove(pr.result_filepath)

    print(output_file, flush=True)
    # print("content types:", content_types)
    # print("Responses count:", valid_records_count)


if __name__ == "__main__":
    exit(main())
