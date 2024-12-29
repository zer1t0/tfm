#!/usr/bin/env python3

from warcio.capture_http import capture_http
from warcio import WARCWriter
from warcio.archiveiterator import ArchiveIterator
import requests  # requests *must* be imported after capture_http
import argparse
from multiprocessing import Pool
import tfmutils as utils
import os
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download given URLs and store the response into a"
        " WARC file",
    )

    parser.add_argument(
        "url",
        help="URL to inspect. If none stdin will be used.",
        nargs="*"
    )

    parser.add_argument(
        "-o", "--out",
        help="WARC file to write the responses",
        required=True,
    )

    parser.add_argument(
        "-w", "--workers",
        help="Concurrent downloads",
        default=10,
        type=int,
    )

    return parser.parse_args()

def main():
    args = parse_args()

    out_file = args.out

    process_pool = Pool(args.workers)
    results = []
    for url in utils.read_text_targets(args.url):
        result = process_pool.apply_async(
            download_url_into_warc, (url,)
        )
        results.append(result)

    join_warcs(out_file, results)

def join_warcs(out_file, results):
    with open(out_file, "wb") as fo:
        writer = WARCWriter(fo)
        for result in results:
            tmp_warc = result.get()
            with open(tmp_warc, "rb") as fi:
                for record in ArchiveIterator(fi):
                    writer.write_record(record)
            os.remove(tmp_warc)
        

def download_url_into_warc(url):
    out_file = "/tmp/{}.warc.gz".format(url.replace("/", "_"))
    
    with open(out_file, 'wb') as fh:
        warc_writer = WARCWriter(fh)
        with capture_http(warc_writer):
            utils.eprint(url)
            try:
                requests.get(url, verify=False, timeout=10)
            except (requests.exceptions.RequestException,
                    UnicodeEncodeError,
                    urllib3.exceptions.LocationParseError):
                pass

    return out_file


if __name__ == "__main__":
    exit(main())
