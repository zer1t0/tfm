#!/usr/bin/env python3
import argparse
import requests
import sys
from pathlib import Path
import os
from tqdm import tqdm
import gzip
import shutil
import tfmutils as utils

URL_NON_200_RESP = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-42/non200responses.paths.gz"

URL_200_RESP = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-42/warc.paths.gz"

SHOW_PROGRESS = False

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-s", "--start-200",
        help="Start index for 200 responses file",
        default=1,
        type=int
    )

    parser.add_argument(
        "-c", "--count-200",
        help="Count of files to download for 200 responses",
        default=10,
        type=int,
    )

    parser.add_argument(
        "-S", "--start-n200",
        help="Start index for non 200 responses file",
        default=1,
        type=int
    )

    parser.add_argument(
        "-C", "--count-n200",
        help="Count of files to download for non 200 responses",
        default=10,
        type=int,
    )

    parser.add_argument(
        "-d", "--dir",
        help="Directory for files",
        default="common-crawl"
    )

    parser.add_argument(
        "-n", "--no-progress",
        help="Do not show progress",
        action="store_true",
    )

    return parser.parse_args()


def main():
    global SHOW_PROGRESS
    args = parse_args()
    SHOW_PROGRESS = not args.no_progress
    out_dir = args.dir
    start_200 = args.start_200
    count_200 = args.count_200
    start_non_200 = args.start_n200
    count_non_200 = args.count_n200

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    responses_200_index_file = os.path.join(out_dir, "warc.paths.gz")
    responses_non_200_index_file = os.path.join(out_dir, "non200responses.paths.gz")

    if count_non_200:
        indexes_non_200 = read_index_paths(
            responses_non_200_index_file,
            start=start_non_200,
            count=count_non_200,
            url=URL_NON_200_RESP,
            out_dir=out_dir
        )
        out_dir_non_200 = os.path.join(out_dir, "non200responses")
        Path(out_dir_non_200).mkdir(parents=True, exist_ok=True)
        download_warc_files(indexes_non_200, out_dir_non_200)


    if count_200:
        indexes_200 = read_index_paths(
            responses_200_index_file,
            start=start_200,
            count=count_200,
            url=URL_200_RESP,
            out_dir=out_dir
        )
        out_dir_200 = os.path.join(out_dir, "200responses")
        Path(out_dir_200).mkdir(parents=True, exist_ok=True)
        download_warc_files(indexes_200, out_dir_200)


def download_warc_files(paths, out_dir):
    for path in paths:
        url = "https://data.commoncrawl.org/{}".format(path)
        local_filepath = gen_path_for_download(out_dir, path)

        if not was_file_completely_downloaded(local_filepath, url):
            utils.eprint("Downloading {}".format(url))
            local_filepath = download_file(url, local_filepath)

        print(local_filepath, flush=True)


def read_index_paths(filepath, start, count, url, out_dir):
    if was_file_completely_downloaded(filepath, url):
        return read_lines(filepath, start, count)

    download_file(url, filepath)
    return read_lines(filepath, start, count)

def was_file_completely_downloaded(filepath, url):
    # Another option is to use the Etag header
    try:
        remote_size = req_file_size(url)
        local_size = os.path.getsize(filepath)
        return remote_size == local_size
    except FileNotFoundError:
        return False

def read_lines(filepath, start, count):
    with gzip.open(filepath, "r") as fi:
        return [
            line.strip().decode()
            for line in fi
        ][start-1:start+count-1]

def req_file_size(url):
    resp = requests.head(url)
    return int(resp.headers["content-length"])


def download_file(url, local_filepath):
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        # Sizes in bytes.
        total_size = int(r.headers.get("content-length", 0))
        block_size = 1024

        r.raise_for_status()

        if SHOW_PROGRESS:
            with tqdm(total=total_size, unit="B", unit_scale=True) as progress_bar:
                with open(local_filepath, "wb") as fo:
                    for data in r.iter_content(block_size):
                        progress_bar.update(len(data))
                        fo.write(data)
        else:
            with open(local_filepath, "wb") as fo:
                for data in r.iter_content(block_size):
                    fo.write(data)

    return local_filepath

def gen_path_for_download(out_dir, url_path):
    return os.path.join(out_dir, url_path.split('/')[-1])


if __name__ == "__main__":
    exit(main())
