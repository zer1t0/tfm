#!/usr/bin/env python3

import argparse
from warcio import ArchiveIterator
import json
from urllib.parse import urlparse
import tfmutils as utils
import csv

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "warc"
    )

    parser.add_argument(
        "json_labels"
    )

    parser.add_argument(
        "out"
    )

    return parser.parse_args()

def main():
    args = parse_args()

    with open(args.json_labels) as fi:
        labels = json.load(fi)

    remove_invalid_labelled_urls(labels)

    with open(args.out, "w") as fo:
        writer = csv.DictWriter(fo, fieldnames=utils.DATASET_FIELDS)
        writer.writeheader()
        with open(args.warc, "rb") as fi:
            for record in ArchiveIterator(fi):
                if utils.get_record_type(record) != "response":
                    continue
            
                record_url = utils.get_record_url(record)
                try:
                    label = labels[record_url]
                except KeyError:
                    continue

                size = utils.get_record_payload_len(record)
                status_code = utils.get_record_status_code(record)
                content_type = utils.get_record_content_type(record)
                body = record.content_stream().read()

                title, text = utils.extract_title_and_text(content_type, body)

                writer.writerow({
                    utils.DATASET_FIELD_LABEL: label,
                    utils.DATASET_FIELD_STATUS_CODE: status_code,
                    utils.DATASET_FIELD_CONTENT_TYPE: content_type,
                    utils.DATASET_FIELD_URL: record_url,
                    utils.DATASET_FIELD_SIZE: size,
                    utils.DATASET_FIELD_TITLE: title,
                    utils.DATASET_FIELD_TEXT: text,
                })


def remove_invalid_labelled_urls(labels):
    invalid_urls = []
    for url_s, label in labels.items():
        if label == "invalid":
            invalid_urls.append(url_s)
            continue

        url = urlparse(url_s)
        if url.path != "/e7aef43b-edde-490a-a95f-a31d978d2bee":
            invalid_urls.append(url_s)
            

    for url in invalid_urls:
        del labels[url]
    
if __name__ == "__main__":
    exit(main())
