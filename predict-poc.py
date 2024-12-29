#!/usr/bin/env python3
import argparse
import tfmutils as utils
from warcio.archiveiterator import ArchiveIterator
import string
import re
import pickle

def parse_args():
    parser = argparse.ArgumentParser(
        description="Predict if it is an error page using the trained models"
    )

    parser.add_argument(
        "file",
        help="It can be an html or warc.gz file",
        nargs="*"
    )

    parser.add_argument(
        "-m", "--model",
        choices=["nb", "rf", "svc", "logreg", "distilbert"],
        default="svc"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    classifier = utils.load_classifier(args.model)

    for html_file in utils.read_text_targets(args.file, try_read_file=False):
        if html_file.endswith(".warc.gz"):
            with open(html_file, "rb") as fi:
                for record in ArchiveIterator(fi):
                    if record.rec_type != "response":
                        continue
                    if record.http_headers is None:
                        continue
                    
                    content_type = utils.get_record_content_type(record)
                    body = record.content_stream().read()
                    
                    text = utils.extract_text(content_type, body) 
                    if not text:
                        continue

                    status_code = utils.get_record_status_code(record)
                    url = utils.get_record_url(record)

                    label = classifier.classify_text(text)
                    print(label, status_code, url)
        else:
            with open(html_file) as fi:
                html_str = fi.read()

            text = utils.extract_text("html", html_str)
            if not text:
                continue
            label = classifier.classify_text(text)
            print(label, html_file)

if __name__ == "__main__":
    exit(main())
