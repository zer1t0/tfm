#!/usr/bin/env python3
import argparse
from selenium import webdriver
import os
import json
import selenium
from selenium.common.exceptions import WebDriverException
from warcio.archiveiterator import ArchiveIterator
import tfmutils as utils
import urllib3

def parse_args():
    parser = argparse.ArgumentParser(
        description="Tag pages manually based on visualization, source code"
        " and extracted text",
    )

    parser.add_argument(
        "warc",
        help="WARC (warc.gz) file to read pages",
    )

    parser.add_argument(
        "out",
        help="JSON file to output the results",
    )

    return parser.parse_args()

def main():
    args = parse_args()

    filepath = args.warc

    try:
        with open(args.out) as fi:
            results = json.load(fi)
    except FileNotFoundError:
        results = {}

    options = webdriver.firefox.options.Options()
    options.set_preference("javascript.enabled", False)

    driver = webdriver.Firefox(options)
    driver.set_page_load_timeout(10)

    try:
        results = classify_pages(filepath, driver, results)
    finally:
        with open(args.out, "w") as fo:
            json.dump(results, fo)

        driver.close()
    
def classify_pages(filepath, driver, results):
    count = 0
    with open(filepath, "rb") as fi:
        for record in ArchiveIterator(fi):
            if not is_url_guid_record(record):
                continue

            url = utils.get_record_url(record)
            if url in results:
                continue

            content_type = utils.get_record_content_type(record)
            body = record.content_stream().read()
            if not body:
                continue

            text = utils.extract_text(content_type, body)
            if not text:
                continue

            print("Content-type: {}".format(content_type))
            print("URL: {}".format(url))
            print("Status code: {}".format(utils.get_record_status_code(record)))
            
            temp_filepath = "/tmp/a.html"
            with open(temp_filepath, "wb") as fo:
                fo.write(body)

            print("==========")
            print(body)
            print("==========")
            print(text)
            print("==========")

            try:
                driver.get("file://" + temp_filepath)
            except selenium.common.exceptions.TimeoutException:
                pass

            count += 1
            print("Number {}".format(count))
            status = ask_for_classification()
            results[url] = status

    return results
    
def ask_for_classification():
    options = ["error", "content", "invalid"]
    while True:
        answer = input("What kind of file is? {}".format(options))
        if answer not in options:
            print("Specify {}".format(" or ".join(options)))
            continue
        break
    return answer
    
def is_url_guid_record(record):
    if record.rec_type != "response":
        return False
    
    try:
        status_code = utils.get_record_status_code(record)
    except AttributeError:
        return False

    if status_code not in ["200"]:
        return False

    url_s = utils.get_record_url(record)
    url = urlparse(url_s)
    if url.path != "/e7aef43b-edde-490a-a95f-a31d978d2bee":
        return False

    return True
                    
        
if __name__ == "__main__":
    main()
