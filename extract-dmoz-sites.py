#!/usr/bin/env python3
import requests
import logging
import bs4
import urllib.parse
import argparse

logger = logging.getLogger(__name__)

SITE_URL = "https://www.dmoz.co.uk/"

def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract all site urls from DMOZ"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="count",
        help="Verbosity",
        default=0
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    init_log(args.verbose)

    resp = requests.get(SITE_URL)
    section_paths = list(extract_sections_urls(resp.text))

    for path in section_paths:
        url = SITE_URL + path
        explore_category(url)
        
    
def explore_category(cat_url):
    logger.info(cat_url)
    resp = requests.get(cat_url)
    for site in extract_sites_urls(resp.text):
        print(site)

    subcat_urls = list(extract_subcategories_urls(resp.text))
    for subcat_url in subcat_urls:
        if subcat_url == "/":
            continue
        url = urllib.parse.urljoin(cat_url, subcat_url)
        explore_category(url)


def extract_sites_urls(html_str):
    soup = bs4.BeautifulSoup(html_str, "html.parser")

    for divt in soup.find_all(class_="site-title"):
        yield divt.parent["href"]


def extract_subcategories_urls(html_str):
    soup = bs4.BeautifulSoup(html_str, "html.parser")

    subcat_soup = soup.find("div", {"id": "subcategories-div"})
    if subcat_soup is None:
        return
    
    for isoup in subcat_soup.find_all(class_="fa-folder-o"):
        atag = isoup.parent.parent
        yield atag["href"]

def extract_sections_urls(html_str):
    soup = bs4.BeautifulSoup(html_str, "html.parser")

    for tc in soup.find_all(class_="top-cat"):
        atag = tc.find("a")
        yield atag["href"]

def init_log(verbosity=0):

    if verbosity == 1:
        level = logging.INFO
    elif verbosity > 1:
        level = logging.DEBUG
    else:
        level = logging.WARN

    logging.basicConfig(
        level=logging.ERROR,
        format="%(levelname)s:%(message)s"
    )
    logger.level = level


if __name__ == "__main__":
    exit(main())
