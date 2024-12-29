#!/usr/bin/env python3
import argparse
import pandas as pd
import re
from functools import partial
import tfmutils as utils
import os

TITLE_REGEXES = [
    "coming soon",
    "under construction",
    "expired",
    "( |^)404( |$)",
    "( |^)503( |$)",
     "not found",
    "( |^)bot( |$)",
    "under.{0,10} maintenance",
    "not be found",
    "internal server error",
    "dynaxml error",
    "sorry, we didn't find any products",
    "sorry, but there are no tickets",
    "server error"
    "error404",
    "request rejected",
    "site is no longer active",
]

CONTENT_REGEXES = [
    "page not found",
    "page.{0,20} under construction",
    "site.{0,20} under construction",
    "(site|domain) is unavailable",
    "mysql server has gone away",
    "sorry, something went wrong",
    "database error",
    "connect error",
    "page( .{0,20}|) could not be found",
    "uncaught error",
    "^fatal error",
    "^warning\t:",
    "page( .{0,50}|) moved",
    "no [a-z]+ found[.]?$",
    "^error occured",
    "^error:",
    "^error code",
    "^error [|]",
    " try later",
    "try again later.$",
    "contact the site owner",
    "site is being upgraded",
    "^sorry!",
    "^sorry,",
    "page\shas\smoved",
    "file not found.{0,100}$",
    "under going scheduled maintenance",
    "domain has expired",
    "^redirecting",
    "domain is temporarily unavailable",
    "query did not match",
    "search yielded no",
    "internal service error"
]

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "dataset",
        help="Dataset in CSV format to label",
        nargs="*",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    for filepath in utils.read_text_targets(args.dataset, try_read_file=False):
        label_dataset(filepath)
        
def label_dataset(filepath):
    utils.eprint("{}: Labelling {}".format(
        os.path.basename(__file__), filepath
    ))
    
    df = pd.read_csv(filepath, na_filter=False)

    df["label"] = df.status_code.map(lambda c: "error" if c != 200 else "content" )

    any_title_regex_match = partial(any_regex_match, TITLE_REGEXES)
    df.loc[
        df["title"].apply(any_title_regex_match) & (df["label"] == "content"), "label"
    ] = "error"

    any_content_regex_match = partial(any_regex_match, CONTENT_REGEXES)
    df.loc[
        df["text"].apply(any_content_regex_match) & (df["label"] == "content"), "label"
    ] = "error"

    out_filepath = filepath + "-tag.csv"    
    df.to_csv(out_filepath, index=False)

    print(out_filepath, flush=True)

def any_regex_match(regexes, text):
    text = text.lower()
    for regex in regexes:
        if not regex:
            continue
        if re.search(regex, text):
            return True
    return False

if __name__ == "__main__":
    exit(main())
