#!/usr/bin/env python3

import argparse
import pandas as pd
import langdetect
import os
from multiprocessing import Pool
import tfmutils as utils

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "file",
        help="CSV files to process. "
        "If none then stdin will be use",
        nargs="*",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    process_pool = Pool()
    results = []
    for filepath in utils.read_text_targets(args.file, try_read_file=False):
        result = process_pool.apply_async(parse_file, (filepath,))
        results.append(result)

    for result in results:
        result.get()


def parse_file(filepath):
    utils.eprint("{}: Getting english from {}".format(
        os.path.basename(__file__), filepath
    ))
    out_filepath = get_out_filepath(filepath)

    df = pd.read_csv(filepath)

    languages = []
    for index, row in df.iterrows():
        try:
            language = detect_text_language(row["text"])
            languages.append(language)
        except TypeError:
            print(index, row["text"])

    df["language"] = languages
    # df["language"] = df["text"].map(lambda t: detect_text_language(t))
    df = df.query("language == 'en'").drop('language', axis=1)

    df.to_csv(out_filepath, index=False)
    
    print(out_filepath, flush=True)

def get_out_filepath(filepath):
    parts = os.path.basename(filepath).split(".")
    if len(parts) == 1:
        out_filename = parts[0] + "-en"
    else:
        out_filename = ".".join(parts[:-1]) + "-en." + parts[-1]

    return os.path.join(os.path.dirname(filepath), out_filename)

def detect_text_language(text):
    try:
        return langdetect.detect(text)
    except langdetect.lang_detect_exception.LangDetectException:
        return "Unknown"


# def get_text_language_based_on_stopwords(text):
#     words = [word.lower() for word in nltk.wordpunct_tokenize(text)]

#     languages_ratios = {}

#     for language in stopwords.fileids():
#         stopwords_set = set(stopwords.words(language))
#         words_set = set(words)
#         common_elements = words_set.intersection(stopwords_set)
#         languages_ratios[language] = len(common_elements)

#     most_rated_language = max(languages_ratios, key=languages_ratios.get)
#     return most_rated_language


if __name__ == "__main__":
    exit(main())
