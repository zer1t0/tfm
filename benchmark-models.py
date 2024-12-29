#!/usr/bin/env python3
import argparse
# import timeit
import time
import pickle
# from transformers import pipeline
# import torch
import tfmutils as utils
import pandas as pd

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "dataset",
        help="Dataset to get texts",
    )

    parser.add_argument(
        "-c", "--count",
        default=0,
        help="Number of items to process",
        type=int,
    )

    return parser.parse_args()


def main():
    args = parse_args()

    df = pd.read_csv(args.dataset)

    texts_count = args.count
    if texts_count:
        texts = df.sample(texts_count).text.tolist()
    else:
        texts = df.text.tolist()
    

    print("Testing each model for {} texts".format(len(texts)))
    for model_name in ["nb", "logreg", "svc", "rf", "distilbert"]:
        print("===== Model {} ======".format(model_name))
        model_time = benchmark_model(model_name, texts)
    
        time_per_classification = model_time / len(texts)
        print("{} seconds total".format(model_time))
        print("{} seconds per text".format(time_per_classification))
        print("===========")
    

    # print("Hey")
    # start = time.perf_counter()

    # for i in range(10):
    #     vectorizer, model = load_model_environment('rf')

    # end = time.perf_counter()

    # print("{}".format(end - start))

    # result = timeit.timeit(
    #     "load_model_environment('rf')",
    #     setup="from __main__ import load_model_environment",
    #     number=11
    # )

    # print("It took {} seconds to load the model".format(result))

def benchmark_model(name, texts):
    classifier = utils.load_classifier(name)

    start = time.perf_counter()
    for text in texts:
        classifier.classify_text(text)

    end = time.perf_counter()

    total_time = end - start

    return total_time

    
if __name__ == "__main__":
    exit(main())
