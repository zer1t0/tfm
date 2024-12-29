#!/bin/bash

usage () {
    >&2 echo "Usage: $0 [small | medium | large | all-sizes | all-languages]"
    exit -1
}

if [[ $# -ne 1 ]]; then
    usage
fi

if [[ "$1" == "all-sizes" ]]; then

    # This generates the csv file used in sizes-correlation notebook
    outfile="$1.csv"
    ./fetch-cc-warcs.py  --count-200 1 --count-n200 10 --no-progress \
        | ./split-warc.py --max-record-size 0 \
        | ./transform-warc.py --max-record-size 0 \
        | ./keep-only-english.py \
        | ./join-csvs.py -o $outfile

elif [[ "$1" == "all-languages" ]]; then

    # This generates the csv file used in domains-language notebook
    outfile="$1.csv"
    ./fetch-cc-warcs.py --count-200 4 --count-n200 40 --no-progress \
        | ./split-warc.py --all-tlds \
        | ./transform-warc.py --all-tlds \
        | ./join-csvs.py -o $outfile

elif [[ "$1" == "small" ]]; then

    outfile="dataset-$1.csv"
    ./fetch-cc-warcs.py  --count-200 4 --count-n200 40 --no-progress \
        | ./split-warc.py --status-code 200 400-599 \
        | ./transform-warc.py --status-code 200 400-599 \
        | ./keep-only-english.py \
        | ./label-dataset.py \
        | ./join-csvs.py -o $outfile

elif [[ "$1" == "medium" ]]; then

    outfile="dataset-$1.csv"
    ./fetch-cc-warcs.py  --count-200 6 --count-n200 60 --no-progress \
        | ./split-warc.py --status-code 200 400-599 \
        | ./transform-warc.py --status-code 200 400-599 \
        | ./keep-only-english.py \
        | ./label-dataset.py \
        | ./join-csvs.py -o $outfile

elif [[ "$1" == "large" ]]; then

    outfile="dataset-$1.csv"
    ./fetch-cc-warcs.py  --count-200 8 --count-n200 80 --no-progress \
        | ./split-warc.py --status-code 200 400-599 \
        | ./transform-warc.py --status-code 200 400-599 \
        | ./keep-only-english.py \
        | ./label-dataset.py \
        | ./join-csvs.py -o $outfile
else
    usage
fi

echo "Generated $outfile"

