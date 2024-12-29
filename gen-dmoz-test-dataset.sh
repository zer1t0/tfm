#!/bin/bash

# Extract all the DMOZ domains
./extract-dmoz-sites.py -v \
    | urld --pattern "{netloc}" \ 
    | sort -u > dmoz-domains.txt

# Request an unexistent path for those available sites
# WARNING: httpx tool is from https://github.com/projectdiscovery/httpx
#          it IS NOT the python tool from httpx package
cat dmoz-domains.txt \
    | httpx \
    | sed 's/$/\/e7aef43b-edde-490a-a95f-a31d978d2bee/' \
    | ./download-webpage.py -o dmoz-guid-urls.warc.gz

# This step is interactive. The script will show you the downloaded webpages
# and you need to indicate if those are an error or not.
./webpage-labeler.py dmoz-guid-urls.warc.gz labels.json

# By using the labels given to the webpages, generate the dataset
./label-from-manual-filter.py dmoz-guid-urls.warc.gz labels.json test-dataset.csv
