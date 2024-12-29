
import csv
import sys

# Some texts are longer than the default field size limit
# so we need to adjust it
csv.field_size_limit(sys.maxsize)

## The HTTP response status code
DATASET_FIELD_STATUS_CODE = "status_code"

# The content type of the document from which the text was extracted
DATASET_FIELD_CONTENT_TYPE = "content_type"

# The URL related to the HTTP response
DATASET_FIELD_URL = "url"

# The size of the HTTP response body
DATASET_FIELD_SIZE = "size"

# The title for HTML pages
DATASET_FIELD_TITLE = "title"

# The extracted text of each document
DATASET_FIELD_TEXT = "text"

# The label to predict
DATASET_FIELD_LABEL = "label"

DATASET_FIELDS = [
    DATASET_FIELD_LABEL,
    DATASET_FIELD_STATUS_CODE,
    DATASET_FIELD_CONTENT_TYPE,
    DATASET_FIELD_URL,
    DATASET_FIELD_SIZE,
    DATASET_FIELD_TITLE,
    DATASET_FIELD_TEXT,
]
