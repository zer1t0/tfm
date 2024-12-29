
from . import warc_utils

DEFAULT_ALLOWED_TLDS = [
    "com", "org", "net", "edu",
    'nz', 'us', 'za', 'gov', 'in', 'au', 'uk',
    'ke', 'link', 'top', 'online', 'cc', 'id',
    'app',
]

DEFAULT_VALID_STATUS_CODES = ["200", "404"]

DEFAULT_MAX_ALLOWED_PAYLOAD_SIZE = 1024 * 100
DEFAULT_MIN_ALLOWED_PAYLOAD_SIZE = 1

class RecordInfo:

    def __init__(self, status_code, content_type, url, size, language="Unknown"):
        self.status_code = status_code
        self.content_type = content_type
        self.url = url
        self.size = size
        self.language = language

class InvalidRecordError(Exception):
    pass

def is_valid_record(
        record,
        max_payload_size=DEFAULT_MAX_ALLOWED_PAYLOAD_SIZE,
        allowed_tlds=DEFAULT_ALLOWED_TLDS,
        valid_status_codes=DEFAULT_VALID_STATUS_CODES,
):
    try:
        parse_record_header(
            record,
            max_payload_size=max_payload_size,
            allowed_tlds=allowed_tlds,
            valid_status_codes=valid_status_codes,
        )
        return True
    except InvalidRecordError:
        return False

# This function is to read information in record header and discard records in
# a quick way since no body is read here. Remember to not read raw_stream or
# content_stream, since the record will be altered
def parse_record_header(
        record,
        max_payload_size=DEFAULT_MAX_ALLOWED_PAYLOAD_SIZE,
        allowed_tlds=DEFAULT_ALLOWED_TLDS,
        valid_status_codes=DEFAULT_VALID_STATUS_CODES,
):
    if record.rec_type != "response":
        raise InvalidRecordError("Is not a response record")

    if record.payload_length == 0:
        raise InvalidRecordError("Empty response")

    if max_payload_size > 0 and record.payload_length > max_payload_size:
        raise InvalidRecordError("Too big")

    status_code = warc_utils.get_record_status_code(record)
    if status_code not in valid_status_codes:
        raise InvalidRecordError("Is not a {} response".format(
            " or ".join(valid_status_codes)
        ))

    content_type = warc_utils.get_record_content_type(record)
    if not warc_utils.is_text_content_type(content_type):
        raise InvalidRecordError("Binary data")

    if allowed_tlds and warc_utils.get_record_tld(record) not in allowed_tlds:
        raise InvalidRecordError("Not an allowed domain")

    record_info = RecordInfo(
        status_code=status_code,
        content_type=content_type,
        url=warc_utils.get_record_url(record),
        size=record.payload_length
    )
    return record_info
