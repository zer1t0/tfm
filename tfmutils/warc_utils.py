from urllib.parse import urlparse

## ========= Work with WARC records ==========

def get_record_type(record):
    return record.rec_type

def get_record_payload_len(record):
    return record.payload_length

def get_record_content_type(record):
    content_type = record.http_headers["content-type"] or "/"
    try:
        content_type = content_type.split(";")[0].strip() or "/"
        # content_type = content_type.split("/")[1]
    except:
        print(content_type)
        raise

    return content_type

def get_record_status_code(record):
    return record.http_headers.statusline.split()[0]

def get_record_url(record):
    return record.rec_headers.get_header('WARC-Target-URI')

def get_record_domain(record):
    url = record.rec_headers.get_header('WARC-Target-URI')
    return urlparse(url).hostname

def get_record_tld(record):
    return get_record_domain(record).split(".")[-1]

def is_text_content_type(content_type):
    return "html" in content_type or \
       "plain" in content_type or \
       "xml" in content_type or \
       "json" in content_type or \
       "calendar" in content_type

def is_text_record(record):
    return is_text_content_type(get_record_content_type(record))
