
from .html_text_extraction import extract_title_and_texts_from_html
from .json_text_extraction import extract_texts_from_json
from .xml_text_extraction import extract_texts_from_xml
from .filter_options import InvalidRecordError
import re
import json

def extract_text(content_type, body):
    return extract_title_and_text(content_type, body)[1]

def extract_title_and_text(content_type, body):
    title = ""
    if "html" in content_type:
        title, texts = extract_title_and_texts_from_html(body)
    elif "xml" in content_type:
        texts = extract_texts_from_xml(body)
    elif "json" in content_type:
        try:
            texts = extract_texts_from_json(body)
        except json.decoder.JSONDecodeError:
            raise InvalidRecordError("Unable to parse json")
    else:
        # By default we assume it is HTML
        title, texts = extract_title_and_texts_from_html(body)

    texts = [remove_extra_spaces(t) for t in texts]
    texts = [t for t in texts if t]
    if len(texts) == 0:
        return title, ""
    
    text = "\t".join(texts)
    return remove_extra_spaces(title), text

def remove_extra_spaces(text):
    return re.sub('\s+', ' ', text).strip()
