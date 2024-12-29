
import re
import sys

from .readin import read_text_targets

from .filter_options import *

from .text_extraction import extract_text, extract_title_and_text, \
    remove_extra_spaces
from .html_text_extraction import extract_title_and_texts_from_html

from .warc_utils import *
from .dataset import *
from .predict import *

def clean_text(text):
    regex = re.compile("[^a-zA-Z0-9_.,' ]")
    text = regex.sub(' ', text)
    text = re.sub('\s+', ' ', text).strip()
    return text


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
