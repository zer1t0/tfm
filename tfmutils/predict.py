
import pickle
import nltk
from nltk.stem.wordnet import WordNetLemmatizer
from abc import ABC, abstractmethod
from .text_extraction import extract_text
import string
import re

class Classifier(ABC):

    def classify(self, content_type, body):
        text = extract_text(content_type, body)
        return self.classify(text)
    
    @abstractmethod
    def classify_text(self, content_type, body):
        pass

class ScikitClassifier(Classifier):

    def __init__(self, vectorizer, model):
        self.vectorizer = vectorizer
        self.model = model

    def classify_text(self, text):
        text_tokens = tokenize(text)
        clean_tokens = list(clean_term_sentence(text_tokens))
        X = self.vectorizer.transform([" ".join(clean_tokens)]).toarray()
        return self.model.predict(X)[0]

class LLMClassifier(Classifier):

    def __init__(self, model):
        self.model = model

    def classify_text(self, text):
        result = self.model(text, truncation=True)[0]
        return result["label"]

def load_classifier(name):
    if name == "distilbert":
        return load_llm_classifier()
    else:
        return load_scikit_classifier(name)

def load_llm_classifier():
    from transformers import pipeline
    import torch

    classifier = pipeline(
        "text-classification",
        model="models/distilbert-model"
    )
    return LLMClassifier(classifier)

def load_scikit_classifier(name):
    vectorizer = load_tfidf_vectorizer()
    model = load_model(name)
    return ScikitClassifier(vectorizer, model)

def load_tfidf_vectorizer():
    with open("models/tfidf.pickle", "rb") as fi:
        return pickle.load(fi)

def load_model(name):
    with open("models/{}.pickle".format(name), "rb") as fi:
        return pickle.load(fi)


def tokenize(text):
    return nltk.word_tokenize(text.lower())

def token_is_error_status_code(token):
    try:
        n = int(token)
        return 400 <= n <= 425 or 500 <= n <= 511
    except ValueError:
        return False

def token_is_punctuation(token):
    for c in token:
        if c not in string.punctuation:
            return False
    return True

def is_token_just_word(token):
    return re.match("^[a-z'_]+$", token)

def clean_term_sentence(ts):
    for token in ts:
        token = token.strip()
        token = token.replace("’", "'")
        if token and not token_is_punctuation(token) \
            and (is_token_just_word(token) or token_is_error_status_code(token)):
            yield WordNetLemmatizer().lemmatize(token)

            

def classify_response(resp):
    if not model:
        load_model_and_vectorizer()

    content_type = resp.headers.get('content-type', "/")
    body = resp.content

    text = extract_text(content_type, body)
    text_tokens = tokenize(text)
    clean_tokens = list(clean_term_sentence(text_tokens))
    X = vectorizer.transform([" ".join(clean_tokens)])
    return model.predict(X)[0]
