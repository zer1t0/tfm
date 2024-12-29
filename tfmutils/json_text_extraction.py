import json
import collections

### ========= JSON text extraction ===========

def extract_texts_from_json(json_str):
    obj = json.loads(json_str, object_pairs_hook=collections.OrderedDict)
    return _texts_from_json_obj(obj)

def _texts_from_json_obj(obj):
    if isinstance(obj, dict):
        return _texts_from_json_dict(obj)
    elif isinstance(obj, list):
        return _texts_from_json_list(obj)
    elif isinstance(obj, str):
        return [obj]
    else:
        return [str(obj)]

def _texts_from_json_dict(d):
    msg = []
    for k, v in d.items():
        msg.append(k)
        msg.extend(_texts_from_json_obj(v))
    return msg

def _texts_from_json_list(l):
    msg = []
    for o in l:
        msg.extend(_texts_from_json_obj(o))
    
    return msg
