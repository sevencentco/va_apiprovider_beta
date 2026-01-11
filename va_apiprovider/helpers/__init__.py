from types import SimpleNamespace
def upper_keys(d):
    """Returns a new dictionary with the keys of `d` converted to upper case
    and the values left unchanged.

    """
    return dict(zip((k.upper() for k in d.keys()), d.values()))

def to_namespace(object):
    if isinstance(object, dict):
        return SimpleNamespace(**{k: to_namespace(v) for k, v in object.items()})
    elif isinstance(object, list):
        return [to_namespace(v) for v in object]
    else:
        return object