import unicodedata
import sys


def _is_python3():
    return sys.version_info[0] > 2


def _is_unicode_compat(data):
    if _is_python3():
        return isinstance(data, str)
    else:
        return isinstance(data, unicode)


def _iter_items_compat(dict_data):
    if _is_python3():
        return {
            byteify(key, ignore_dicts=True): byteify(value, ignore_dicts=True)
            for key, value in dict_data.items()
        }
    else:
        return {
            byteify(key, ignore_dicts=True): byteify(value, ignore_dicts=True)
            for key, value in dict_data.iteritems()
        }


def _byteify_unicode_compat(data):
    return data if _is_python3() else unicodedata.normalize('NFKD', data).encode('ascii', 'ignore')


def byteify(data, ignore_dicts = False):
    # if this is a unicode string, return its string representation
    if _is_unicode_compat(data):
        return _byteify_unicode_compat(data)
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [ byteify(item, ignore_dicts=True) for item in data ]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return _iter_items_compat(data)
    # if it's anything else, return it in its original form
    return data