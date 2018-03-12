# -*- coding: utf-8 -*-
from diskcache import Cache

class CacheHandler(Cache):
    def __init__(self, *args, **kwargs):
        self._enabled = kwargs.get('enabled', True)

        if self._enabled:
            super(CacheHandler, self).__init__(*args, **kwargs)


    def set(self, *args, **kwargs):
        if self._enabled:
            return super(CacheHandler, self).set(*args, **kwargs)
        return True
    __setitem__ = set


    def __exit__(self, *args, **kwargs):
        if self._enabled:
            super(CacheHandler, self).__exit__(*args, **kwargs)


    def __contains__(self, key):
    	if self._enabled:
            return super(CacheHandler, self).__contains__(key)
        return False


    def __getitem__(self, key):
    	if self._enabled:
            return super(CacheHandler, self).__getitem__(key)
        raise KeyError(key)
