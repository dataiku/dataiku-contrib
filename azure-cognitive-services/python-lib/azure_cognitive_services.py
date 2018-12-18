import itertools

#==============================================================================
# HELPER FUNCTIONS
#==============================================================================

# Utility to iterate over the Dataset records by chunk
def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk
