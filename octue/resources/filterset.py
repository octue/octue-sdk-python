class FilterSet:
    def __init__(self, iterable=None):
        self._set = set(iterable or [])

    def __iter__(self):
        yield from self._set

    def __len__(self):
        return len(self._set)

    def __contains__(self, item):
        return item in self._set

    def add(self, item):
        self._set.add(item)
