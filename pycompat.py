class defaultdict(dict):

    def __init__(self, default_factory):
        if not callable(default_factory):
            raise TypeError('first argument must be callable')
        self.default_factory = default_factory

    def __getitem__(self, key):
        if key in self:
            return self.get(key)
        return self.setdefault(key, self.default_factory())


def partial(func, *args, **keywords):
    def newfunc(*fargs, **fkeywords):
        newkeywords = keywords.copy()
        newkeywords.update(fkeywords)
        return func(*(args + fargs), **newkeywords)
    newfunc.func = func
    newfunc.args = args
    newfunc.keywords = keywords
    return newfunc


def any(iterable):
    for element in iterable:
        if element:
            return True
    return False


def all(iterable):
    for element in iterable:
        if not element:
            return False
    return True
