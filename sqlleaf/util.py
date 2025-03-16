import typing as t


def unique(sequence: t.List):
    """
    Return a list of unique elements in a list while preserving insertion order.
    """
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]


def flatten(lst: t.List):
    """
    Flatten a potentially nested list into a single list.
    For example,
        [a, 1, [b, c]]
    returns
        [a, 1, b, c]
    """
    new_list = []
    for l in lst:
        if isinstance(l, list):
            for ll in l:
                new_list.append(ll)
        else:
            new_list.append(l)
    return new_list


def type_name(typ) -> str:
    """
    Return the name of a type's class.

    Example:
        type_name(sqlglot.class.Expression) -> 'expression'
    """
    return type(typ).__name__.lower()


def chunks(lst, n):
    """
    Yield successive n-sized chunks from lst.
    """
    return [lst[i:i + n] for i in range(0, len(lst), n)]
