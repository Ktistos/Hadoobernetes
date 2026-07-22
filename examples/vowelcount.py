from typing import Iterable, Tuple, List

def map(key: str, value: str) -> Iterable[Tuple[str, int]]:
    """
    Emits (vowel, 1) for every vowel in the text.
    """
    for char in value.lower():
        if char in 'aeiou':
            yield (char, 1)

def reduce(key: str, values: List[str]) -> Iterable[Tuple[str, int]]:
    """
    Sums counts for each vowel.
    """
    total = sum(int(v) for v in values)
    yield (key, total)
