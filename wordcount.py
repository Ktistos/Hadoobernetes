import re
from typing import Iterable, Tuple, List

def map(key: str, value: str) -> Iterable[Tuple[str, int]]:
    """
    Tokenizes a line of text into words and emits a count of 1 for each word.
    
    Args:
        key (str): The starting byte offset of the line.
        value (str): The raw text of the line.
        
    Yields:
        Tuple[str, int]: A key-value pair of (word, 1).
    """
    words = re.findall(r'[a-zA-Z0-9]+', value.lower())
    for word in words:
        yield (word, 1)

def reduce(key: str, values: List[str]) -> Iterable[Tuple[str, int]]:
    """
    Aggregates the counts for a specific word.
    
    Args:
        key (str): The grouped word.
        values (List[str]): A list of stringified integers emitted by the mappers.
        
    Yields:
        Tuple[str, int]: The word and its total count.
    """
    total = sum(int(v) for v in values)
    yield (key, total)