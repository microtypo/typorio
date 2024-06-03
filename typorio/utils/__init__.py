import random
from typing import List


def shuffle_rows(rows: List[List[str]]):
    timestamps = [row[-1] for row in rows]
    random.shuffle(timestamps)
    random.shuffle(rows)
    for row in rows:
        row[-1] = timestamps.pop()
    return rows
