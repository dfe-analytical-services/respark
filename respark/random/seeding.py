from typing import Any
import hashlib


def vary_seed(base_seed: int, *tokens: Any) -> int:
    """
    Derive a deterministic 63-bit non-negative seed from a base seed and arbitrary tokens.

    Args:
        base_seed: The run-level seed to vary.
        *tokens:   Any mix of values (ints, strs, etc.) used to decorrelate the seed.
                   Each token is stringified and included in the hash payload.

    Returns:
        int: A deterministic, non-negative 63-bit integer suitable for seeding RNGs.
    """

    payload = "|".join([str(base_seed), *map(str, tokens)]).encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    val64 = int.from_bytes(digest[:8], byteorder="big", signed=False)
    mixed = val64 ^ (base_seed & 0x7FFFFFFFFFFFFFFF)

    return mixed & 0x7FFFFFFFFFFFFFFF
