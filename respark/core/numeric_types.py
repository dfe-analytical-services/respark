import math
import sys

from pyspark.sql import types as T


###
# Dervive type-specific limits and bounds
###
INTEGRAL_BITS = {
    "byte": 8,
    "short": 16,
    "int": 32,
    "long": 64,
}

INTEGRAL_CAST = {
    "byte": T.ByteType(),
    "short": T.ShortType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
}

INTEGRAL_BOUNDS = {
    name: {
        "min_value": -(1 << (bits - 1)),
        "max_value": (1 << (bits - 1)) - 1,
    }
    for name, bits in INTEGRAL_BITS.items()
}

# IEEE-754 ranges
DOUBLE_MAX = sys.float_info.max
DOUBLE_MIN = -sys.float_info.max

FLOAT_MAX = math.ldexp(2.0 - 2.0**-23, 127)
FLOAT_MIN = -FLOAT_MAX

FRACTIONAL_BOUNDS = {
    "float": (FLOAT_MIN, FLOAT_MAX),
    "double": (DOUBLE_MIN, DOUBLE_MAX),
}

FRACTIONAL_CAST = {
    "float": T.FloatType(),
    "double": T.DoubleType(),
}
