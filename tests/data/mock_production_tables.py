from datetime import date
from pyspark.sql import types as T, DataFrame, SparkSession
import pytest

# Employees Table: Mock table of sensitive data
# ---
employees_schema = T.StructType(
    [
        T.StructField("first_name", T.StringType(), False),
        T.StructField("last_name", T.StringType(), False),
        T.StructField("department_id", T.IntegerType(), False),
        T.StructField("start_date", T.DateType(), False),
        T.StructField("salary", T.IntegerType(), False),
    ]
)

employees_rows = [
    ("Ben", "Carter", 1, date(2020, 2, 1), 29500),
    ("Mia", "Turner", 2, date(2020, 2, 2), 41000),
    ("Noah", "Phillips", 3, date(2020, 2, 3), 46000),
    ("Freya", "Watson", 4, date(2020, 2, 4), 51000),
    ("James", "Hughes", 5, date(2020, 2, 5), 53000),
    ("Evie", "Cole", 1, date(2020, 2, 6), 32000),
    ("Theo", "Morris", 2, date(2020, 2, 7), 37000),
    ("Grace", "Reed", 3, date(2020, 2, 8), 44000),
    ("Archie", "Foster", 4, date(2020, 2, 9), 48000),
    ("Ella", "Chapman", 5, date(2020, 2, 10), 56000),
]

# Departments Table: Mock table of non-sensitive lookup data
# ---
departments_schema = T.StructType(
    [
        T.StructField("department_id", T.IntegerType(), False),
        T.StructField("department_name", T.StringType(), False),
    ]
)

departments_rows = [
    (1, "HR"),
    (2, "Finance"),
    (3, "Engineering"),
    (4, "Marketing"),
    (5, "IT"),
]


# Invalid Employees Table: Mock invalid table of sensitive data
# ---
invalid_employees_schema = T.StructType(
    [
        T.StructField("first_name", T.StringType(), True),
        T.StructField("last_name", T.StringType(), True),
        T.StructField("enrolled_courses", T.ArrayType(T.StringType()), False),
    ]
)

invalid_employees_rows = [
    ("Oliver", "Hughes", ["Office Induction", "Introduction to Data"]),
    ("Amelia", "Clark", ["Office Induction", "First Aider Training"]),
    ("Jack", "Turner", [""]),
]
