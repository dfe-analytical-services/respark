from datetime import date
from decimal import Decimal
from pyspark.sql import types as T

# Employees Table: Mock table of sensitive data
# ---
employees_schema = T.StructType(
    [
        T.StructField("employee_id", T.LongType(), False),
        T.StructField("first_name", T.StringType(), False),
        T.StructField("last_name", T.StringType(), False),
        T.StructField("department_id", T.IntegerType(), False),
        T.StructField("is_current", T.BooleanType(), False),
    ]
)

employees_rows = [
    (199756800, "Ben", "Carter", 1, True),
    (327628800, "Mia", "Turner", 2, True),
    (175132800, "Noah", "Phillips", 3, True),
    (
        188870400,
        "Freya",
        "Watson",
        4,
        True,
    ),
    (418176000, "James", "Hughes", 5, True),
    (103680000, "Evie", "Cole", 1, False),
    (556531200, "Theo", "Morris", 2, False),
    (505958400, "Grace", "Reed", 3, True),
    (852076800, "Archie", "Foster", 4, False),
    (1410912000, "Ella", "Chapman", 5, True),
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


# Sales Table: Mock table of sensitive sales data
# ---
sales_schema = T.StructType(
    [
        T.StructField("sale_id", T.LongType(), False),
        T.StructField("sale_date", T.DateType(), False),
        T.StructField("sale_total_gbp", T.DecimalType(10, 2), False),
        T.StructField("delivery_distance_miles", T.DoubleType(), False),
        T.StructField("customer_feedback_score", T.FloatType(), True),
    ]
)

sales_rows = [
    (10019823, date(2025, 1, 14), Decimal("812.45"), 47.23, 4.2),
    (10028765, date(2025, 2, 27), Decimal("129.99"), 33.87, None),
    (10037642, date(2025, 3, 19), Decimal("654.32"), 58.12, 3.8),
    (10046589, date(2025, 4, 5), Decimal("298.75"), 21.45, 5.0),
    (10055431, date(2025, 5, 23), Decimal("987.65"), 76.34, 2.9),
    (10064378, date(2025, 6, 30), Decimal("432.10"), 12.56, 3.5),
    (10073214, date(2025, 7, 15), Decimal("210.00"), 45.67, None),
    (10082167, date(2025, 8, 9), Decimal("765.43"), 89.01, 4.0),
    (10091025, date(2025, 9, 27), Decimal("543.21"), 38.29, 2.3),
    (10100012, date(2025, 10, 18), Decimal("321.89"), 64.78, 3.6),
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
