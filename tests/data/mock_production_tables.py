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
    (231135102, "Ben", "Carter", 1, True),
    (61400061, "Mia", "Turner", 2, True),
    (248680976, "Noah", "Phillips", 3, True),
    (338987992, "Freya", "Watson", 4, True),
    (111094643, "James", "Hughes", 5, True),
    (729873417, "Evie", "Cole", 1, False),
    (782765247, "Theo", "Morris", 2, False),
    (670250006, "Grace", "Reed", 3, True),
    (20511562, "Archie", "Foster", 4, False),
    (480925900, "Ella", "Chapman", 5, True),
    (823529880, "Liam", "Green", 4, True),
    (81749755, "Olivia", "Hall", 4, True),
    (540629010, "Jacob", "Wright", 4, False),
    (692782366, "Emily", "King", 4, True),
    (440231364, "Daniel", "Scott", 4, True),
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
    (4, "Sales"),
    (5, "IT"),
]


# Sales Table: Mock table of sensitive sales data
# ---

sales_schema = T.StructType(
    [
        T.StructField("sale_id", T.LongType(), False),
        T.StructField("sale_date", T.DateType(), False),
        T.StructField("employee_id", T.LongType(), False),
        T.StructField("sale_total_gbp", T.DecimalType(10, 2), False),
        T.StructField("delivery_distance_miles", T.DoubleType(), False),
        T.StructField("customer_feedback_score", T.FloatType(), True),
    ]
)


sales_rows = [
    (987620765, date(2025, 8, 23), 338987992, Decimal("372.64"), 91.36, None),
    (952790649, date(2025, 5, 7), 823529880, Decimal("991.27"), 24.51, 2.6),
    (906997872, date(2024, 10, 23), 81749755, Decimal("71.82"), 16.34, 2.1),
    (913259394, date(2025, 9, 4), 20511562, Decimal("821.16"), 88.71, None),
    (346680751, date(2025, 4, 3), 338987992, Decimal("266.72"), 37.99, None),
    (533488945, date(2025, 9, 9), 20511562, Decimal("782.54"), 9.48, 5.0),
    (516569467, date(2025, 7, 3), 440231364, Decimal("677.59"), 17.53, None),
    (603332132, date(2025, 9, 30), 81749755, Decimal("781.26"), 82.19, None),
    (851216903, date(2025, 4, 5), 338987992, Decimal("897.01"), 27.05, None),
    (688358908, date(2024, 12, 16), 440231364, Decimal("415.73"), 41.86, None),
    (994407999, date(2025, 3, 11), 81749755, Decimal("517.32"), 12.16, 1.9),
    (807904290, date(2024, 12, 4), 440231364, Decimal("672.83"), 90.98, None),
    (671708347, date(2025, 4, 5), 20511562, Decimal("651.80"), 97.83, 1.6),
    (867594066, date(2025, 9, 16), 540629010, Decimal("631.38"), 75.83, None),
    (366648514, date(2025, 4, 22), 823529880, Decimal("513.02"), 99.49, None),
    (509494898, date(2025, 1, 26), 823529880, Decimal("137.22"), 74.43, 2.6),
    (157542849, date(2025, 7, 4), 440231364, Decimal("922.64"), 13.36, 3.2),
    (243729774, date(2024, 10, 26), 338987992, Decimal("530.81"), 62.41, 4.9),
    (593314814, date(2025, 8, 14), 20511562, Decimal("160.86"), 85.50, 3.7),
    (138143141, date(2025, 1, 24), 20511562, Decimal("168.46"), 78.44, 3.4),
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
