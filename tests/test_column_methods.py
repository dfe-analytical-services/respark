import pytest
from datetime import date
from pyspark.sql import types as T
from respark.profiling import (
    StringColumnProfile,
    NumericalColumnProfile,
    DateColumnProfile,
    profile_string_column,
    profile_numerical_column,
    profile_date_column
)
    
sample_schema = T.StructType([
    T.StructField("first_name", T.StringType(), True),
    T.StructField("last_name", T.StringType(), True),
    T.StructField("department_id", T.IntegerType(), False),
    T.StructField("employment_start_date", T.DateType(), True),
])

sample_data = [
    ("Oliver", "Hughes", 1, date(2018, 5, 14)),
    ("Amelia", "Clark", 2, date(2019, 7, 22)),
    ("Jack", "Turner", 2, date(2020, 1, 10)),
    ("Emily", "Walker", 3, date(2017, 3, 5)),
    ("Harry", "Robinson", 1, date(2021, 11, 30)),
    ("Isla", "Wood", 5, date(2016, 9, 18)),
    ("George", "Hall", 4, date(2022, 2, 1)),
    ("Ava", "Thompson", 1, date(2020, 6, 25)),
    ("Noah", "Wright", 2, date(2015, 12, 12)),
    ("Sophia", "Green", 1, date(2018, 4, 3)),
]


def test_creates_valid_StringProfile(spark):
    sample_df = spark.createDataFrame(sample_data, sample_schema)
    first_name_profile = profile_string_column(sample_df, "first_name")

    assert isinstance(first_name_profile, StringColumnProfile)
    assert first_name_profile.name == "first_name"
    assert first_name_profile.normalised_type == "string"
    assert first_name_profile.nullable is True
    assert first_name_profile.min_length == 3
    assert first_name_profile.max_length == 6
    assert first_name_profile.mean_length == 4.9

def test_creates_valid_NumericalProfile(spark):
    sample_df = spark.createDataFrame(sample_data, sample_schema)
    department_id_profile = profile_numerical_column(sample_df, "department_id")

    assert isinstance(department_id_profile, NumericalColumnProfile)
    assert department_id_profile.name == "department_id"
    assert department_id_profile.normalised_type == "numeric"
    assert department_id_profile.nullable is False
    assert department_id_profile.min_value == 1
    assert department_id_profile.max_value == 5
    assert department_id_profile.mean_value == 2.2


def test_creates_valid_DateProfile(spark):
    sample_df = spark.createDataFrame(sample_data, sample_schema)
    date_profile = profile_date_column(sample_df, "employment_start_date")

    assert isinstance(date_profile, DateColumnProfile)
    assert date_profile.name == "employment_start_date"
    assert date_profile.normalised_type == "date"
    assert date_profile.nullable is True
    assert date_profile.min_date is not None
    assert date_profile.max_date is not None
    assert date_profile.min_date.isoformat() == "2015-12-12"
    assert date_profile.max_date.isoformat() == "2022-02-01"
