from datetime import date
from pyspark.sql import types as T

from respark.profiling import SchemaProfiler
from respark.planning import SchemaGenerationPlan, make_generation_plan


def test_generation_plan_infers_rules_and_params(spark):

    #Test Data
    employees_schema = T.StructType([
        T.StructField("first_name", T.StringType(), False),     
        T.StructField("last_name", T.StringType(), False),      
        T.StructField("department_id", T.IntegerType(), False), 
        T.StructField("start_date", T.DateType(), False),       
        T.StructField("salary", T.IntegerType(), False),        
    ])

    employees_data = [
        ("Amy",       "Hill",     1, date(2020, 1, 1), 28000),
        ("Liam",      "Baker",    3, date(2020, 1, 2), 45000),
        ("Charlotte", "Thompson", 5, date(2020, 1, 3), 52000),
    ]

    departments_schema = T.StructType([
        T.StructField("department_id", T.IntegerType(), False),  
        T.StructField("department_name", T.StringType(), False)        
    ])

    departments_data = [
        (1, "HR"),
        (2, "Software"),
        (3, "Sales"),
    ]

    employees_df = spark.createDataFrame(employees_data, employees_schema)
    departments_df = spark.createDataFrame(departments_data, departments_schema)

    # Generate test profile and proposed generation plan
    profiler = SchemaProfiler()
    schema_profile = profiler.profile_schema({
        "employees": employees_df,
        "departments": departments_df,
    })
    plan = make_generation_plan(schema_profile)

    # Check Generation Plan
    assert isinstance(plan, SchemaGenerationPlan)
    tables = {t.name: t for t in plan.tables}
    assert set(tables) == {"employees", "departments"}
    assert tables["employees"].row_count == len(employees_data)
    assert tables["departments"].row_count == len(departments_data)


    emp_cols = {c.name: c for c in tables["employees"].columns}
    dep_cols = {c.name: c for c in tables["departments"].columns}

    # Check default assigned rules
    assert {c.name: c.rule for c in tables["employees"].columns} == {
        "first_name": "random_string",
        "last_name": "random_string",
        "department_id": "random_int",
        "start_date": "random_date",
        "salary": "random_int",
    }
    assert {c.name: c.rule for c in tables["departments"].columns} == {
        "department_id": "random_int",
        "department_name": "random_string"
    }

    # Check parameters passed as expected

    assert emp_cols["first_name"].params.get("min_length") == 3
    assert emp_cols["first_name"].params.get("max_length") == 9

    assert emp_cols["last_name"].params.get("min_length") == 4
    assert emp_cols["last_name"].params.get("max_length") == 8

    assert dep_cols["department_name"].params.get("min_length") == 2
    assert dep_cols["department_name"].params.get("max_length") == 8

    assert emp_cols["department_id"].params.get("min_value") == 1
    assert emp_cols["department_id"].params.get("max_value") == 5

    assert emp_cols["salary"].params.get("min_value") == 28000
    assert emp_cols["salary"].params.get("max_value") == 52000

    assert dep_cols["department_id"].params.get("min_value") == 1
    assert dep_cols["department_id"].params.get("max_value") == 3

    assert emp_cols["start_date"].params.get("min_date") == "2020-01-01"
    assert emp_cols["start_date"].params.get("max_date") == "2020-01-03"

    # Test .to_dict()
    as_dict = plan.to_dict()
    assert isinstance(as_dict, dict)
    assert "tables" in as_dict and isinstance(as_dict["tables"], list)
 
