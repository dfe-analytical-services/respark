from typing import Dict, Any
import hashlib
from respark.planning import SchemaGenerationPlan, TableGenerationPlan, get_rule
from pyspark.sql import SparkSession, DataFrame, Column, functions as F, types as T


def _create_stable_seed(base_seed: int, *tokens: Any) -> int:
    payload = "|".join([str(base_seed), *map(str, tokens)]).encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    val64 = int.from_bytes(digest[:8], byteorder="big", signed=False)
    mixed = val64 ^ (base_seed & 0x7FFFFFFFFFFFFFFF)

    return mixed & 0x7FFFFFFFFFFFFFFF


def _str_to_spark_type(type_as_str: str) -> T.DataType:
    if type_as_str == "numeric":
        return T.IntegerType()
    if type_as_str == "string":
        return T.StringType()
    if type_as_str == "date":
        return T.DateType()
    else:
        raise TypeError("Unsupported Type")


class SynthSchemaGenerator:
    def __init__(self, spark: SparkSession, seed: int = 18151210):
        self.spark = spark
        self.seed = int(seed)

    def generate_synthetic_schema(
        self, spark: SparkSession, schema_gen_plan: SchemaGenerationPlan
    ) -> Dict[str, DataFrame]:

        synth_schema: Dict[str, DataFrame] = {}

        for table_plan in schema_gen_plan.tables:
            table_generator = SynthTableGenerator(
                spark=self.spark,
                table_gen_plan=table_plan,
                seed=self.seed,
            )
            synth_schema[table_generator.table_gen_plan.name] = (
                table_generator.generate_synthetic_table()
            )

        return synth_schema


class SynthTableGenerator:
    def __init__(
        self,
        spark: SparkSession,
        table_gen_plan: TableGenerationPlan,
        seed: int = 18151210,
    ):
        self.spark = spark
        self.table_gen_plan = table_gen_plan
        self.table_name = table_gen_plan.name
        self.row_count = table_gen_plan.row_count
        self.seed = seed

    def generate_synthetic_table(self):
        snyth_df = self.spark.range(0, self.row_count, 1)
        snyth_df = snyth_df.withColumnRenamed("id", "__row_idx")

        for column_plan in self.table_gen_plan.columns:
            col_seed = _create_stable_seed(
                self.seed, self.table_name, column_plan.name, column_plan.rule
            )

            exec_params = {
                **column_plan.params,
                "__seed": col_seed,
                "__table": self.table_name,
                "__column": column_plan.name,
                "__dtype": column_plan.data_type,
                "__row_idx": F.col("__row_idx"),
            }

            rule = get_rule(column_plan.rule, **exec_params)
            col_expr: Column = rule.generate_column()

            target_dtype = _str_to_spark_type(column_plan.data_type)
            col_expr = col_expr.cast(target_dtype)

            snyth_df = snyth_df.withColumn(column_plan.name, col_expr)

        ordered_cols = [cgp.name for cgp in self.table_gen_plan.columns]
        return snyth_df.select("__row_idx", *ordered_cols).drop("__row_idx")
