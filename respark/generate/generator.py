from typing import Dict, Any, Optional
import hashlib
from respark.plan import SchemaGenerationPlan, TableGenerationPlan
from respark.rules import get_generation_rule
from pyspark.sql import SparkSession, DataFrame, Column, functions as F, types as T


def _create_stable_seed(base_seed: int, *tokens: Any) -> int:
    payload = "|".join([str(base_seed), *map(str, tokens)]).encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    val64 = int.from_bytes(digest[:8], byteorder="big", signed=False)
    mixed = val64 ^ (base_seed & 0x7FFFFFFFFFFFFFFF)

    return mixed & 0x7FFFFFFFFFFFFFFF


TYPE_DISPATCH = {
    "boolean": T.BooleanType(),
    "double": T.DoubleType(),
    "decimal": T.DecimalType(),
    "date": T.DateType(),
    "float": T.FloatType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
    "string": T.StringType(),
}


class SynthSchemaGenerator:
    def __init__(
        self,
        spark: SparkSession,
        seed: int = 18151210,
        references: Optional[Dict[str, DataFrame]] = None,
    ):
        self.spark = spark
        self.seed = int(seed)
        self.references = references or {}

    def generate_synthetic_schema(
        self, schema_gen_plan: SchemaGenerationPlan
    ) -> Dict[str, DataFrame]:

        synth_schema: Dict[str, DataFrame] = {}

        for table_plan in schema_gen_plan.tables:
            table_generator = SynthTableGenerator(
                spark_session=self.spark,
                table_gen_plan=table_plan,
                seed=self.seed,
                references=self.references,
            )
            synth_schema[table_generator.table_gen_plan.name] = (
                table_generator.generate_synthetic_table()
            )

        return synth_schema


class SynthTableGenerator:
    def __init__(
        self,
        spark_session: SparkSession,
        table_gen_plan: TableGenerationPlan,
        seed: int = 18151210,
        references: Optional[Dict[str, DataFrame]] = None,
    ):
        self.spark = spark_session
        self.table_gen_plan = table_gen_plan
        self.table_name = table_gen_plan.name
        self.row_count = table_gen_plan.row_count
        self.seed = seed
        self.references = references or {}

    def generate_synthetic_table(self):
        synth_df = self.spark.range(0, self.row_count, 1)
        synth_df = synth_df.withColumnRenamed("id", "__row_idx")

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

            if column_plan.rule == "reuse_from_set":
                ref_key = exec_params["reference_df"]
                exec_params["reference_df"] = self.references[ref_key]
                exec_params.setdefault("reference_col", column_plan.name)

            rule = get_generation_rule(column_plan.rule, **exec_params)
            col_expr: Column = rule.generate_column()

            try:
                target_dtype = TYPE_DISPATCH[column_plan.data_type]
            except KeyError:
                raise ValueError(f"Unsupported data type: '{column_plan.data_type}' ")

            col_expr = col_expr.cast(target_dtype)

            synth_df = synth_df.withColumn(column_plan.name, col_expr)

        ordered_cols = [cgp.name for cgp in self.table_gen_plan.columns]
        return synth_df.select("__row_idx", *ordered_cols).drop("__row_idx")
