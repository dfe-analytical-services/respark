from typing import Dict, Any, Optional, List, TYPE_CHECKING
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from respark.plan import SchemaGenerationPlan, TableGenerationPlan
from respark.rules import get_generation_rule
from respark.profile import FkConstraint, DAG
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


if TYPE_CHECKING:
    from respark.runtime import ResparkRuntime


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
        runtime: Optional["ResparkRuntime"],
        seed: int = 18151210,
        references: Optional[Dict[str, DataFrame]] = None,
    ):
        self.spark = spark
        self.seed = int(seed)
        self.references = references or {}
        self.runtime = runtime

    def generate_synthetic_schema(
        self,
        schema_gen_plan: SchemaGenerationPlan,
        fk_constraints: Dict[str, FkConstraint],
    ) -> Dict[str, DataFrame]:

        table_plan_map: Dict[str, TableGenerationPlan] = {
            tgp.name: tgp for tgp in schema_gen_plan.tables
        }
        table_names = list(table_plan_map.keys())

        synth_schema: Dict[str, DataFrame] = {}

        dag = DAG.from_fk_constraints(table_names, fk_constraints)
        layers = dag.compute_layers()

        for layer in layers:
            with ThreadPoolExecutor() as ex:
                futures = {
                    ex.submit(self._generate_table, table_plan_map[name]): name
                    for name in layer
                }

                for fut in as_completed(futures):
                    name = futures[fut]
                    df = fut.result()
                    synth_schema[name] = df
                    if self.runtime is not None:
                        self.runtime.generated_synthetics[name] = df

        return synth_schema

    def _generate_table(self, table_plan: TableGenerationPlan) -> DataFrame:
        tg = SynthTableGenerator(
            spark_session=self.spark,
            table_gen_plan=table_plan,
            seed=self.seed,
            references=self.references,
            runtime=self.runtime,
        )
        return tg.generate_synthetic_table()


class SynthTableGenerator:
    def __init__(
        self,
        spark_session: SparkSession,
        runtime: Optional["ResparkRuntime"],
        table_gen_plan: TableGenerationPlan,
        seed: int = 18151210,
        references: Optional[Dict[str, DataFrame]] = None,
    ):
        self.spark = spark_session
        self.runtime = runtime
        self.table_gen_plan = table_gen_plan
        self.table_name = table_gen_plan.name
        self.row_count = table_gen_plan.row_count
        self.seed = seed
        self.references = references or {}

    def generate_synthetic_table(self) -> DataFrame:
        synth_df = self.spark.range(0, self.row_count, 1).withColumnRenamed(
            "id", "__row_idx"
        )

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

            rule = get_generation_rule(column_plan.rule, **exec_params)

            try:
                target_dtype = TYPE_DISPATCH[column_plan.data_type]
            except KeyError:
                raise ValueError(f"Unsupported data type: '{column_plan.data_type}'")

            if column_plan.name not in synth_df.columns:
                synth_df = synth_df.withColumn(
                    column_plan.name, F.lit(None).cast(target_dtype)
                )

            synth_df = rule.apply(synth_df, self.runtime, target_col=column_plan.name)

            synth_df = synth_df.withColumn(
                column_plan.name, F.col(column_plan.name).cast(target_dtype)
            )

        ordered_cols = [cgp.name for cgp in self.table_gen_plan.columns]
        return synth_df.select("__row_idx", *ordered_cols).drop("__row_idx")
