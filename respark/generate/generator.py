from typing import Dict, Any, Optional, List, TYPE_CHECKING
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from respark.plan import SchemaGenerationPlan, TableGenerationPlan, ColumnGenerationPlan
from respark.rules.registry import get_generation_rule
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
    "timestamp_ltz": T.TimestampType(),
    "timestamp_ntz": T.TimestampNTZType(),
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
    ) -> Dict[str, DataFrame]:

        table_plan_map: Dict[str, TableGenerationPlan] = {
            table_plan.name: table_plan for table_plan in schema_gen_plan.table_plans
        }

        synth_schema: Dict[str, DataFrame] = {}
        if schema_gen_plan.table_generation_layers is None:
            schema_gen_plan.build_inter_table_dependencies()
        layers = schema_gen_plan.table_generation_layers

        if layers is not None:
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

        col_plan_map: Dict[str, ColumnGenerationPlan] = {
            col_plan.name: col_plan for col_plan in self.table_gen_plan.column_plans
        }

        if self.table_gen_plan.column_generation_layers is None:
            self.table_gen_plan.build_inter_col_dependencies()

        layers = self.table_gen_plan.column_generation_layers

        if layers is not None:
            for wave in layers:
                with ThreadPoolExecutor() as ex:
                    futures = {
                        ex.submit(
                            self._produce_column_df, synth_df, col_plan_map[col_name]
                        ): col_name
                        for col_name in wave
                    }
                    list_col_dfs: List[DataFrame] = []
                    for fut in as_completed(futures):
                        col_df = fut.result()
                        list_col_dfs.append(col_df)

                for col in list_col_dfs:
                    synth_df = synth_df.join(col, on="__row_idx", how="inner")

        ordered_cols = [col_plan.name for col_plan in self.table_gen_plan.column_plans]
        return synth_df.select("__row_idx", *ordered_cols).drop("__row_idx")

    def _produce_column_df(
        self, base_df: DataFrame, column_plan: ColumnGenerationPlan
    ) -> DataFrame:
        """
        Create a Dataframe (__row_idx, target_col) for a single column
        of generated synthetic data.
        """
        col_name = column_plan.name
        target_dtype_str = column_plan.data_type

        try:
            target_dtype = TYPE_DISPATCH[target_dtype_str]
        except KeyError:
            raise ValueError(f"Unsupported data type: '{target_dtype_str}'")

        col_seed = _create_stable_seed(
            self.seed, self.table_name, col_name, column_plan.rule
        )

        exec_params = {
            **column_plan.params,
            "__seed": col_seed,
            "__table": self.table_name,
            "__column": col_name,
            "__dtype": target_dtype_str,
            "__row_idx": F.col("__row_idx"),
        }

        rule = get_generation_rule(column_plan.rule, **exec_params)

        synth_col_df = rule.apply(base_df, self.runtime, target_col=col_name)
        synth_col_df = synth_col_df.withColumn(
            col_name, F.col(col_name).cast(target_dtype)
        )

        return synth_col_df.select("__row_idx", col_name)
