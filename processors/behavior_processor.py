from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import read_from_postgres, write_to_postgres


class BehaviorProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )

        enriched_df = df.withColumn(
            "screen_name", self.get_field("screen_name", "eventData_json")
        ).withColumn("id_item", self.get_field("id_item", "eventData_json"))

        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_game_version)

        df_screen = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("date_id").alias("behavior_date_id"),
            F.col("time_id"),
            F.col("screen_name").cast("int"),
            F.col("id_item"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )

        write_to_postgres(df_screen, f"{self.schema}.fact_user_behavior")
