from pyspark.sql import DataFrame, functions as F, types as T
from processors.base_processor import BaseProcessor
from common.postgres_writer import write_to_postgres, read_from_postgres


class PlayerChageProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        dim_level = read_from_postgres(self.spark, f"{self.schema}.dim_level")

        enriched_df = df.withColumn("name", self.get_field("name", "eventData_json"))

        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_game_version)
        df_fact = self.map_level_id(df_fact, dim_level)

        df_has_items = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("level_id"),
            F.col("date_id").alias("action_date_id"),
            F.col("time_id"),
            F.col("name").alias("action"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )
        write_to_postgres(df_has_items, f"{self.schema}.fact_player_action")
