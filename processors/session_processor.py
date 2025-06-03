from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import write_to_postgres, read_from_postgres


class SessionProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )

        enriched_df = df.withColumn(
            "active_day", F.get_json_object("userProperty_json", "$.active_day")
        ).withColumn(
            "retention_day", F.get_json_object("userProperty_json", "$.retention_day")
        )

        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_game_version)

        df_fact = df_fact.withColumn("is_active", F.lit(1))

        df_out = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("date_id"),
            F.col("is_active"),
            F.col("active_day").cast("int"),
            F.col("retention_day").cast("int"),
            F.col("batch_id"),
        )

        write_to_postgres(df_out, f"{self.schema}.fact_player_activity")
