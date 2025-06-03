from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.dimension_updater import update_dim_ad, update_dim_game_position
from common.postgres_writer import read_from_postgres, write_to_postgres
from conf.mapping_table import common_table


class AdProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        dim_game_position = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_position"
        )
        dim_level = read_from_postgres(self.spark, f"{self.schema}.dim_level")
        dim_ad = read_from_postgres(self.spark, common_table["dim_ad"])

        enriched_df = (
            df.withColumn("position", self.get_field("position", "eventData_json"))
            .withColumn("ad_format", self.get_field("ad_format", "eventData_json"))
            .withColumn("ad_platform", self.get_field("ad_platform", "eventData_json"))
            .withColumn("ad_network", self.get_field("ad_network", "eventData_json"))
            .withColumn("is_show", self.get_field("is_show", "eventData_json"))
            .withColumn("load_time", self.get_field("load_time", "eventData_json"))
            .withColumn("value", self.get_field("value", "eventData_json"))
        )

        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_game_version)
        df_fact = self.map_level_id(df_fact, dim_level)
        df_fact = self.map_event_type_id(df_fact, self.broadcast_vars["dim_event_type"])

        update_dim_game_position(df_fact, dim_game_position, self.schema)
        dim_game_position = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_position"
        )
        df_fact = self.map_game_postition_id(df_fact, dim_game_position)

        update_dim_ad(df_fact, dim_ad)
        dim_ad = read_from_postgres(self.spark, common_table["dim_ad"])
        df_fact = self.map_ad_id(df_fact, dim_ad)

        df_fact = df_fact.withColumn(
            "is_success",
            F.when(F.col("is_show") == 1, F.lit(True)).otherwise(F.lit(False)),
        )

        df_ad_view = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("level_id"),
            F.col("event_type_id"),
            F.col("date_id").alias("view_date_id"),
            F.col("time_id").alias("view_time_id"),
            F.col("game_position_id").alias("ad_position_id"),
            F.col("ad_id"),
            F.col("is_success"),
            F.col("load_time").cast("int"),
            F.col("value").alias("rewarded").cast("int"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )

        write_to_postgres(df_ad_view, f"{self.schema}.fact_ad_view")
