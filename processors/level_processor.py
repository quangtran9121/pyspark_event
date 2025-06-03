from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import write_to_postgres, read_from_postgres


class LevelProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        # dim_game_position = read_from_postgres(self.spark, f"{self.schema}.dim_game_position")
        dim_level = read_from_postgres(self.spark, f"{self.schema}.dim_level")

        df_fact = self.map_user_id(df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_game_version)
        df_fact = self.map_level_id(df_fact, dim_level)
        df_fact = self.map_event_type_id(df_fact, self.broadcast_vars["dim_event_type"])

        self.process_level_event(
            df_fact.filter(F.col("eventName") != "level_second_chance")
        )
        self.process_level_second_chance(
            df_fact.filter(F.col("eventName") == "level_second_chance")
        )

    def process_level_event(self, df: DataFrame) -> None:
        enriched_df = (
            df.withColumn(
                "durationPlay", self.get_field("durationPlay", "eventData_json")
            )
            .withColumn(
                "durationTotal", self.get_field("durationTotal", "eventData_json")
            )
            .withColumn(
                "require_time", self.get_field("require_time", "eventData_json")
            )
            # .withColumn("play_time", self.get_field("play_time", "eventData_json")) \
            .withColumn("play_index", self.get_field("play_index", "eventData_json"))
            .withColumn("result", self.get_field("result", "eventData_json"))
        )

        df_level_event = enriched_df.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("level_id"),
            F.col("event_type_id"),
            F.col("date_id").alias("event_date_id"),
            F.col("time_id"),
            F.col("durationPlay").cast("int").alias("play_time"),
            F.col("play_index").cast("int"),
            F.col("result"),
            F.col("require_time").cast("int"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )

        write_to_postgres(df_level_event, f"{self.schema}.fact_level_event")

    def process_level_second_chance(self, df: DataFrame) -> None:
        enriched_df = df.withColumn(
            "bonus_type", self.get_field("bonus_type", "eventData_json")
        ).withColumn("bonus_amount", self.get_field("bonus_amount", "eventData_json"))

        df_level_event = enriched_df.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("level_id"),
            F.col("event_type_id"),
            F.col("date_id").alias("event_date_id"),
            F.col("time_id"),
            F.col("bonus_type"),
            F.col("bonus_amount").cast("int"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )

        write_to_postgres(df_level_event, f"{self.schema}.fact_level_event")
