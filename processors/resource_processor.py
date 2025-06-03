from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.dimension_updater import (
    update_dim_item,
    update_dim_item_type,
    update_dim_game_position,
)
from common.postgres_writer import write_to_postgres, read_from_postgres


class ResourceProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        self.logger.debug(f"start process event resource")
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        dim_game_position = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_position"
        )
        dim_level = read_from_postgres(self.spark, f"{self.schema}.dim_level")
        dim_item = read_from_postgres(self.spark, f"{self.schema}.dim_item")
        dim_item_type = read_from_postgres(self.spark, f"{self.schema}.dim_item_type")

        enriched_df = (
            df.withColumn("reason", self.get_field("reason", "eventData_json"))
            .withColumn("position", self.get_field("position", "eventData_json"))
            .withColumn(
                "resource_type", self.get_field("resource_type", "eventData_json")
            )
            .withColumn(
                "resource_name", self.get_field("resource_name", "eventData_json")
            )
            .withColumn(
                "resource_amount", self.get_field("resource_amount", "eventData_json")
            )
        )

        self.logger.debug(f"start mapping id")
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

        df_fact = self.process_array_df(df_fact)
        self.post_process_df(df_fact, dim_item_type, dim_item)

    def process_array_df(self, df: DataFrame) -> DataFrame:
        df_exploded = df.withColumn(
            "resource_name_array", F.split(F.col("resource_name"), ",")
        ).withColumn("resource_amount_array", F.split(F.col("resource_amount"), ","))

        df_exploded = df_exploded.withColumn(
            "resource_pairs",
            F.arrays_zip("resource_name_array", "resource_amount_array"),
        )
        df_exploded = df_exploded.withColumn(
            "resource_pair", F.explode("resource_pairs")
        )

        df_exploded = (
            df_exploded.withColumn(
                "resource_name", F.col("resource_pair.resource_name_array")
            )
            .withColumn("resource_amount", F.col("resource_pair.resource_amount_array"))
            .drop(
                "resource_name_array",
                "resource_amount_array",
                "resource_pairs",
                "resource_pair",
            )
        )

        # df_exploded.show(truncate=False)

        return df_exploded

    def post_process_df(
        self, df: DataFrame, dim_item_type: DataFrame, dim_item: DataFrame
    ) -> None:
        update_dim_item_type(df, dim_item_type, self.schema)
        dim_item_type_updated = read_from_postgres(
            self.spark, f"{self.schema}.dim_item_type"
        )
        df_fact = self.map_item_type_id(df, dim_item_type_updated)

        update_dim_item(df_fact, dim_item, self.schema)
        dim_item_updated = read_from_postgres(self.spark, f"{self.schema}.dim_item")
        df_fact = self.map_item_id(df_fact, dim_item_updated)

        df_fact = df_fact.withColumn(
            "transaction_type",
            F.when(F.col("eventName") == "resource_sink", F.lit("sink")).otherwise(
                F.lit("source")
            ),
        )

        df_out = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("level_id"),
            F.col("event_type_id"),
            F.col("date_id").alias("transaction_date_id"),
            F.col("time_id"),
            F.col("transaction_type"),
            F.col("item_id"),
            F.col("resource_amount").alias("quantity").cast("int"),
            F.col("game_position_id"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )
        self.logger.debug(f"start write fact to db")
        write_to_postgres(df_out, f"{self.schema}.fact_item_transaction")
