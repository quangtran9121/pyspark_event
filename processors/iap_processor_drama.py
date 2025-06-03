from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import write_to_postgres, read_from_postgres
from common.dimension_updater import (
    update_dim_game_position,
    update_dim_package,
    update_show_type,
)


class IAPProcessor(BaseProcessor):
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
        dim_package = read_from_postgres(self.spark, f"{self.schema}.dim_package")
        dim_show_type = read_from_postgres(self.spark, f"{self.schema}.dim_show_type")

        enriched_df = (
            df.withColumn("position", self.get_field("position", "eventData_json"))
            .withColumn("show_type", self.get_field("show_type", "eventData_json"))
            .withColumn("pack_name", self.get_field("pack_name", "eventData_json"))
            .withColumn(
                "status",
                F.when(
                    self.get_field("status", "eventData_json").isNotNull(),
                    F.when(
                        F.lower(
                            self.get_field("status", "eventData_json").cast("string")
                        )
                        == "success",
                        F.lit(True),
                    )
                    .when(
                        F.lower(
                            self.get_field("status", "eventData_json").cast("string")
                        )
                        == "error",
                        F.lit(False),
                    )
                    .otherwise(
                        self.get_field("status", "eventData_json").cast("boolean")
                    ),
                ).otherwise(F.lit(None).cast("boolean")),
            )
        )

        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_game_version)

        update_dim_game_position(df_fact, dim_game_position, self.schema)
        dim_game_position = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_position"
        )
        df_fact = self.map_game_postition_id(df_fact, dim_game_position)

        update_dim_package(df_fact, dim_package, self.schema)
        dim_package = read_from_postgres(self.spark, f"{self.schema}.dim_package")
        df_fact = self.map_package_id(df_fact, dim_package)

        update_show_type(df_fact, dim_show_type, self.schema)
        dim_show_type = read_from_postgres(self.spark, f"{self.schema}.dim_show_type")
        df_fact = self.map_show_type_id(df_fact, dim_show_type)

        # df_fact.show(10, truncate=False)
        df_iap = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("date_id").alias("iap_date_id"),
            F.col("time_id").alias("iap_time_id"),
            F.col("game_position_id").alias("iap_position_id"),
            F.col("show_type_id"),
            F.col("package_id"),
            F.col("status").cast("boolean"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )
        write_to_postgres(df_iap, f"{self.schema}.fact_iap")

        self.process_iap_purchase(df_fact)

    def process_iap_purchase(self, df: DataFrame) -> None:
        iap_purchase_df = df.filter(F.col("eventName") == "iap_purchase")

        enriched_iap_purchase_df = iap_purchase_df.withColumn(
            "currencyCode", self.get_field("currencyCode", "eventData_json")
        ).withColumn("price", self.get_field("price", "eventData_json"))

        df_fact = self.map_currency_id(
            enriched_iap_purchase_df, self.broadcast_vars["dim_currency"]
        )

        df_purchase_action = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("date_id").alias("purchase_date_id"),
            F.col("time_id").alias("purchase_time_id"),
            F.col("package_id"),
            F.col("price").cast("float"),
            F.col("currency_id"),
            F.col("game_position_id").alias("purchase_position_id"),
            F.col("status"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )
        df_success = df_purchase_action.filter(F.col("status").cast("boolean") == True)
        df_cancel = df_purchase_action.filter(F.col("status").cast("boolean") == False)
        write_to_postgres(
            df_success.drop("status"), f"{self.schema}.fact_purchase_action"
        )
        write_to_postgres(
            df_cancel.drop("status"), f"{self.schema}.fact_cancel_purchase_action"
        )
