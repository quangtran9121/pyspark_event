from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import read_from_postgres, write_to_postgres


class PurchaseFailReportProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        self.logger.debug(f"start process event resource purchase_fail_report")
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        enriched_df = (
            df.withColumn(
                "reason", self.get_field("reason", "eventData_json")
            )
            .withColumn("other_reason", self.get_field("other_reason", "eventData_json"))
        )

        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_game_version)

        df_screen = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("date_id"),
            F.col("time_id"),
            F.col("reason"),
            F.col("other_reason"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )
        self.logger.debug(f"start write fact to fact_purchase_fail_report")
        write_to_postgres(df_screen, f"{self.schema}.fact_purchase_fail_report")
