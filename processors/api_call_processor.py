from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import read_from_postgres, write_to_postgres
from common.dimension_updater import update_dim_api_type


class APICallProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        dim_api_type = read_from_postgres(self.spark, f"{self.schema}.dim_api_type")
        enriched_df = (
            df.withColumn(
                "api_call_duration", self.get_field("duration", "eventData_json")
            )
            .withColumn("api_call_status", self.get_field("status", "eventData_json"))
            .withColumn("api_type_name", self.get_field("type", "eventData_json"))
            .withColumn("api_error_message", self.get_field("error", "eventData_json"))
        )

        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_game_version)
        df_fact = self.map_event_type_id(df_fact, self.broadcast_vars["dim_event_type"])
        update_dim_api_type(df_fact, dim_api_type, self.schema)
        dim_api_type = read_from_postgres(self.spark, f"{self.schema}.dim_api_type")
        df_fact = self.map_api_call_type_id(df_fact, dim_api_type)
        df_fact_api_call = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("event_type_id"),
            F.col("date_id"),
            F.col("time_id"),
            F.col("api_type_id").cast("int"),
            F.col("api_call_duration").cast("int"),
            F.col("api_call_status").cast("boolean"),
            F.col("api_error_message"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )

        write_to_postgres(df_fact_api_call, f"{self.schema}.fact_api_call")
