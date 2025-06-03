from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import write_to_postgres, read_from_postgres
from common.dimension_updater import update_dim_email, update_dim_feedback
import time


class FeedbackProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        self.logger.debug(f"start process event resource")
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_app_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        dim_email = read_from_postgres(self.spark, "common_dim_schema.dim_email")
        dim_feedback = read_from_postgres(self.spark, f"{self.schema}.dim_feedback")
        enriched_df = (
            df.withColumn("email", self.get_field("email", "eventData_json"))
            .withColumn("feedback", self.get_field("feedback", "eventData_json"))
        )
        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_app_version)
        update_dim_email(df_fact, dim_email, "common_dim_schema.dim_email")
        update_dim_feedback(df_fact, dim_feedback, f"{self.schema}.dim_feedback")
        dim_email = read_from_postgres(self.spark, "common_dim_schema.dim_email")
        dim_feedback = read_from_postgres(self.spark, f"{self.schema}.dim_feedback")

        df_fact = self.map_email_id(df_fact, dim_email)
        df_fact = self.map_feedback_id(df_fact, dim_feedback)
        self.post_data_process(df_fact)

    def post_data_process(self, df: DataFrame) -> None:
        df_out = df.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("date_id"),
            F.col("time_id"),
            F.col("email_id"),
            F.col("feedback_id"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )
        
        self.logger.debug(f"start write fact to db")
        
        # Gọi hàm để ghi dữ liệu vào Postgres
        write_to_postgres(df_out, f"{self.schema}.fact_feedback")

