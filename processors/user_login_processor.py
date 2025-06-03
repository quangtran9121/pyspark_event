from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import read_from_postgres, write_to_postgres


class UserLoginProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        self.logger.debug(f"start process event resource login")
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        enriched_df = df.withColumn(
            "mail", self.get_field("email", "eventData_json")
        ).withColumn(
            "status",
            F.when(
                self.get_field("status", "eventData_json").cast("int") == 1, F.lit(True)
            ).otherwise(F.lit(False)),
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
            F.col("mail"),
            F.col("status").cast("boolean"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )
        self.logger.debug(f"start write fact to fact_user_login")
        write_to_postgres(df_screen, f"{self.schema}.fact_user_login")
