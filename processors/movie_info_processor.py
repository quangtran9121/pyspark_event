from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import write_to_postgres, read_from_postgres
from common.dimension_updater import update_dim_movie_type
import time


class MovieInfoProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        self.logger.debug(f"start process event resource")
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_language = read_from_postgres(self.spark, "common_dim_schema.dim_language")
        dim_app_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        dim_movie_type = read_from_postgres(self.spark, f"{self.schema}.dim_movie_type")
        enriched_df = (
            df.withColumn("movie_type", self.get_field("type", "eventData_json"))
            .withColumn("movie_id", self.get_field("movie_id", "eventData_json"))
            .withColumn(
                "movie_category", self.get_field("movie_category", "eventData_json")
            )
            .withColumn("ep_id", self.get_field("ep_id", "eventData_json"))
            .withColumn(
                "time_watching", self.get_field("time_watching", "eventData_json")
            )
            .withColumn(
                "language_code", self.get_field("language", "eventData_json")
            )
        )
        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_app_version)
        update_dim_movie_type(df_fact, dim_movie_type, self.schema)
        dim_movie_type = read_from_postgres(self.spark, f"{self.schema}.dim_movie_type")

        df_fact = self.map_movie_type_id(df_fact, dim_movie_type)
        df_fact = self.map_language_id(df_fact, dim_language)
        self.post_data_process(df_fact)

    def map_movie_type_id(
        self,
        df: DataFrame,
        dim_movie_type: DataFrame,
    ) -> DataFrame:
        return_df = df.join(
            dim_movie_type, df.movie_type == dim_movie_type.movie_type_name, "left"
        )
        return_df = return_df.drop("movie_type_name", "movie_type")
        return return_df

    def post_data_process(self, df: DataFrame) -> None:
        df_out = df.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("date_id").alias("movie_date_id"),
            F.col("movie_type_id"),
            F.col("time_id"),
            F.col("movie_id").cast("int"),
            F.col("language_id").cast("int"),
            F.col("movie_category").cast("int"),
            F.col("ep_id").cast("int"),
            F.col("time_watching").cast("int"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )
        self.logger.debug(f"start write fact to db")
        write_to_postgres(df_out, f"{self.schema}.fact_movie_info")
