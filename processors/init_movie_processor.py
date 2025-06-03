from pyspark.sql import DataFrame, functions as F
from processors.base_processor import BaseProcessor
from common.postgres_writer import read_from_postgres, write_to_postgres
from common.dimension_updater import update_dim_movie_place


class InitMovieProcessor(BaseProcessor):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        super().__init__(spark, broadcast_vars, schema, logger)

    def process(self, df: DataFrame) -> None:
        dim_user = read_from_postgres(self.spark, f"{self.schema}.dim_user")
        dim_game_version = read_from_postgres(
            self.spark, f"{self.schema}.dim_game_version"
        )
        dim_movie_place = read_from_postgres(
            self.spark, f"{self.schema}.dim_movie_place"
        )
        enriched_df = (
            df.withColumn("movie_place", self.get_field("place", "eventData_json"))
            .withColumn("movie_type", self.get_field("type", "eventData_json"))
            .withColumn("movie_id", self.get_field("movie_id", "eventData_json"))
            .withColumn("movie_ep_id", self.get_field("ep_id", "eventData_json"))
            .withColumn("movie_status", self.get_field("status", "eventData_json"))
            .withColumn("movie_duration", self.get_field("duration", "eventData_json"))
        )

        df_fact = self.map_user_id(enriched_df, dim_user)
        df_fact = self.map_time_id(df_fact)
        df_fact = self.map_date_id(df_fact)
        df_fact = self.filter_date_id(df_fact)
        df_fact = self.map_version_id(df_fact, dim_game_version)
        df_fact = self.map_event_type_id(df_fact, self.broadcast_vars["dim_event_type"])
        update_dim_movie_place(df_fact, dim_movie_place, self.schema)
        dim_movie_place = read_from_postgres(
            self.spark, f"{self.schema}.dim_movie_place"
        )
        df_fact = self.map_move_place_id(df_fact, dim_movie_place)
        df_fact_api_call = df_fact.select(
            F.col("user_id"),
            F.col("version_id"),
            F.col("event_type_id"),
            F.col("date_id"),
            F.col("time_id"),
            F.col("movie_place_id").cast("int"),
            F.col("movie_type"),
            F.col("movie_id").cast("int"),
            F.col("movie_ep_id").cast("int"),
            F.col("movie_status").cast("boolean"),
            F.col("movie_duration").cast("int"),
            F.col("sequence_id").cast("int"),
            F.col("batch_id"),
        )

        write_to_postgres(df_fact_api_call, f"{self.schema}.fact_init_movie")
