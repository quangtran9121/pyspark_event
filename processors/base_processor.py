from abc import ABC, abstractmethod
from conf.config import config_app
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from pyspark.sql import Window
from pyspark.sql import DataFrame, functions as F
from common.postgres_writer import write_to_postgres, read_from_postgres


class BaseProcessor(ABC):
    def __init__(self, spark, broadcast_vars, schema, logger) -> None:
        self.spark = spark
        self.broadcast_vars = broadcast_vars
        self.schema = schema
        self.logger = logger

    @abstractmethod
    def process(self, df: DataFrame) -> None:
        pass

    def get_platfrom_id(seft):
        if F.get_json_object("userProperty_json", "platform") == "Android":
            return 2
        else:
            return 1

    def get_field(self, field_name: str, object_name: str):
        return F.get_json_object(object_name, f"$.{field_name}")

    def map_time_id(self, df: DataFrame) -> DataFrame:
        df_map = df.withColumn(
            "time_id",
            F.coalesce(
                F.coalesce(F.hour("timestamp"), F.lit(0)) * 3600
                + F.coalesce(F.minute("timestamp"), F.lit(0)) * 60
                + F.coalesce(F.second("timestamp"), F.lit(0))
            ).cast("int"),
        )
        return df_map

    def map_date_id(self, df: DataFrame) -> DataFrame:
        df_map = df.withColumn(
            "date_id",
            F.coalesce(
                F.date_format(F.to_date("timestamp"), "yyyyMMdd"),
                F.lit("19700101"),  # Default date_id for nulls
            ).cast("int"),
        )
        return df_map

    def map_user_id(
        self,
        df: DataFrame,
        dim_user: DataFrame,
        list_drop_column: list = [
            "user_uuid",
            "registration_date_id",
            "location_id",
            "device_id",
            "language_id",
        ],
    ) -> DataFrame:
        df = df.withColumnRenamed("user_id", "user_uuid")
        df_map = df.join(dim_user, dim_user.user_uuid == df.user_uuid, "left")
        for col in list_drop_column:
            df_map = df_map.drop(col)

        return df_map

    def map_ad_id(self, df: DataFrame, dim_ad: DataFrame) -> DataFrame:
        dim_ad_alias = dim_ad.alias("da")
        df_map = df.join(
            dim_ad_alias,
            (dim_ad_alias.ad_format == df.ad_format)
            & (dim_ad_alias.ad_platform == df.ad_platform)
            & (dim_ad_alias.ad_network == df.ad_network),
            "left",
        )
        df_map = df_map.drop("da.ad_format", "da.ad_platform", "da.ad_network")
        return df_map

    def map_version_id(self, df: DataFrame, dim_game_version: DataFrame) -> DataFrame:
        dim_game_version_alias = dim_game_version.alias("dgv")
        df_map = df.join(
            dim_game_version_alias,
            dim_game_version_alias.version_number == F.col("app_version"),
            "left",
        )
        df_map = df_map.drop("dgv.game_id", "dgv.version_number", "dgv.start_date_id")
        return df_map

    def map_level_id(self, df: DataFrame, dim_level: DataFrame) -> DataFrame:
        columns_to_check = ["mode"]
        for col in columns_to_check:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(None))
        dim_level_renamed = dim_level.withColumnRenamed("mode", "dim_mode")

        df_map = df.join(
            dim_level_renamed,
            (dim_level_renamed.level_number == F.col("level"))
            & (
                (dim_level_renamed.dim_mode == F.col("mode"))
                | (dim_level_renamed.dim_mode.isNull() & F.col("mode").isNull())
            ),
            "left",
        )

        df_map = df_map.drop("level_number", "dim_mode")

        return df_map

    def map_event_type_id(self, df: DataFrame, dim_event_type: DataFrame) -> DataFrame:
        df_map = df.join(
            dim_event_type,
            dim_event_type.event_name == F.col("eventName"),
            "left",
        )
        df_map = df_map.drop("event_name", "event_type")

        return df_map

    def map_item_id(self, df: DataFrame, dim_item: DataFrame) -> DataFrame:
        df_map = df.join(
            dim_item,
            (dim_item.item_type_id == df.item_type_id)
            & (dim_item.item_name == df.resource_name),
            "left",
        )

        df_map = df_map.drop(dim_item.item_type_id, dim_item.item_name)

        return df_map

    def map_item_type_id(self, df: DataFrame, dim_item_type: DataFrame) -> DataFrame:
        df_map = df.join(
            dim_item_type,
            dim_item_type.item_type_name == F.col("resource_type"),
            "left",
        )
        df_map = df_map.drop("item_type_name")

        return df_map

    def map_currency_id(self, df: DataFrame, dim_currency: DataFrame) -> DataFrame:
        df_map = df.join(
            dim_currency,
            dim_currency.currency_code == F.col("currencyCode"),
            "left",
        )

        df_map = df_map.drop("currency_name", "currency_code")

        return df_map

    def map_game_postition_id(
        self, df: DataFrame, dim_game_position: DataFrame
    ) -> DataFrame:
        df_map = df.join(
            dim_game_position,
            (dim_game_position.position == df.position),
            "left",
        )
        df_map = df_map.drop(dim_game_position.position)

        return df_map

    def map_package_id(self, df: DataFrame, dim_package: DataFrame) -> DataFrame:
        df_map = df.join(
            dim_package,
            (dim_package.package_name == df.pack_name),
            "left",
        )
        df_map = df_map.drop("package_name")

        return df_map

    def map_show_type_id(self, df: DataFrame, dim_show_type: DataFrame) -> DataFrame:
        df_map = df.join(
            dim_show_type,
            (dim_show_type.show_type == df.show_type),
            "left",
        )
        df_map = df_map.drop(dim_show_type.show_type)

        return df_map

    def map_location_id(self, df: DataFrame, dim_location: DataFrame) -> DataFrame:
        dim_location_renamed = dim_location.withColumnRenamed("city", "dim_city")
        # dim_location_renamed.show(20, truncate=False)
        df_map = df.join(
            dim_location_renamed,
            (dim_location_renamed.country_code == F.col("countryCode"))
            & (dim_location_renamed.dim_city == F.col("city")),
            "left",
        )
        # df_map.show(20, truncate=False)
        df_map = df_map.drop("country", "country_code", "dim_city")
        df_check = df_map.filter(F.col("location_id").isNull())
        # df_check.show(20, truncate=True)

        return df_map

    def map_user_platfrom(self, df: DataFrame) -> DataFrame:
        pass

    def filter_date_id(self, df: DataFrame) -> DataFrame:
        df_filter = df.withColumn(
            "date_id_filter",
            F.when(
                (F.col("date_id").cast("int") > 20241101)
                & (
                    F.col("date_id").cast("int")
                    <= F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int")
                ),
                F.col("date_id"),
            ).otherwise(F.lit(19700101)),
        )
        df_filter = df_filter.drop("date_id")
        df_filter = df_filter.withColumnRenamed("date_id_filter", "date_id")

        return df_filter

    def merge_staging_to_dim_user(
        self, df: DataFrame, db_table: str, merge_sql: str
    ) -> None:
        def delete_data_table(session):
            delete_data_sql = f"delete from {db_table}"
            session.execute(text(delete_data_sql))
            session.commit()

        # Execute the upsert
        engine = create_engine(config_app.env.POSTGRES_URL_ENGINE)
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            delete_data_table(session)
            write_to_postgres(df, db_table)
            session.execute(text(merge_sql))
            session.commit()
            delete_data_table(session)
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def get_latest_record(self, df: DataFrame) -> DataFrame:
        w = Window.partitionBy("playerId").orderBy(F.col("date_id").desc())

        df_ranked = df.withColumn("rn", F.row_number().over(w))

        df_latest = df_ranked.filter(F.col("rn") == 1).drop("rn")

        return df_latest

    def map_api_call_type_id(self, df: DataFrame, dim_api_call: DataFrame) -> DataFrame:
        dim_api_call.printSchema()
        dim_api_call.show(5)
        dim_api_call = dim_api_call.withColumnRenamed(
            "api_type_name", "dim_api_type_name"
        )
        df_map = df.join(
            dim_api_call,
            (dim_api_call.dim_api_type_name == df.api_type_name),
            "left",
        )
        df_map = df_map.drop("dim_api_type_name")
        return df_map

    def map_move_place_id(self, df: DataFrame, dim_movie_place: DataFrame) -> DataFrame:
        dim_movie_place.printSchema()
        dim_movie_place.show(5)
        dim_movie_place = dim_movie_place.withColumnRenamed(
            "movie_place", "dim_movie_place"
        )
        df_map = df.join(
            dim_movie_place,
            (dim_movie_place.dim_movie_place == df.movie_place),
            "left",
        )
        df_map = df_map.drop("dim_movie_place")
        return df_map
    
    def map_language_id(self, df: DataFrame, dim_language: DataFrame) -> DataFrame:
        df_map = df.join(
            dim_language,
            dim_language.language_code == df.language_code,
            "left",
        )

        df_map = df_map.drop("language_code", "language_name")
        
        return df_map
    
    def map_email_id(self, df: DataFrame, dim_email: DataFrame) -> DataFrame:
        df_map = df.join(
            dim_email,
            dim_email.email == df.email,
            "left",
        )

        df_map = df_map.drop("email")
        
        return df_map
    
    def map_feedback_id(self, df: DataFrame, dim_feedback: DataFrame) -> DataFrame:
        df_map = df.join(
            dim_feedback,
            dim_feedback.feedback == df.feedback,
            "left",
        )

        df_map = df_map.drop("feedback")
        
        return df_map