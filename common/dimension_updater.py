from pyspark.sql import DataFrame, functions as F, types as T
from common.postgres_writer import write_to_postgres, read_from_postgres
from conf.mapping_table import common_table


def update_dim_user(
    spark, df: DataFrame, dim_user: DataFrame, game_schema: str
) -> None:
    df_users = df.groupBy("user_id").agg(
        F.first("install_day").alias("install_day"),
        F.first("device_info").alias("device_info"),
        F.first("platform").alias("platform"),
        F.first("country_code").alias("country_code"),
        F.first("country_city").alias("country_city"),
        F.first("language").alias("language_code"),
    )

    dim_location = read_from_postgres(spark, common_table["dim_location"])
    dim_device = read_from_postgres(spark, common_table["dim_device"])
    dim_language = read_from_postgres(spark, common_table["dim_language"])

    df_users = df_users.join(
        dim_location,
        (dim_location.country_code == df_users.country_code)
        & (dim_location.city == df_users.country_city),
        "left",
    )

    df_users = df_users.join(
        dim_device,
        (dim_device.device_info == df_users.device_info)
        & (dim_device.platform == df_users.platform),
        "left",
    )
    df_users = df_users.join(
        dim_language,
        (dim_language.language_code == df_users.language_code),
        "left",
    )

    df_users = df_users.withColumnRenamed("user_id", "user_uuid")

    df_users_joined = df_users.join(
        dim_user, dim_user.user_uuid == df_users.user_uuid, "left"
    )

    df_users_joined = df_users_joined.drop(
        dim_user.user_uuid,
        dim_user.location_id,
        dim_user.device_id,
        dim_user.language_id,
    )

    df_users_joined = df_users_joined.withColumn(
        "install_day_filter",
        F.when(
            (F.col("install_day").cast("int") > 20241101)
            & (
                F.col("install_day").cast("int")
                <= F.date_format(F.current_timestamp(), "yyyyMMdd").cast("int")
            ),
            F.col("install_day"),
        ).otherwise(F.lit(19700101)),
    )

    new_users = df_users_joined.filter(dim_user.user_id.isNull()).select(
        F.col("user_uuid"),
        F.col("device_id"),
        F.col("install_day_filter").alias("registration_date_id").cast("int"),
        F.col("location_id"),
        F.col("language_id"),
    )

    write_to_postgres(new_users, f"{game_schema}.dim_user")


def update_dim_level(df: DataFrame, dim_level: DataFrame, game_schema: str) -> None:
    columns_to_check = ["mode"]
    # existing_columns = [col for col in columns_to_check if col in df.columns]
    for col in columns_to_check:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))

    df = df.filter(F.col("level").isNotNull())
    df_levels = (
        df.select("level", "mode").distinct()
        # .withColumnRenamed("appId", "game_id")
        # .withColumnRenamed("level", "level_number")
    )
    dim_level_renamed = dim_level.withColumnRenamed("mode", "dim_mode")

    df_levels_joined = df_levels.join(
        dim_level_renamed,
        (dim_level_renamed.level_number == F.col("level"))
        & (
            (dim_level_renamed.dim_mode == F.col("mode"))
            | (dim_level_renamed.dim_mode.isNull() & F.col("mode").isNull())
        ),
        "left",
    )
    df_levels_joined = df_levels_joined.drop("dim_mode")

    new_levels = (
        df_levels_joined.filter(dim_level.level_id.isNull()).select(
            F.col("level").cast("int").alias("level_number"),
            F.col("mode").cast("int"),
        )
        # .distinct()
    )
    # new_levels.show(50, truncate=False)
    write_to_postgres(new_levels, f"{game_schema}.dim_level")


def update_dim_game_version(
    df: DataFrame, dim_game_version: DataFrame, dim_game: DataFrame, game_schema: str
) -> None:
    df_versions = df.select("app_name", "app_version", "platform").distinct()

    df_versions = df_versions.withColumn(
        "platform_id",
        F.when(F.upper(F.col("platform")) == "ANDROID", F.lit(2)).otherwise(F.lit(1)),
    )

    df_version_game = df_versions.join(
        dim_game,
        (dim_game.game_name == df_versions.app_name)
        & (dim_game.platform_id == df_versions.platform_id),
        "left",
    )

    df_version_game = df_version_game.drop(
        dim_game.genre_id,
        dim_game.platform_id,
        dim_game.package_name,
        dim_game.game_name,
        dim_game.start_date_id,
    )

    df_versions_joined = df_version_game.join(
        dim_game_version,
        (dim_game_version.game_id == df_version_game.game_id)
        & (dim_game_version.version_number == df_version_game.app_version),
        "left",
    )

    df_versions_joined = df_versions_joined.drop(
        dim_game_version.game_id,
        dim_game_version.version_number,
        dim_game_version.start_date_id,
    )

    new_versions = (
        df_versions_joined.filter(dim_game_version.version_id.isNull())
        .select(F.col("game_id").cast("int"), "app_version")
        .withColumnRenamed("app_version", "version_number")
    )
    # new_versions.show(50, truncate=False)
    new_versions = new_versions.filter(F.col("game_id").isNotNull())
    write_to_postgres(new_versions, f"{game_schema}.dim_game_version")


def update_dim_ad(df: DataFrame, dim_ad: DataFrame) -> None:
    df_ads = df.select("ad_format", "ad_platform", "ad_network").distinct()
    df_ads_joined = df_ads.join(
        dim_ad,
        (dim_ad.ad_format == df_ads.ad_format)
        & (dim_ad.ad_platform == df_ads.ad_platform)
        & (dim_ad.ad_network == df_ads.ad_network),
        "left",
    )
    df_ads_joined = df_ads_joined.drop(
        dim_ad.ad_format, dim_ad.ad_platform, dim_ad.ad_network
    )
    new_ads = df_ads_joined.filter(dim_ad.ad_id.isNull()).select(
        F.col("ad_format"), F.col("ad_platform"), F.col("ad_network")
    )

    write_to_postgres(new_ads, common_table["dim_ad"])


def update_dim_item(df: DataFrame, dim_item: DataFrame, game_schema: str) -> None:
    df_items = df.select("item_type_id", "resource_name").distinct()
    df_items_joined = df_items.join(
        dim_item,
        (dim_item.item_type_id == df_items.item_type_id)
        & (dim_item.item_name == df_items.resource_name),
        "left",
    )
    df_items_joined = df_items_joined.drop(dim_item.item_type_id)

    new_items = (
        df_items_joined.filter(dim_item.item_id.isNull())
        .select(F.col("item_type_id"), F.col("resource_name"))
        .withColumnRenamed("resource_name", "item_name")
    )
    # new_items.show(10, truncate=False)
    write_to_postgres(new_items, f"{game_schema}.dim_item")


def update_dim_item_type(
    df: DataFrame, dim_item_type: DataFrame, game_schema: str
) -> None:
    df_item_types = df.select("resource_type").distinct()
    df_items_joined = df_item_types.join(
        dim_item_type,
        dim_item_type.item_type_name == F.col("resource_type"),
        "left",
    )

    new_item_types = (
        df_items_joined.filter(dim_item_type.item_type_id.isNull())
        .select(F.col("resource_type"))
        .withColumnRenamed("resource_type", "item_type_name")
    )
    write_to_postgres(new_item_types, f"{game_schema}.dim_item_type")


def update_dim_package(df: DataFrame, dim_package: DataFrame, game_schema: str) -> None:
    df_packages = df.select("pack_name").distinct()
    # df_packages.show(10, truncate=False)

    df_packages_joined = df_packages.join(
        dim_package,
        (
            (dim_package.package_name == df_packages.pack_name)
            | (dim_package.package_name.isNull() & df_packages.pack_name.isNull())
        ),
        "left",
    )
    df_packages_joined = df_packages_joined.drop("package_name")

    new_packages = (
        df_packages_joined.filter(dim_package.package_id.isNull())
        .select(F.col("pack_name"))
        .withColumnRenamed("pack_name", "package_name")
    )

    # new_packages.show(10, truncate=False)
    write_to_postgres(new_packages, f"{game_schema}.dim_package")


def update_show_type(df: DataFrame, dim_show_type: DataFrame, game_schema: str) -> None:
    df_show_type = df.select("show_type").distinct()

    df_show_type_joined = df_show_type.join(
        dim_show_type, (dim_show_type.show_type == df_show_type.show_type), "left"
    )
    df_show_type_joined = df_show_type_joined.drop(dim_show_type.show_type)

    new_show_type = df_show_type_joined.filter(
        dim_show_type.show_type_id.isNull()
    ).select(F.col("show_type"))
    write_to_postgres(new_show_type, f"{game_schema}.dim_show_type")


def update_dim_game_position(
    df: DataFrame, dim_game_position: DataFrame, game_schema: str
) -> None:
    df_game_position = df.select("position").distinct()

    df_game_position_joined = df_game_position.join(
        dim_game_position,
        (dim_game_position.position == df_game_position.position),
        "left",
    )
    df_game_position_joined = df_game_position_joined.drop(dim_game_position.position)

    new_game_position = df_game_position_joined.filter(
        dim_game_position.game_position_id.isNull()
    ).select(F.col("position"))

    write_to_postgres(new_game_position, f"{game_schema}.dim_game_position")


def update_dim_location(df: DataFrame, dim_location: DataFrame) -> None:
    def utf8_to_unicode(s):
        if s is None:
            return None
        return s.encode("latin-1", errors="replace").decode("utf-8")

    # df = df.filter(F.col("country_city").isNotNull())
    df_locations = (
        df.select("country_name", "country_code", "country_city")
        .distinct()
        .withColumnRenamed("country_name", "df_country_name")
        .withColumnRenamed("country_code", "df_country_code")
        .withColumnRenamed("country_city", "df_city")
    )
    df_locations_joined = df_locations.join(
        dim_location,
        (dim_location.country_code == F.col("df_country_code"))
        & (dim_location.city == F.col("df_city")),
        "left",
    )

    new_locations = df_locations_joined.filter(
        dim_location.location_id.isNull()
    ).select(F.col("df_country_name"), F.col("df_country_code"), F.col("df_city"))

    # decode_udf = F.udf(utf8_to_unicode, T.StringType())
    # new_locations = new_locations.withColumn("city", decode_udf("df_city")).withColumn(
    #     "country", decode_udf("df_country_name")
    # )
    # new_locations = new_locations.drop("df_city", "df_country_name")

    new_locations = (
        new_locations.withColumnRenamed("df_country_name", "country")
        .withColumnRenamed("df_country_code", "country_code")
        .withColumnRenamed("df_city", "city")
    )

    # new_locations.show(10, truncate=False)
    write_to_postgres(new_locations, common_table["dim_location"])


def update_dim_device(df: DataFrame, dim_device: DataFrame) -> None:
    df_devices = df.select("device_info", "platform").distinct()

    df_devices_joined = df_devices.join(
        dim_device,
        (dim_device.device_info == df_devices.device_info)
        & (dim_device.platform == df_devices.platform),
        "left",
    )
    df_devices_joined = df_devices_joined.drop(
        dim_device.device_info, dim_device.platform
    )

    new_device = df_devices_joined.filter(dim_device.device_id.isNull()).select(
        F.col("device_info"), F.col("platform")
    )

    write_to_postgres(new_device, common_table["dim_device"])


def update_dim_game(df: DataFrame, dim_game: DataFrame) -> None:
    df_games = df.select("app_name", "platform").distinct()
    df_games = df_games.withColumn(
        "platform_id",
        F.when(F.upper(F.col("platform")) == "ANDROID", F.lit(2)).otherwise(F.lit(1)),
    )

    df_games_joined = df_games.join(
        dim_game,
        (dim_game.game_name == df_games.app_name)
        & (dim_game.platform_id == df_games.platform_id),
        "left",
    )
    df_games_joined = df_games_joined.drop(dim_game.game_name, dim_game.platform_id)

    new_games = (
        df_games_joined.filter(dim_game.game_id.isNull())
        .select("platform_id", "app_name")
        .withColumnRenamed("app_name", "game_name")
    )
    write_to_postgres(new_games, common_table["dim_game"])


def update_dimensions(spark, game_schema: str, df: DataFrame) -> None:
    dim_user = read_from_postgres(spark, f"{game_schema}.dim_user")
    dim_game_version = read_from_postgres(spark, f"{game_schema}.dim_game_version")
    dim_level = read_from_postgres(spark, f"{game_schema}.dim_level")
    dim_location = read_from_postgres(spark, common_table["dim_location"])
    dim_device = read_from_postgres(spark, common_table["dim_device"])
    dim_game = read_from_postgres(spark, common_table["dim_game"])

    update_dim_game(df, dim_game)
    update_dim_location(df, dim_location)
    update_dim_device(df, dim_device)
    update_dim_user(spark, df, dim_user, game_schema)
    update_dim_game_version(df, dim_game_version, dim_game, game_schema)
    update_dim_level(df, dim_level, game_schema)


def update_dim_movie_type(
    df: DataFrame, dim_movie_type: DataFrame, schema: str
) -> None:
    df_movies = df.select("movie_type").distinct()
    df_movies_join = df_movies.join(
        dim_movie_type,
        dim_movie_type.movie_type_name == df_movies.movie_type,
        "left",
    )
    df_new_movie_type = (
        df_movies_join.filter(dim_movie_type.movie_type_id.isNull())
        .select("movie_type")
        .withColumnRenamed("movie_type", "movie_type_name")
    )
    write_to_postgres(df_new_movie_type, f"{schema}.dim_movie_type")


def update_dim_api_type(df: DataFrame, dim_api_type: DataFrame, schema: str) -> None:
    df_api_call = df.select("api_type_name").distinct().alias("df_api_call")
    dim_api_type = dim_api_type.alias("dim_api_type")  # Đặt alias cho bảng

    df_api_call_join = df_api_call.join(
        dim_api_type,
        F.col("dim_api_type.api_type_name") == F.col("df_api_call.api_type_name"),
        "left",
    )

    df_new_api_call_type = df_api_call_join.filter(
        F.col("dim_api_type.api_type_id").isNull()
    ).select(
        F.col("df_api_call.api_type_name").alias("api_type_name")
    )  # Chỉ giữ cột từ df_api_call

    df_new_api_call_type = df_new_api_call_type.dropDuplicates(["api_type_name"])

    # Debug kiểm tra dữ liệu

    write_to_postgres(df_new_api_call_type, f"{schema}.dim_api_type")


def update_dim_movie_place(
    df: DataFrame, dim_movie_place: DataFrame, schema: str
) -> None:
    df_init_movie = df.select("movie_place").distinct().alias("df_init_movie")
    dim_movie_place = dim_movie_place.alias("dim_movie_place")  # Đặt alias cho bảng

    df_movie_place_join = df_init_movie.join(
        dim_movie_place,
        F.col("dim_movie_place.movie_place") == F.col("df_init_movie.movie_place"),
        "left",
    )

    df_new_movie_place = df_movie_place_join.filter(
        F.col("dim_movie_place.movie_place_id").isNull()
    ).select(
        F.col("df_init_movie.movie_place").alias("movie_place")
    )  # Chỉ giữ cột từ df_api_call

    df_new_movie_place = df_new_movie_place.dropDuplicates(["movie_place"])

    # Debug kiểm tra dữ liệu

    write_to_postgres(df_new_movie_place, f"{schema}.dim_movie_place")

def update_dim_email(
    df: DataFrame, dim_email: DataFrame, schema: str
) -> None:
    df_email = df.select("email").distinct()

    df_email_joined = df_email.join(
        dim_email,
        (
            (dim_email.email == df_email.email)
            | (dim_email.email.isNull() & df_email.email.isNull())
        ),
        "left",
    )

    new_emails = (
        df_email_joined.filter(dim_email.email_id.isNull())
        .select(F.col("email"))
    )

    write_to_postgres(new_emails, f"{schema}.dim_email")
    
def update_dim_feedback(
    df: DataFrame, dim_feedback: DataFrame, schema: str
) -> None:
    df_feedback = df.select("feedback").distinct()

    df_feedback_joined = df_feedback.join(
        dim_feedback,
        (
            (dim_feedback.feedback == df_feedback.feedback)
            | (dim_feedback.feedback.isNull() & df_feedback.feedback.isNull())
        ),
        "left",
    )

    new_feedbacks = (
        df_feedback_joined.filter(dim_feedback.feedback_id.isNull())
        .select(F.col("feedback"))
    )

    write_to_postgres(new_feedbacks, f"{schema}.dim_feedback")