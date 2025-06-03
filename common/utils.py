from pyspark.sql import DataFrame, functions as F

from common.clickhouse_writer import write_to_clickhouse


def get_schema_for_topic(topic: str) -> str:
    schema = "game" + topic[14:] + ".schema"
    schema = schema.replace(".", "_")

    return schema


def raw_df_to_db(df: DataFrame, topic: str, batch_id: int) -> None:
    if "test" in topic:
        env = "stag"
        df_to_click_house(df, topic, env, batch_id)
    else:
        env = "prod"
        df_to_click_house(df, topic, env, batch_id)


def df_to_click_house(df: DataFrame, topic: str, env: str, batch_id: int) -> None:
    package_name = topic[11:]
    table_name = package_name.replace(".", "_")

    df_out = transform_to_clickhouse(df, batch_id)
    write_to_clickhouse(df_out, table_name, env)


def transform_to_clickhouse(df: DataFrame, batch_id: int) -> DataFrame:
    df = (
        df.withColumn("eventData_json", F.to_json(F.col("eventData")))
        .withColumn("userProperty_json", F.to_json(F.col("userProperty")))
        .withColumn("user_id", F.get_json_object("userProperty_json", "$.user_id"))
        .withColumn(
            "app_version", F.get_json_object("userProperty_json", "$.app_version")
        )
        .withColumn("batch_id", F.lit(batch_id))
    )

    df = df.na.fill(
        {"user_id": "unknown", "sequenceId": 9999999, "eventName": "unknown", "_ts": 1}
    )

    df_out = df.select(
        F.col("sequenceId").alias("sequence_id").cast("long"),
        F.col("eventName").alias("event_name"),
        F.col("eventData_json").alias("event_data"),
        F.col("userProperty_json").alias("user_properties"),
        F.col("user_id"),
        F.col("app_version"),
        F.col("_ts"),
        F.col("batch_id"),
    )

    return df_out


def flatten_raw_df(raw_df: DataFrame, batch_id: int) -> DataFrame:
    df = (
        raw_df.withColumn("eventData_json", F.to_json(F.col("eventData")))
        .withColumn("userProperty_json", F.to_json(F.col("userProperty")))
        .withColumn("timestamp", (F.col("_ts") / 1000).cast("timestamp"))
    )

    df = (
        df.withColumn("sequence_id", F.col("sequenceId").cast("int"))
        .withColumn("user_id", F.get_json_object("userProperty_json", "$.user_id"))
        .withColumn("app_name", F.get_json_object("userProperty_json", "$.app_name"))
        .withColumn(
            "app_version", F.get_json_object("userProperty_json", "$.app_version")
        )
        .withColumn(
            "country_city", F.get_json_object("userProperty_json", "$.country_city")
        )
        .withColumn(
            "country_code", F.get_json_object("userProperty_json", "$.country_code")
        )
        .withColumn(
            "country_name", F.get_json_object("userProperty_json", "$.country_name")
        )
        .withColumn("language", F.get_json_object("userProperty_json", "$.language"))
        .withColumn("platform", F.get_json_object("userProperty_json", "$.platform"))
        .withColumn(
            "event_date", F.get_json_object("userProperty_json", "$.event_date")
        )
        .withColumn(
            "session_number", F.get_json_object("userProperty_json", "$.session_number")
        )
        .withColumn(
            "install_day", F.get_json_object("userProperty_json", "$.install_day")
        )
        .withColumn(
            "device_info", F.get_json_object("userProperty_json", "$.device_info")
        )
        .withColumn(
            "session_id", F.get_json_object("userProperty_json", "$.session_id")
        )
        .withColumn("level", F.get_json_object("eventData_json", "$.level"))
        .withColumn("mode", F.get_json_object("eventData_json", "$.mode"))
        .withColumn("batch_id", F.lit(batch_id).cast("int"))
    )

    return df
