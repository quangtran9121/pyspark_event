from pyspark.sql import types as T


message_schema = T.StructType(
    [
        T.StructField("sequenceId", T.IntegerType()),
        T.StructField("eventName", T.StringType()),
        T.StructField("eventData", T.MapType(T.StringType(), T.StringType(), True)),
        T.StructField("_ts", T.LongType()),
        T.StructField("userProperty", T.MapType(T.StringType(), T.StringType(), True)),
    ]
)


iap_schema = T.StructType(
    [
        T.StructField("position", T.StringType(), True),
        T.StructField("show_type", T.StringType(), True),
        T.StructField("pack_name", T.StringType(), True),
    ]
)

iap_purchase_schema = T.StructType(
    [
        T.StructField("position", T.StringType(), True),
        T.StructField("show_type", T.StringType(), True),
        T.StructField("pack_name", T.StringType(), True),
        T.StructField("price", T.IntegerType(), True),
        T.StructField("currency", T.IntegerType(), True),
    ]
)

ad_load_schema = T.StructType(
    [
        T.StructField("ad_format", T.StringType(), True),
        T.StructField("ad_platform", T.StringType(), True),
        T.StructField("ad_network", T.StringType(), True),
        T.StructField("position", T.StringType(), True),
        T.StructField("is_load", T.IntegerType(), True),
        T.StructField("load_time", T.IntegerType(), True),
    ]
)

ad_show_schema = T.StructType(
    [
        T.StructField("ad_format", T.StringType(), True),
        T.StructField("ad_platform", T.StringType(), True),
        T.StructField("ad_network", T.StringType(), True),
        T.StructField("position", T.StringType(), True),
        T.StructField("is_show", T.IntegerType(), True),
        T.StructField("value", T.IntegerType(), True),
    ]
)

level_play_schema = T.StructType(
    [
        T.StructField("level", T.IntegerType(), True),
        T.StructField("require_time", T.IntegerType(), True),
        T.StructField("play_type", T.StringType(), True),
        T.StructField("play_index", T.IntegerType(), True),
        T.StructField("mode", T.StringType(), True),
    ]
)

level_end_schema = T.StructType(
    [
        T.StructField("level", T.IntegerType(), True),
        T.StructField("require_time", T.IntegerType(), True),
        T.StructField("mode", T.StringType(), True),
        T.StructField("play_time", T.IntegerType(), True),
        T.StructField("play_index", T.IntegerType(), True),
        T.StructField("result", T.StringType(), True),
    ]
)

level_second_chance_schema = T.StructType(
    [
        T.StructField("bonus_type", T.StringType(), True),
        T.StructField("bonus_amount", T.IntegerType(), True),
    ]
)

resource_schema = T.StructType(
    [
        T.StructField("resource_type", T.StringType(), True),
        T.StructField("resource_name", T.StringType(), True),
        T.StructField("resource_amount", T.StringType(), True),
        T.StructField("reason", T.StringType(), True),
        T.StructField("position", T.StringType(), True),
    ]
)

player_change_schema = T.StructType(
    [
        T.StructField("change", T.StringType(), True),
        T.StructField("new_id", T.StringType(), True),
    ]
)


# Map event names to schemas
EVENT_SCHEMAS = {
    "message": message_schema,
    "iap_show": iap_schema,
    "iap_click": iap_schema,
    "iap_purchase": iap_purchase_schema,
    "ad_load": ad_load_schema,
    "ad_show": ad_show_schema,
    "level_play": level_play_schema,
    "level_end": level_end_schema,
    "level_second_chance": level_second_chance_schema,
    "resource_sink": resource_schema,
    "resource_earn": resource_schema,
    "player_change": player_change_schema,
}
