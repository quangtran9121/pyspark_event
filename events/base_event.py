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

# Map event names to schemas
EVENT_SCHEMAS = {
    "message": message_schema,
}
