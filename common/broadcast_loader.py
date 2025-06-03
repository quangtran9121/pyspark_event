from pyspark.sql import functions as F
from common.postgres_writer import read_from_postgres
from conf.mapping_table import common_table


def load_and_broadcast_dimensions(spark) -> dict:
    dim_currency = read_from_postgres(spark, common_table["dim_currency"])
    dim_platform = read_from_postgres(spark, common_table["dim_platform"])
    dim_event_type = read_from_postgres(spark, common_table["dim_event_type"])
    dim_time = read_from_postgres(spark, common_table["dim_time"])

    return {
        "dim_currency": F.broadcast(dim_currency),
        "dim_platform": F.broadcast(dim_platform),
        "dim_event_type": F.broadcast(dim_event_type),
        "dim_time": F.broadcast(dim_time),
    }
