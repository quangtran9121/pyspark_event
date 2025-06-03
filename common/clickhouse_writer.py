from pyspark.sql import DataFrame, SparkSession
from conf.config import config_app


def write_to_clickhouse(df: DataFrame, clickhouse_table: str, env: str) -> None:
    if env == "stag":
        jdbc_config = {
            "url": config_app.env.CLICKHOUSE_STAG_URL,
            "user": config_app.env.CLICKHOUSE_USER,
            "password": config_app.env.CLICKHOUSE_PASSWORD,
            "driver": config_app.event.clickhouse_driver,
        }
    else:
        jdbc_config = {
            "url": config_app.env.CLICKHOUSE_PROD_URL,
            "user": config_app.env.CLICKHOUSE_USER,
            "password": config_app.env.CLICKHOUSE_PASSWORD,
            "driver": config_app.event.clickhouse_driver,
        }
    df.write.format("jdbc").options(**jdbc_config).option(
        "dbtable", clickhouse_table
    ).mode("append").save()
