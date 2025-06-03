from pyspark.sql import DataFrame, SparkSession
from conf.config import config_app

jdbc_config = {
    "url": config_app.env.POSTGRES_URL,
    "user": config_app.env.POSTGRES_USER,
    "password": config_app.env.POSTGRES_PASSWORD,
    "driver": config_app.event.postgres_driver,
}


def write_to_postgres(df: DataFrame, table_name: str) -> None:
    """
    Writes the DataFrame to a PostgreSQL table.
    """
    df.write.format("jdbc").options(**jdbc_config).option("dbtable", table_name).mode(
        "append"
    ).save()


def overwrite_to_postgres(df: DataFrame, table_name: str) -> None:
    """
    Writes the DataFrame to a PostgreSQL table.
    """
    df.write.format("jdbc").options(**jdbc_config).option("dbtable", table_name).mode(
        "overwrite"
    ).save()


def read_from_postgres(spark: SparkSession, table_name: str) -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .options(**jdbc_config)
        .option("dbtable", table_name)
        .load()
    )

    return df
