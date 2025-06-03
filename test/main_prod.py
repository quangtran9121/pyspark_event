from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from conf.config import config_app
from events.base_event import EVENT_SCHEMAS
from common.dimension_updater import update_dimensions
from common.broadcast_loader import load_and_broadcast_dimensions
from common.utils import get_schema_for_topic, flatten_raw_df
from processors import (
    base_processor,
    ad_processor,
    iap_processor,
    level_processor,
    resource_processor,
    player_change_processor,
    session_start_processor
)
from common.logger import get_logger

 
def main():
    logger = get_logger("prod", config_app.path.logs_folder)
    spark = (
        SparkSession.builder.appName(config_app.event.app_name)
        .config(
            "spark.jars.packages", 
            config_app.event.spark_jars_packages,
        )
        .config(
            "spark.jars", config_app.path.postgres_resource_path,
        )
        .getOrCreate()
    )

    # Read from Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config_app.event.kafka_bootstrap_servers)
        .option("subscribePattern", config_app.event.kafka_topic_pattern)
        .option("kafka.security.protocol", config_app.event.kafka_security_protocol)
        .option("kafka.sasl.mechanism", config_app.event.kafka_sasl_mechanism)
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config_app.env.KAFKA_USER}" password="{config_app.env.KAFKA_PASSWORD}";',
        )
        .option("maxOffsetsPerTrigger", config_app.event.num_process)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false") 
        .load()
    )

    parsed_df = kafka_df.selectExpr(
        "CAST(topic AS STRING) as kafka_topic",
        "CAST(value AS STRING) as message"
    )

    def foreach_batch_function(batch_df: DataFrame, batch_id: int) -> None:
        logger.info(f"Processing batch {batch_id}")

        if batch_df.count() == 0:
            return
        
        topics = [r["kafka_topic"] for r in batch_df.select("kafka_topic").distinct().collect()]
        
        for topic in topics:
            if "test" in topic:
                continue
            df_topic = batch_df.filter(F.col("kafka_topic") == topic)
    
            game_schema = get_schema_for_topic(topic)

            json_df = df_topic.withColumn(
                "json_data", F.from_json(F.col("message"), EVENT_SCHEMAS["message"])
            ).select("kafka_topic", "json_data.*")
            
            flatten_df = flatten_raw_df(json_df)
            flatten_df = flatten_df.filter(flatten_df.user_id.isNotNull())
            
            broadcast_vars = load_and_broadcast_dimensions(spark)
            update_dimensions(spark, game_schema, flatten_df, broadcast_vars)

            event_names = json_df.select(F.col("eventName")).distinct().collect()
            for row in event_names:
                event_name = row["eventName"]
                processor_class = get_processor_class(event_name)
                if processor_class:
                    logger.info(f"Get processor for event: {event_name}")
                    processor = processor_class(spark, broadcast_vars, game_schema)
                    event_df = flatten_df.filter(F.col("eventName") == event_name)
                    processor.process(event_df)
                else:
                    logger.warning(f"No processor found for event: {event_name}")


    query = (
        parsed_df
            .repartition(config_app.event.num_partition)
            .writeStream
            .foreachBatch(foreach_batch_function)
            .option("checkpointLocation", config_app.path.checkpoint_location_prod)
            # .trigger(processingTime=f"{settings.BATCH_DURATION} seconds")
            .start()
    )

    query.awaitTermination()


def get_processor_class(event_name: str) -> base_processor.BaseProcessor:
    processors = {
        "iap_show": iap_processor.IAPProcessor,
        "iap_click": iap_processor.IAPProcessor,
        "iap_purchase": iap_processor.IAPProcessor,
        "ad_load": ad_processor.AdProcessor,
        "ad_show": ad_processor.AdProcessor,
        "level_play": level_processor.LevelProcessor,
        "level_end": level_processor.LevelProcessor,
        "level_second_chance": level_processor.LevelProcessor,
        "resource_sink": resource_processor.ResourceProcessor,
        "resource_earn": resource_processor.ResourceProcessor,
        "player_change": player_change_processor.PlayerChageProcessor, 
        # "session_start": session_start_processor.SessionStartProcessor,
        # "first_open": session_start_processor.SessionStartProcessor,
    }

    return processors.get(event_name)


if __name__ == "__main__":
    main()
