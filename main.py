import signal
import time
import sys
import threading
from pyspark.sql import SparkSession, DataFrame, functions as F
from conf.config import config_app
from events.base_event import EVENT_SCHEMAS
from common.dimension_updater import update_dimensions
from common.broadcast_loader import load_and_broadcast_dimensions
from common.utils import get_schema_for_topic, flatten_raw_df, raw_df_to_db
from processors import (
    base_processor,
    ad_processor,
    iap_processor,
    level_processor,
    movie_info_processor,
    resource_processor,
    player_change_processor,
    screen_processor,
    behavior_processor,
    session_processor,
    api_call_processor,
    init_movie_processor,
    movie_click_processor,
    iap_processor_drama,
    purchase_fail_report_processor,
    user_login_processor,
    feedback_processor,
)
from common.logger import get_logger

# Global flags
is_processing = False
shutdown_requested = False
current_processing_event = None
current_processing_topic = None
query = None
logger = None
spark = None

# Lock for shutdown operations
shutdown_lock = threading.RLock()


def safe_shutdown():
    """Safely shut down all resources without depending on Py4J calls that might fail"""
    global shutdown_requested, is_processing, logger, query, spark

    with shutdown_lock:
        # Only run the shutdown sequence once
        if getattr(safe_shutdown, "already_called", False):
            return
        safe_shutdown.already_called = True

    logger.info("Initiating safe shutdown sequence...")

    # Set the flag to prevent new batch processing
    shutdown_requested = True

    # Wait for current processing to complete with a timeout
    if is_processing:
        logger.info("Waiting for current processing to complete (max 10 minutes)...")
        wait_start = time.time()
        while is_processing and (time.time() - wait_start) < 600:  # 10 minute timeout
            time.sleep(5)
            logger.info(f"Still waiting... ({int(time.time() - wait_start)}s elapsed)")

    logger.info("Stopping Spark streaming query...")
    try:
        # Try to stop the query gracefully, but don't depend on it working
        if query is not None:
            query.stop()
    except Exception as e:
        logger.error(
            f"Error stopping query (this may be normal during shutdown): {str(e)}"
        )

    logger.info("Stopping Spark session...")
    try:
        # Try to stop Spark session gracefully, but don't depend on it
        if spark is not None:
            spark.stop()
    except Exception as e:
        logger.error(
            f"Error stopping Spark session (this may be normal during shutdown): {str(e)}"
        )

    logger.info("Shutdown sequence completed. Exiting...")
    # Force exit after cleanup
    sys.exit(0)


def handle_shutdown(signum, frame):
    """Signal handler that triggers safe shutdown"""
    global logger

    # Log which signal we received
    signal_name = (
        signal.Signals(signum).name
        if hasattr(signal, "Signals")
        else f"signal {signum}"
    )
    if logger:
        logger.info(f"Received {signal_name}. Initiating shutdown...")

    # Start graceful shutdown in a separate thread to avoid Py4J reentrant call issues
    threading.Thread(target=safe_shutdown, daemon=True).start()


def main():
    global \
        is_processing, \
        query, \
        logger, \
        spark, \
        shutdown_requested, \
        current_processing_event, \
        current_processing_topic

    # Initialize logger early
    logger = get_logger("prod", config_app.path.logs_folder)
    logger.info("Starting application...")

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)  # Handles Ctrl+C

    # Create Spark session
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(config_app.event.app_name)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "com.clickhouse:clickhouse-jdbc:0.7.2",
        )
        .config(
            "spark.jars",
            config_app.path.postgres_resource_path,
        )
        # Add this configuration to improve shutdown behavior
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
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
    # kafka_df = (
    #     spark.readStream.format("kafka")
    #     .option("kafka.bootstrap.servers", config_app.event.kafka_bootstrap_servers)
    #     .option(
    #         "subscribe", "topic_game.com.xgame.drama.max"
    #     )  # Specify a single topic here
    #     .option("kafka.security.protocol", config_app.event.kafka_security_protocol)
    #     .option("kafka.sasl.mechanism", config_app.event.kafka_sasl_mechanism)
    #     .option(
    #         "kafka.sasl.jaas.config",
    #         f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config_app.env.KAFKA_USER}" password="{config_app.env.KAFKA_PASSWORD}";',
    #     )
    #     .option("maxOffsetsPerTrigger", config_app.event.num_process)
    #     .option("startingOffsets", "earliest")
    #     .option("failOnDataLoss", "false")
    #     .load()
    # )

    parsed_df = kafka_df.selectExpr(
        "CAST(topic AS STRING) as kafka_topic", "CAST(value AS STRING) as message"
    )

    def foreach_batch_function(batch_df: DataFrame, batch_id: int) -> None:
        global \
            is_processing, \
            shutdown_requested, \
            current_processing_event, \
            current_processing_topic

        # Skip processing if shutdown is already requested
        if shutdown_requested:
            logger.info(f"Shutdown requested. Skipping batch {batch_id}.")
            return

        logger.info(f"Starting to process batch {batch_id}")
        is_processing = True

        try:
            # Check again for shutdown request after potentially long count operation
            if shutdown_requested:
                logger.info(
                    f"Shutdown requested during batch {batch_id} preparation. Skipping."
                )
                return

            if batch_df.count() == 0:
                logger.info(f"Batch {batch_id} is empty. Skipping.")
                return

            topics = [
                r["kafka_topic"]
                for r in batch_df.select("kafka_topic").distinct().collect()
            ]

            for topic in topics:
                if topic != "topic_game.com.xgame.drama.max":
                    continue
                # Check for shutdown request before processing each topic
                if shutdown_requested:
                    logger.info(
                        f"Shutdown requested. Skipping remaining topics in batch {batch_id}."
                    )
                    break

                if "test" in topic:
                    continue

                current_processing_topic = topic
                logger.info(f"Processing topic: {topic} (batch {batch_id})")

                try:
                    df_topic = batch_df.filter(F.col("kafka_topic") == topic)
                    game_schema = get_schema_for_topic(topic)
                    json_df = df_topic.withColumn(
                        "json_data",
                        F.from_json(F.col("message"), EVENT_SCHEMAS["message"]),
                    ).select("kafka_topic", "json_data.*")

                    logger.info(
                        f"Writing raw events to ClickHouse for topic {topic}..."
                    )
                    raw_df_to_db(json_df, topic, batch_id)
                    logger.info(
                        f"Finished writing raw events to ClickHouse for topic {topic}"
                    )

                    flatten_df = flatten_raw_df(json_df, batch_id)
                    flatten_df = flatten_df.filter(flatten_df.user_id.isNotNull())

                    broadcast_vars = load_and_broadcast_dimensions(spark)
                    update_dimensions(spark, game_schema, flatten_df)

                    event_names = (
                        json_df.select(F.col("eventName")).distinct().collect()
                    )

                    logger.info(
                        f"Event names in topic {topic}: {[row['eventName'] for row in event_names]}"
                    )  # Log event names

                    for row in event_names:
                        event_name = row["eventName"]
                        if (
                            topic == "topic_game.com.xgame.drama.max"
                            and event_name == "iap_purchase"
                        ):
                            current_processing_event = event_name
                            processor_class = iap_processor_drama.IAPProcessor
                            if processor_class:
                                logger.info(
                                    f"Processing event: {event_name}"
                                )  # Log before processing event
                                try:
                                    processor = processor_class(
                                        spark, broadcast_vars, game_schema, logger
                                    )
                                    event_df = flatten_df.filter(
                                        F.col("eventName") == event_name
                                    )
                                    processor.process(event_df)
                                    logger.info(
                                        f"Completed processing event: {event_name}"
                                    )  # Log after processing event
                                except Exception as e:
                                    logger.error(
                                        f"Error processing event {event_name}: {str(e)}"
                                    )
                            else:
                                logger.warning(
                                    f"No processor found for event: {event_name}"
                                )
                            current_processing_event = None
                        else:
                            current_processing_event = event_name
                            processor_class = get_processor_class(event_name)

                            if processor_class:
                                logger.info(
                                    f"Processing event: {event_name}"
                                )  # Log before processing event
                                try:
                                    processor = processor_class(
                                        spark, broadcast_vars, game_schema, logger
                                    )
                                    event_df = flatten_df.filter(
                                        F.col("eventName") == event_name
                                    )
                                    processor.process(event_df)
                                    logger.info(
                                        f"Completed processing event: {event_name}"
                                    )  # Log after processing event
                                except Exception as e:
                                    logger.error(
                                        f"Error processing event {event_name}: {str(e)}"
                                    )
                            else:
                                logger.warning(
                                    f"No processor found for event: {event_name}"
                                )

                            current_processing_event = None
                    logger.info(f"Completed processing topic: {topic}")
                except Exception as e:
                    logger.error(f"Error processing topic {topic}: {str(e)}")
                finally:
                    current_processing_topic = None

        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
        finally:
            is_processing = False
            logger.info(f"Finished processing batch {batch_id}")

    try:
        logger.info("Starting streaming query...")
        query = (
            parsed_df.repartition(config_app.event.num_partition)
            .writeStream.foreachBatch(foreach_batch_function)
            .option("checkpointLocation", config_app.path.checkpoint_location)
            .start()
        )

        logger.info("Streaming query started. Waiting for termination...")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error in main streaming loop: {str(e)}")
        safe_shutdown()


def get_processor_class(event_name: str):
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
        "screen_play": screen_processor.ScreenProcessor,
        "screen_end": screen_processor.ScreenProcessor,
        "behavior": behavior_processor.BehaviorProcessor,
        "session_start": session_processor.SessionProcessor,
        "first_open": session_processor.SessionProcessor,
        "movie_info": movie_info_processor.MovieInfoProcessor,
        "api_call": api_call_processor.APICallProcessor,
        "init_movie": init_movie_processor.InitMovieProcessor,
        "movie_click": movie_click_processor.MovieClickProcessor,
        "login": user_login_processor.UserLoginProcessor,
        "purchase_fail_report": purchase_fail_report_processor.PurchaseFailReportProcessor,
        "feedback": feedback_processor.FeedbackProcessor,
    }

    return processors.get(event_name)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        if logger:
            logger.error(f"Unhandled exception in main: {str(e)}")
        else:
            print(f"CRITICAL ERROR: {str(e)}")

        # Try to perform safe shutdown
        safe_shutdown()
