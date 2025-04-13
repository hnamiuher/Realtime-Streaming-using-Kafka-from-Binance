import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("✅ Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.crypto_data (
        symbol TEXT PRIMARY KEY,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume FLOAT,
        time BIGINT
    );
    """)
    print("✅ Table created successfully!")


def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.local.dir", "C:\\test_khoaluan\\Realtime-Streaming-using-Kafka-from-Binance\\tmp") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logging.info("✅ Spark connection created.")
        return spark
    except Exception as e:
        logging.error(f"❌ Failed to create Spark session: {e}")
        return None


def connect_to_kafka(spark_conn):
    try:
        kafka_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "crypto_kline") \
            .option("startingOffsets", "earliest") \
            .load()
        kafka_df.printSchema()
        logging.info("✅ Connected to Kafka topic.")
        return kafka_df
    except Exception as e:
        logging.error(f"❌ Failed to connect to Kafka: {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("open", FloatType(), False),
        StructField("high", FloatType(), False),
        StructField("low", FloatType(), False),
        StructField("close", FloatType(), False),
        StructField("volume", FloatType(), False),
        StructField("time", LongType(), False)
    ])

    return spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")


def write_to_cassandra(batch_df, batch_id):
    try:
        print(f"🔁 Batch {batch_id} received:")
        batch_df.show(truncate=False)

        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="crypto_data", keyspace="spark_streams") \
            .save()

        logging.info(f"✅ Batch {batch_id} written to Cassandra.")
    except Exception as e:
        logging.error(f"❌ Error writing batch {batch_id} to Cassandra: {e}")


def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"❌ Failed to connect to Cassandra: {e}")
        return None


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn:
        spark_df = connect_to_kafka(spark_conn)

        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)

            session = create_cassandra_connection()
            if session:
                create_keyspace(session)
                create_table(session)

                print("💡 Checkpoint path: file:///C:/test_khoaluan/Realtime-Streaming-using-Kafka-from-Binance/tmp")

                try:
                    streaming_query = selection_df.writeStream \
                        .foreachBatch(write_to_cassandra) \
                        .outputMode("append") \
                        .option("checkpointLocation", "file:///C:/test_khoaluan/Realtime-Streaming-using-Kafka-from-Binance/tmp") \
                        .start()

                    logging.info("🚀 Streaming query started.")
                    streaming_query.awaitTermination()
                except Exception as e:
                    logging.error(f"❌ Error during streaming: {e}")
