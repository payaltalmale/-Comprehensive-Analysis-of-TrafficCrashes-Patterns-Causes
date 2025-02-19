from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, window, count, avg, sum, max, col, from_json, substring, when, desc, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Kafka Configuration
KAFKA_TOPIC = "traffic-data-stream"
KAFKA_BROKER = "localhost:9092"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka-Spark-Streaming-Cassandra") \
    .master("local[*]") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.cassandra.connection.host", "192.168.43.50") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
    .getOrCreate()

# Define Schema for Incoming Data
traffic_schema = StructType([
    StructField("CRASH_RECORD_ID", StringType(), True),
    StructField("CRASH_DATE", StringType(), True),
    StructField("POSTED_SPEED_LIMIT", IntegerType(), True),
    StructField("TRAFFIC_CONTROL_DEVICE", StringType(), True),
    StructField("DEVICE_CONDITION", StringType(), True),
    StructField("WEATHER_CONDITION", StringType(), True),
    StructField("LIGHTING_CONDITION", StringType(), True),
    StructField("FIRST_CRASH_TYPE", StringType(), True),
    StructField("TRAFFICWAY_TYPE", StringType(), True),
    StructField("ALIGNMENT", StringType(), True),
    StructField("ROADWAY_SURFACE_COND", StringType(), True),
    StructField("ROAD_DEFECT", StringType(), True),
    StructField("REPORT_TYPE", StringType(), True),
    StructField("CRASH_TYPE", StringType(), True),
    StructField("INTERSECTION_RELATED_I", StringType(), True),
    StructField("NOT_RIGHT_OF_WAY_I", StringType(), True),
    StructField("HIT_AND_RUN_I", StringType(), True),
    StructField("DAMAGE", StringType(), True),
    StructField("DATE_POLICE_NOTIFIED", StringType(), True),
    StructField("PRIM_CONTRIBUTORY_CAUSE", StringType(), True),
    StructField("SEC_CONTRIBUTORY_CAUSE", StringType(), True),
    StructField("STREET_NO", IntegerType(), True),
    StructField("STREET_DIRECTION", StringType(), True),
    StructField("STREET_NAME", StringType(), True),
    StructField("BEAT_OF_OCCURRENCE", IntegerType(), True),
    StructField("NUM_UNITS", IntegerType(), True),
    StructField("MOST_SEVERE_INJURY", StringType(), True),
    StructField("INJURIES_TOTAL", IntegerType(), True),
    StructField("INJURIES_FATAL", IntegerType(), True),
    StructField("INJURIES_INCAPACITATING", IntegerType(), True),
    StructField("INJURIES_NON_INCAPACITATING", IntegerType(), True),
    StructField("INJURIES_REPORTED_NOT_EVIDENT", IntegerType(), True),
    StructField("INJURIES_NO_INDICATION", IntegerType(), True),
    StructField("INJURIES_UNKNOWN", IntegerType(), True),
    StructField("CRASH_HOUR", IntegerType(), True),
    StructField("CRASH_DAY_OF_WEEK", IntegerType(), True),
    StructField("CRASH_MONTH", IntegerType(), True),
    StructField("LATITUDE", FloatType(), True),
    StructField("LONGITUDE", FloatType(), True),
    StructField("Timestamp", StringType(), True),  #
])

# Read data from Kafka
traffic_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value from binary to string and parse JSON
traffic_stream = traffic_stream.selectExpr("CAST(value AS STRING)")
traffic_stream = traffic_stream.withColumn("data", from_json(col("value"), traffic_schema)).select("data.*")

# Transformation: Drop unnecessary columns
transformed_df1 = traffic_stream.drop("INTERSECTION_RELATED_I") \
    .drop("NOT_RIGHT_OF_WAY_I") \
    .drop("DATE_POLICE_NOTIFIED") \
    .drop("BEAT_OF_OCCURRENCE") \
    .drop("SEC_CONTRIBUTORY_CAUSE") \
    .drop("INJURIES_NON_INCAPACITATING") \
    .drop("INJURIES_REPORTED_NOT_EVIDENT")
# Extract year from crash date column
transformed_df2 = transformed_df1.withColumn("year", substring(transformed_df1['CRASH_DATE'], 7, 4)) \
    .drop("CRASH_DATE")

# Replace "NaN" with "N" in HIT_AND_RUN_I column
transformed_df3 = transformed_df2.withColumn("HIT_AND_RUN_I", when(transformed_df2["HIT_AND_RUN_I"] == "NaN", "N").otherwise(transformed_df2["HIT_AND_RUN_I"]))

# Compute Mode for Each Column (Assumed you computed these values from a batch DataFrame or another method)
# Here I'm assuming these values are precomputed.
mode_values = {
    "STREET_DIRECTION": "W",
    "STREET_NAME": "WESTERN AVE",
    "MOST_SEVERE_INJURY": "NO INDICATION OF INJURY",
    "INJURIES_NO_INDICATION": 2,
    "INJURIES_TOTAL": 0,
    "INJURIES_UNKNOWN": 0,
    "INJURIES_FATAL": 0,
    "INJURIES_INCAPACITATING": 0,
    "LATITUDE": 41.976201,
    "LONGITUDE": -87.905309
}

# Broadcast Mode Values
mode_values_broadcast = spark.sparkContext.broadcast(mode_values)

# Apply Mode Value in Streaming Data
df_replaced = transformed_df3
for col_name, mode_value in mode_values_broadcast.value.items():
    df_replaced = df_replaced.withColumn(
        col_name, when((col(col_name).isNull()) | (col(col_name) == "NaN"), mode_value).otherwise(col(col_name))
    )
df = df_replaced.toDF(*[c.lower() for c in  df_replaced.columns])


# **Aggregations**
avg_speed_limit = df.agg(avg("POSTED_SPEED_LIMIT").alias("avg_speed_limit"))
crash_type_count = df.groupBy("CRASH_TYPE").agg(count("*").alias("total_accidents"))
lighting_weather = df.groupBy("LIGHTING_CONDITION", "WEATHER_CONDITION").agg(count("*").alias("total_crashes"))
total_injuries = df.groupBy("INJURIES_TOTAL").agg(count("*").alias("crash_count"))
accidents_by_hour = df.groupBy("CRASH_HOUR").agg(count("*").alias("accidents_per_hour"))
most_severe_injuries = df.groupBy("MOST_SEVERE_INJURY").agg(count("*").alias("total_cases"))
crashes_by_day = df.groupBy("CRASH_DAY_OF_WEEK").agg(count("*").alias("crashes_per_day"))
fatal_injuries_ratio = df.agg((sum("INJURIES_FATAL") / sum("INJURIES_TOTAL")).alias("fatality_ratio"))

# **Window Function: Rolling crash count per hour**
rolling_crashes = df.groupBy(window(col("Timestamp"), "1 hour")).agg(count("*").alias("crashes_per_hour"))

# **Streaming Outputs**
queries = [
    avg_speed_limit.writeStream.outputMode("update").format("console").start(),
    crash_type_count.writeStream.outputMode("update").format("console").start(),
    lighting_weather.writeStream.outputMode("update").format("console").start(),
    total_injuries.writeStream.outputMode("update").format("console").start(),
    accidents_by_hour.writeStream.outputMode("update").format("console").start(),
    most_severe_injuries.writeStream.outputMode("update").format("console").start(),
    crashes_by_day.writeStream.outputMode("update").format("console").start(),
    fatal_injuries_ratio.writeStream.outputMode("update").format("console").start(),
    rolling_crashes.writeStream.outputMode("update").format("console").start()
]

# **Writing to Cassandra**


def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "traffic_data") \
        .option("table", "traffic_data_table2") \
        .mode("append") \
        .save()

query_cassandra = df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

# **Wait for termination**
for query in queries:
    query.awaitTermination()

query_cassandra.awaitTermination()
