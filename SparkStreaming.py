import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession

# Create a local StreamingContext with two working thread and batch interval of 1 second

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

streaming_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "viewrecords") \
    .option("startingOffsets", "earliest") \
    .load()

streaming_df.createOrReplaceTempView("view")

query = spark.sql("select window, cast(value as string), count(1) c from view group by window(timestamp,'60 seconds'), value order by c desc")

q2 = query.writeStream.format("console").outputMode("complete").option("truncate",False).option("numRows",50).start()

def foreach_batch_function(batch_df, batch_id):
    # Convert the key and value columns to strings and print them
    for row in batch_df.collect():
        print("Key: {}, Value: {}, BatchId: {}".format(row['key'].decode('utf-8'), row['value'].decode('utf-8'),batch_id))

#query = streaming_df.writeStream.foreachBatch(foreach_batch_function).start()

q2.awaitTermination()
#query.awaitTermination()