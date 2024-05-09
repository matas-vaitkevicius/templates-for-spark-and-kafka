from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

def process_logs(rdd):
    if not rdd.isEmpty():
        # Convert RDD to DataFrame
        df = spark.createDataFrame(rdd, ["log_message"])

        # Show the log messages
        df.show(truncate=False)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("LogStreamConsumer") \
    .getOrCreate()

# Initialize StreamingContext
ssc = StreamingContext(spark.sparkContext, batchDuration=10)

# Create a DStream that represents streaming data from port 8989
lines = ssc.socketTextStream("localhost", 8989)

# Split each line into a tuple of (log_level, timestamp)
logs = lines.map(lambda line: tuple(line.strip().split(',')))

# Process the log messages
logs.foreachRDD(process_logs)

# Start the streaming context
ssc.start()
ssc.awaitTermination()