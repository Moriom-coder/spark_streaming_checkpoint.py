"""
Apache Spark Structured Streaming assignment
--------------------------------------------
Data source: JSON files arriving in an input directory.

The program:
1. Monitors the input directory for new JSON files.
2. Parses each file using an explicit schema.
3. Writes streaming results to Parquet.
4. Uses a durable checkpoint directory to recover progress and state
   after an unexpected failure or restart.

Example input file (place inside ./stream_input/):
{"id": 1, "name": "Asha", "department": "CSE", "marks": 85.5}

Run:
    spark-submit spark_streaming_checkpoint.py

Requirements:
    pip install pyspark

Note:
    Use a persistent local or distributed filesystem for the input, output,
    and checkpoint directories. Do not delete the checkpoint directory
    between restarts if you want Spark to recover the previous query.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)


def main():
    # Create the Spark session.
    spark = (
        SparkSession.builder
        .appName("FileStreamWithFaultToleranceAndCheckpointing")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Define the schema of incoming JSON records.
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("marks", DoubleType(), True),
    ])

    # Directories used by the streaming application.
    input_path = "./stream_input"
    output_path = "./stream_output"
    checkpoint_path = "./checkpoint/spark_file_stream"

    # Read newly arriving JSON files as a streaming DataFrame.
    streaming_df = (
        spark.readStream
        .format("json")
        .schema(schema)
        .option("maxFilesPerTrigger", 1)
        .load(input_path)
    )

    # Example transformation: keep records with a valid student ID.
    processed_df = streaming_df.filter(streaming_df.id.isNotNull())

    # Write the stream to Parquet in append mode.
    # checkpointLocation stores source offsets and commit information,
    # allowing Spark to resume processing after a failure.
    query = (
        processed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Streaming started. Add JSON files to:", input_path)
    print("Output is being written to:", output_path)
    print("Checkpoint data is stored at:", checkpoint_path)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping the streaming query...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
