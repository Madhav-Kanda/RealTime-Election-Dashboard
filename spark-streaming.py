from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import sum as _sum

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("Voting-System")
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')  # spark kafka integration
             .config('spark.jars', '/Users/madhav/Desktop/Code/Voting-system/postgresql-42.7.3.jar')  # postgresql drivers
             .config('spark.sql.adaptive.enable', 'false')  # disable adaptive query execution
             .getOrCreate())

    vote_schema = StructType([
        StructField('voter_id', StringType(), True),
        StructField('candidate_id', StringType(), True),
        StructField('voting_time', TimestampType(), True),
        StructField('state', StringType(), True),
        StructField('gender', StringType(), True),  # Added gender field
        StructField('vote', IntegerType(), True)
    ])

    votes_df = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'localhost:9092')
                .option('subscribe', 'votes_topic')
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col('value'), vote_schema).alias('data'))
                .select('data.*'))

    # Data Preprocessing and watermarking
    votes_df = votes_df.withColumn('voting_time', col('voting_time').cast(TimestampType())) \
                .withColumn('vote', col('vote').cast(IntegerType()))

    enriched_votes_df = votes_df.withWatermark('voting_time', '1 minute')

    # Aggregate votes per candidate and turnout by location and gender
    votes_per_candidate = enriched_votes_df.groupBy('candidate_id', 'state').agg(_sum('vote').alias('total_votes'))
    turnout_by_location = enriched_votes_df.groupBy('state').count().alias('total_votes')
    turnout_by_gender = enriched_votes_df.groupBy('gender').count().alias('total_votes')

    # Writing the results to Kafka
    votes_per_candidate_to_kafka = (votes_per_candidate.selectExpr('to_json(struct(*)) AS value')
                                    .writeStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', 'localhost:9092')
                                    .option('topic', 'aggregated_votes_per_candidate')
                                    .option('checkpointLocation', '/Users/madhav/Desktop/Code/Voting-system/checkpoints/checkpoint1')
                                    .outputMode('update')
                                    .start())

    turnout_by_location_to_kafka = (turnout_by_location.selectExpr('to_json(struct(*)) AS value')
                                    .writeStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', 'localhost:9092')
                                    .option('topic', 'aggregated_turnout_by_location')
                                    .option('checkpointLocation', '/Users/madhav/Desktop/Code/Voting-system/checkpoints/checkpoint2')
                                    .outputMode('update')
                                    .start())

    turnout_by_gender_to_kafka = (turnout_by_gender.selectExpr('to_json(struct(*)) AS value')
                                  .writeStream
                                  .format('kafka')
                                  .option('kafka.bootstrap.servers', 'localhost:9092')
                                  .option('topic', 'aggregated_turnout_by_gender')
                                  .option('checkpointLocation', '/Users/madhav/Desktop/Code/Voting-system/checkpoints/checkpoint3')
                                  .outputMode('update')
                                  .start())

    # Await termination for the streaming queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()
    turnout_by_gender_to_kafka.awaitTermination()
