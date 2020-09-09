import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", DateType(), True),
    StructField("call_date", DateType(), True),
    StructField("offense_date", DateType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):
    '''
    This function generates four queries, the first two are completions of the starter code, the latter two are to fulfil the rubric requirements
    1) query: simple count aggregation by original_crime_type_name
    2) join_query: simple count aggregation by description. Demonstrates successful join of static radio_codes with calls for service stream
    3) rubric_query_1: count of events by original_crime_type_name by hour with watermarking: "different types of crimes occurred in certain time frames"
    4) rubric_query_2: count of events by hour with watermarking:"how many calls occurred in certain time frames"
    '''
    
    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", "SFPD.EVENTS.CALLS_FOR_SERVICE") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 10) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    
    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.selectExpr("original_crime_type_name", "disposition")

    # count the number of original crime type
    # 1) query: simple count aggregation by original_crime_type_name
    agg_df = distinct_table.groupBy("original_crime_type_name").count()

    agg_df.isStreaming
    
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    
    query = agg_df \
        .writeStream \
        .format("console") \
        .queryName("count_by_original_crime_type_name") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .start()

    # TODO attach a ProgressReporter
    # query.awaitTermination()
    
    # TODO get the right radio code json path
    # 2) join_query: simple count aggregation by description. Demonstrates successful join of static radio_codes with calls for service stream
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_table = distinct_table.join(radio_code_df, "disposition", "left_outer")
    join_table_agg = join_table.groupBy("description").count()

    join_query = join_table_agg \
        .writeStream \
        .format("console") \
        .queryName("count_by_disposition_description") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .start()    
    
    # 3) rubric_query_1: count of events by original_crime_type_name by hour with watermarking: "different types of crimes occurred in certain time frames"
    crime_event_table = service_table.selectExpr("call_date_time", "original_crime_type_name")
    
    crime_type_incident_rate_by_hour_agg = crime_event_table \
        .withWatermark("call_date_time", "30 minutes") \
        .groupBy(
            psf.window(crime_event_table.call_date_time, "1 hour", "1 hour"),
            crime_event_table.original_crime_type_name) \
        .count() \
        .orderBy(["window", "count"], ascending=[1, 0]) \
    
    crime_type_incident_rate_by_hour_query = crime_type_incident_rate_by_hour_agg \
        .writeStream \
        .format("console") \
        .queryName("crime_type_incident_rate_by_hour") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .start()    
    
    # 4) rubric_query_2: count of events by hour with watermarking:"how many calls occurred in certain time frames"
    incident_rate_by_hour_agg = crime_event_table \
        .withWatermark("call_date_time", "1 hour") \
        .groupBy(
            psf.window(crime_event_table.call_date_time, "1 hour", "1 hour")) \
        .count() \
        .orderBy(["window"]) \
        
    incident_rate_by_hour_query = incident_rate_by_hour_agg \
        .writeStream \
        .format("console") \
        .queryName("incident_rate_by_hour") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .start()   
        
    query.awaitTermination()
    join_query.awaitTermination()
    crime_type_incident_rate_by_hour_query.awaitTermination()
    incident_rate_by_hour_query.awaitTermination()
    
if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    logger.info("Spark started")
    spark.sparkContext.setLogLevel('WARN')
    run_spark_job(spark)

    spark.stop()
