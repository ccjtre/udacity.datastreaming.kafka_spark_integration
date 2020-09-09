# udacity.datastreaming.kafka_spark_integration
SF Crime Statistics with Spark Streaming

## How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

### Default trigger (runs micro-batch as soon as it can) versus ProcessingTime trigger with n-seconds micro-batch interval
Setting a ProcessingTime trigger with a 5-second micro-batch interval on the most intensive query (crime_type_incident_rate_by_hour) had a detrimental effect on the latency of the data, as evidenced by this warning message in spark:

020-09-09 05:22:44 WARN  ProcessingTimeExecutor:66 - Current batch is falling behind. The trigger interval is 5000 milliseconds, but spent 46218 milliseconds

Maintaining the Default trigger (runs micro-batch as soon as it can) was more performant in terms of latency, based on the durationMs subgroup metrics in the progress report

### maxOffsetsPerTrigger
Changing this property affected the inputRowsPerSecond to processedRowsPerSecond ratio (throughout); generally increasing the value of maxOffsetsPerTrigger increased the ratio of inputRowsPerSecond to processedRowsPerSecond.

## What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

### Default trigger
Maintaining the Default trigger (runs micro-batch as soon as it can) was more performant in terms of latency for the most intensive query (crime_type_incident_rate_by_hour).

### maxOffsetsPerTrigger
A value of 500 for maxOffsetsPerTrigger seemed to maintain an steady inputRowsPerSecond:processedRowsPerSecond ratio of ~12.6:~12.8; throughput declined on exceeding this maxOffsetsPerTrigger value.
