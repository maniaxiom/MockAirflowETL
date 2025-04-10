from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lit, lag, when, sum as Fsum, countDistinct, to_timestamp
from pyspark.sql.types import LongType
import boto3
import json
import os
import subprocess
from confluent_kafka import Consumer, KafkaException, KafkaError
from airflow.hooks.base_hook import BaseHook
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# Set this variable to True or False
STORE_LOCALLY = True

# Utility function to get previous date
def get_previous_date(date_str):
    # Convert the input string to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    # Subtract one day
    previous_date = date_obj - timedelta(days=1)
    # Convert back to a string
    return previous_date.strftime("%Y-%m-%d")

# Function to consume and store Kafka messages (JSON)
def consume_and_store_messages(local_raw_path,
                               local_error_path, 
                               s3_bucket, 
                               raw_data_path, 
                               raw_error_path, 
                               topic,
                               run_date,
                               spark,
                               **kwargs):

    # Retrieve Kafka connection from Airflow's connection management
    kafka_conn_id = "kafka_default"  # Airflow's connection ID for Kafka
    kafka_hook = KafkaConsumerHook(kafka_config_id=kafka_conn_id,
                                   topics=[topic])

    # Schema registry for avro 
    conn = BaseHook.get_connection("kafka_schema_registry")
    sr_conf = {
        'url': conn.host,
        'basic.auth.user.info': "{}:{}".format(conn.login, conn.password)
    }

    schema_registry_client = SchemaRegistryClient(sr_conf)
    print("subjects: "+str(schema_registry_client.get_subjects()))
    schema_str = schema_registry_client.get_latest_version(topic+"-value")\
                                       .schema.schema_str
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    # Initialize AWS S3 client
    s3 = boto3.client("s3")

    # Get raw messages and errors
    raw_messages = []
    errors = []

    try:
        consumer = kafka_hook.get_consumer()
        consumer.subscribe([topic])

        while True:
            print("Fetching messages")
            msgs = consumer.consume(num_messages=100, timeout=10.0)
            print("Fetched messages")
            if msgs != []:
                print(msgs)
                for msg in msgs:
                    decoded_msg = avro_deserializer(msg.value(), 
                                                    SerializationContext(msg.topic(),  # noqa: E501
                                                    MessageField.VALUE))
                    if msg.error():
                        errors.append({"error": str(msg.error()), 
                                       "raw_message": msg.value()})
                        continue
                    try:
                        raw_messages.append(decoded_msg)
                    except Exception as e:
                        errors.append({"error": str(e), 
                                       "raw_message": msg.value()})
            else:
                print("No messages left to read.")
                break

    finally:
        consumer.close()

    print("Errors: ")
    print(errors)

    print("Raw msgs: ")
    print(raw_messages)

    # Store raw messages and errors
    if STORE_LOCALLY:
        error_dir = local_error_path + "/event_date={}".format(run_date)
        os.makedirs(error_dir, exist_ok=True)
        error_file = error_dir + "/errors-{}.json".format(str(datetime.now()))
        with open(error_file, "w") as error_file:
            json.dump(errors, error_file)
    else:
        s3.put_object(Bucket=s3_bucket, 
                      Key="error/{}/errors-{}.json".format(run_date, 
                                                           str(datetime.now())),  # noqa: E501
                      Body=json.dumps(errors))

    # Store raw messages in S3 as Parquet
    if raw_messages:
        df = spark.createDataFrame(raw_messages)

        # Add processing information
        df = df.withColumn("event_date", df["event_time"].cast("date"))
        df = df.withColumn("run_date", lit(run_date))

        if STORE_LOCALLY:
            os.makedirs(local_raw_path, exist_ok=True)
            if os.path.exists(local_raw_path):
                print(f"Directory created successfully: {local_raw_path}")
            else:
                print(f"Failed to create directory: {local_raw_path}")
            df.show()
            df.write.parquet(local_raw_path, 
                             mode="overwrite", 
                             partitionBy=["event_date", "run_date"])
            tmp_contents = subprocess.run(["ls", "-l", local_raw_path], 
                                          capture_output=True, 
                                          text=True)
            print("Contents of {}:\n".format(local_raw_path),
                  tmp_contents.stdout)

            print("Wrote df with " + str(df.count())
                  + " records to " + local_raw_path)  # noqa: W503
        else:
            df.write.parquet(raw_data_path, 
                             partitionBy=["event_date", "run_date"],
                             mode="append")  # should never be run twice but in case it is by accident, then at least it doesnt cause data loss  # noqa: E501


# Function to process data and upload to BigQuery
def process_data_and_upload(run_date,
                            local_raw_path,
                            raw_data_path,
                            spark,
                            **kwargs):  # remove run_id store_locally and event_date should not have any default value and should be a mandatory param  # noqa: E501
    # Read raw data
    prev_ds = get_previous_date(run_date)  # Using this instead of prev_ds for manually triggered runs to work correctly  # noqa: E501
    local_raw_path = local_raw_path + "/event_date={}".format(prev_ds)
    raw_data_path = raw_data_path + "/event_date={}".format(prev_ds)
    tmp_contents = subprocess.run(["ls", "-l", local_raw_path], 
                                  capture_output=True, 
                                  text=True)
    print("Contents of {}:\n".format(local_raw_path), tmp_contents.stdout)   
    if STORE_LOCALLY: 
        raw_df = spark.read.parquet(local_raw_path) 
    else:
        raw_df = spark.read.parquet(raw_data_path)

    # Session calculation
    raw_df = raw_df.withColumn("event_time",
                               to_timestamp("event_time",
                                            "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    user_key = ["user_id", "device"]
    window_event = Window.partitionBy(user_key).orderBy("event_time")
    session_df = raw_df.withColumn("prev_ev_time", 
                                   lag(raw_df["event_time"], 1, None)
                                   .over(window_event))
    session_df = session_df.withColumn("time_dif", 
                                       when(session_df["prev_ev_time"] == None,  # noqa: E501, E711
                                            None)
                                       .otherwise(session_df["event_time"] -  # noqa: E501, W504
                                                  session_df["prev_ev_time"]))  
    session_df = session_df.withColumn("event_ts_dif_sec", 
                                       session_df["time_dif"].cast(LongType()))
    session_df = session_df.withColumn("is_new_session", 
                                       when(session_df["event_ts_dif_sec"] < 30 * 60, 0)  # noqa: E501
                                       .otherwise(1))
    window_session = window_event.rowsBetween(Window.unboundedPreceding, 0)
    session_df = session_df.withColumn('calc_session_id', 
                                       Fsum(session_df['is_new_session'])
                                       .over(window_session))
    session_df.where("event_ts_dif_sec = 30*60").show(10, False)
    session_df.orderBy(["user_id", "device", "calc_session_id"]).show(10, False)

    # Process metrics: Count distinct sessions per user per day
    processed_df = session_df.groupBy(user_key).agg(
        countDistinct("calc_session_id").alias("session_count")
    )

    # Add in processing time for metrics (To help for versioning in case we re-run this task after ingesting re-corrected messages)  # noqa: E501
    processed_df = processed_df.withColumn("metrics_ts", lit(datetime.now()))

    raw_df.show(20, False)
    processed_df.show(10, False)

    # Store metrics in BigQuery
    bigquery_df = processed_df
    bigquery_hook = BigQueryHook()
    bigquery_hook.insert_all(
        dataset_id="<your_dataset>",
        table_id="<your_table>",
        rows=bigquery_df.collect()
    )


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,  # Send an email when actually deploying
    "retries": 0
}

# Define the DAG
with DAG(
    dag_id="ingest_tv_events_4",
    default_args=default_args,
    description="""Consume Kafka messages (JSON), process with PySpark, 
                and upload to BigQuery with Parquet storage""",
    schedule_interval="0 0 * * *",  # Runs daily at midnight
    start_date=datetime(2024, 12, 26),
    catchup=False,
    tags=["kafka", "pyspark", "bigquery", "parquet"],
) as dag:
    
    # Configuration (Better way: Fetch configs from a separate YAML file)
    local_raw_path = "/usr/local/airflow/data/tv_stream_events/raw/"
    local_error_path = "/usr/local/airflow/data/tv_stream_events/error/"
    s3_bucket = "<your_s3_bucket>"
    raw_data_path = "s3://{}/tv-stream-events/raw/".format(s3_bucket)
    raw_error_path = "s3://{}/tv-stream-events/error/".format(s3_bucket)
    bigquery_table = "tv.video.event_metrics"
    kafka_topic = "tv-1"

    # Setup
    spark = SparkSession.builder.appName("AirflowPySpark").getOrCreate()

    # Task Definitions
    task_consume = PythonOperator(
        task_id="consume_and_store_messages",
        python_callable=consume_and_store_messages,
        op_kwargs={"raw_data_path": raw_data_path,
                   "raw_error_path": raw_error_path,
                   "local_raw_path": local_raw_path,
                   "local_error_path": local_error_path,
                   "s3_bucket": s3_bucket,
                   "run_date": "{{ ds }}",
                   "topic": kafka_topic,
                   "spark": spark},
        provide_context=True,
    )

    task_process = PythonOperator(
        task_id="process_data_and_upload",
        python_callable=process_data_and_upload,
        op_kwargs={"run_date": "{{ ds }}",
                   "raw_data_path": "{}".format(raw_data_path),
                   "local_raw_path": "{}".format(local_raw_path),
                   "spark": spark},
        provide_context=True,
    )

    task_consume >> task_process
