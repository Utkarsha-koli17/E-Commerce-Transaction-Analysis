from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def check_data_quality(**kwargs):
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='metastore',
        user='bigdata',
        password='bigdata123'
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ecommerce_cleaned")
    count = cur.fetchone()[0]
    conn.close()
    print(f"✅ Total records in database: {count}")
    return count

def check_kafka_lag(**kwargs):
    from kafka import KafkaConsumer, TopicPartition
    
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        group_id='airflow-monitor'
    )
    
    partitions = consumer.partitions_for_topic('ecommerce-transactions')
    if not partitions:
        print("⚠️ No partitions found for topic")
        return 0
    
    total_lag = 0
    for partition in partitions:
        tp = TopicPartition('ecommerce-transactions', partition)
        consumer.assign([tp])
        current_pos = consumer.position(tp)
        consumer.seek_to_end(tp)
        end_offset = consumer.position(tp)
        lag = end_offset - current_pos
        total_lag += lag
        print(f"Partition {partition}: Current={current_pos}, End={end_offset}, Lag={lag}")
    
    consumer.close()
    print(f"📊 Total Kafka lag: {total_lag} messages")
    return total_lag

with DAG(
    'ecommerce_spark_etl_pipeline',
    default_args=default_args,
    description='Process e-commerce data from Kafka to PostgreSQL using Spark',
    schedule_interval='@hourly',
    catchup=False,
    tags=['ecommerce', 'spark', 'etl'],
) as dag:

    check_kafka = PythonOperator(
        task_id='check_kafka_lag',
        python_callable=check_kafka_lag,
    )

    spark_etl = SparkSubmitOperator(
        task_id='spark_etl_job',
        application='/opt/spark_etl.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.postgresql:postgresql:42.2.18',
        total_executor_cores=4,
        executor_memory='2G',
        driver_memory='1G',
        conf={
            'spark.sql.shuffle.partitions': '8',
            'spark.sql.legacy.timeParserPolicy': 'LEGACY',
            'spark.streaming.stopGracefullyOnShutdown': 'true',
        },
        name='ecommerce-etl-airflow',
    )

    verify_data = PythonOperator(
        task_id='verify_data_quality',
        python_callable=check_data_quality,
    )

    success_notification = BashOperator(
        task_id='success_notification',
        bash_command='echo "✅ ETL pipeline completed successfully at $(date)"',
    )

    check_kafka >> spark_etl >> verify_data >> success_notification
