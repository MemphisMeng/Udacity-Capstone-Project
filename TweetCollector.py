from airflow import DAG
from plugins.operators import tweetFeedOperator
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'catchup': False,
    'execution_timeout': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
            default_args=default_args,
            description='Create Data Warehouse in DynamoDB and Recommendation System',
            schedule_interval='0 * * * *' # hourly
            )

twitterCollector = tweetFeedOperator(
    task_id='load_user_table',
    dag=dag,
    spark=spark,
    directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/"
)

twitterCollector