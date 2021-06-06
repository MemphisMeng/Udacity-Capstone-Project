import boto3
import pandas as pd
import re
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import os
import csv
from airflow import DAG
# import configparser
from decouple import config
from airflow.operators.dummy import DummyOperator
from plugins.operators import loadTableOperator
from plugins.operators import recommendationOperator
from plugins.operators import DataQualityOperator

#config = configparser.ConfigParser()
#config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config('AWS_SECRET_ACCESS_KEY')

## Step 1: create AWS connection

# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name='us-east-1'
)

# Creating the high level object oriented interface
resource = boto3.resource(
    's3',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name='us-east-1'
)

clientResponse = client.list_buckets()

## Step 2: create DynamoDB connection
dynamodb = boto3.client(
    'dynamodb',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name='us-east-1'
)

## Step 3: create Spark Session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

## Step 4: prepare DAGs
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
            schedule_interval='@once'
            )

def strip_year(text):
'''
The original text contains the year when the film was released. This found is used to extract the title of the film only
@input:
    text: string, the format should in like (regex): .*? \(\d{4}\)
@return:
    res: string
'''
    try:
        res = re.findall(r'(.*?) \(\d{4}\)', text)[0]
    except:
        res = text
    return res


def match_tag(tags):
'''
Replace the name of tags with their conrresponding IDs
@input:
    tags: string, string of a list including tags, being separated by ","
@return:
    res: list of integers
'''
    res = []
    tagList = tags.split(',')
    for tag in tagList:
        if tag.strip() == 'tv movie':
            res.append(1129)
        else:
            res.append(tag_df['tagId'].loc[tag_df['tag'] == tag.strip()].values[0])

    return res


def match_actor(actors):
'''
Replace the name of actors with their conrresponding IDs
@input:
    tags: string, string of a list including actors, being separated by ","
@return:
    res: list of integers
'''
    res = []
    actorList = actors.split(', ')
    for actor in actorList:
        print(actor)
        res.append(actor_df['actorId'].loc[actor_df['actor'] == actor.strip()].values[0])

    return res


## Step 5: access to data lake/source
# read tag file
tags = pd.read_csv("~/PycharmProjects/UdacityCapstoneProject/data/tag.csv", quoting=csv.QUOTE_NONE, error_bad_lines=False, engine='python')

# read movie rating file
rating = pd.read_csv("~/PycharmProjects/UdacityCapstoneProject/data/rating.csv", quoting=csv.QUOTE_NONE, error_bad_lines=False, engine='python')

# read movie file
movie_full = pd.read_csv("~/PycharmProjects/UdacityCapstoneProject/data/movies.csv", quoting=csv.QUOTE_NONE, error_bad_lines=False, engine='python')[['id', 'title', 'genres', 'cast']]
movie = pd.read_csv("~/PycharmProjects/UdacityCapstoneProject/data/movie.csv", quoting=csv.QUOTE_NONE, error_bad_lines=False, engine='python')

print("Data Collection Complete!")

## Step 6: data warehousing
# user dimension
user_df = pd.DataFrame(rating['userId'].unique())

# tag dimension
tag_df = tags.copy()

# actor dimension
all_actors = [s.split(", ") for s in movie_full[movie_full.cast.notnull()].cast]
actors = [item.strip() for l in all_actors for item in l]
unique_actors = set(actors)
actor_df = pd.DataFrame(unique_actors).reset_index()
actor_df.rename(columns={'index': 'actorId', 0: 'actor'}, inplace=True)

print("Actor dimension Complete!")

# time dimension
time_df = pd.DataFrame()
time_df['timestamp'] = rating['timestamp'].unique()
time_df['year'] = time_df['timestamp'].astype('datetime64').dt.year
month = time_df['timestamp'].astype('datetime64').dt.month
day = time_df['timestamp'].astype('datetime64').dt.day

# movie dimension
movie['title'] = movie['title'].apply(strip_year)
df = pd.merge(left=movie, right=movie_full, left_on='title', right_on='title', how='left')
movie_df = df[['movieId', 'title', 'genres_y', 'cast']].drop_duplicates('movieId')
movie_df.rename(columns={'genres_y': 'genres'}, inplace=True)
movie_df['genres'].loc[movie_df.genres.notnull()] = movie_df['genres'].loc[movie_df.genres.notnull()].apply(
match_tag)
movie_df = movie_df.head(1000)
movie_df['cast'].loc[movie_df.cast.notnull()] = movie_df['cast'].loc[movie_df.cast.notnull()].apply(match_actor)

# rating fact
rating_df = rating.copy()
print("Data Warehousing completed!")

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

## Step 7: write in parquets and store to S3
# user table
load_user_table = loadTableOperator(
task_id='load_user_table',
dag=dag,
spark=spark,
df=user_df,
directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
table_name="users"
)

# time table
load_time_table = loadTableOperator(
task_id='load_time_table',
dag=dag,
spark=spark,
df=time_df,
directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
table_name="time"
)

# tag table
load_tag_table = loadTableOperator(
task_id='load_tag_table',
dag=dag,
spark=spark,
df=tag_df,
directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
table_name="tags"
)

# actor table
load_actor_table = loadTableOperator(
task_id='load_actor_table',
dag=dag,
spark=spark,
df=actor_df,
directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
table_name="actors"
)

# movie table
load_movie_table = loadTableOperator(
task_id='load_movie_table',
dag=dag,
spark=spark,
df=movie_df,
directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
table_name="movies"
)

# rating table
load_rating_table = loadTableOperator(
task_id='load_rating_table',
dag=dag,
spark=spark,
df=rating_df,
directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
table_name="rating"
)

phrase_one_operator = DummyOperator(task_id='Phrase_one_execution', dag=dag)

recommender = recommendationOperator(
    task_id='create_recommendation',
    dag=dag,
    spark=spark,
    directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/"
)

## quality tests
user_test = DataQualityOperator(
    task_id='user_test',
    dag=dag,
    spark=spark,
    directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
    table_name="users"
)

time_test = DataQualityOperator(
    task_id='time_test',
    dag=dag,
    spark=spark,
    directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
    table_name="time"
)

tag_test = DataQualityOperator(
    task_id='tag_test',
    dag=dag,
    spark=spark,
    directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
    table_name="tags"
)

actor_test = DataQualityOperator(
    task_id='actor_test',
    dag=dag,
    spark=spark,
    directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
    table_name="actors"
)

movie_test = DataQualityOperator(
    task_id='movie_test',
    dag=dag,
    spark=spark,
    directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
    table_name="movies"
)

rating_test = DataQualityOperator(
    task_id='rating_test',
    dag=dag,
    spark=spark,
    directory="s3://aws-emr-resources-699444535296-us-east-1/data lake/",
    table_name="rating"
)

end_operator = DummyOperator(task_id="End_execution", dag=dag)

start_operator >> load_user_table >> user_test >> recommender
start_operator >> load_time_table >> time_test >> recommender
start_operator >> load_tag_table >> tag_test >> recommender
start_operator >> load_actor_table >> actor_test >> recommender
start_operator >> load_movie_table >> movie_test >> recommender
start_operator >> load_rating_table >> rating_test >> recommender
recommender >> end_operator

print("Pipeline Building Complete!")
