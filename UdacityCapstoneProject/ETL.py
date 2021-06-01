import boto3
import pandas as pd
import dask.dataframe as dd
import re
from datetime import datetime, timedelta
import os
from airflow import DAG
import configparser
from airflow.operators.dummy import DummyOperator
from plugins.operators import loadTableOperator

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['INFO']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['INFO']['AWS_SECRET_ACCESS_KEY']

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


def strip_year(text):
    try:
        res = re.findall(r'(.*?) \(\d{4}\)', text)[0]
    except:
        res = text
    return res


def match_tag(tags):
    res = []
    tagList = tags.split(',')
    for tag in tagList:
        if tag.strip() == 'tv movie':
            res.append(1129)
        else:
            res.append(tag_df['tagId'].loc[tag_df['tag'] == tag.strip()].values[0])

    return res


def match_actor(actors):
    res = []
    actorList = actors.split(', ')
    for actor in actorList:
        print(actor)
        res.append(actor_df['actorId'].loc[actor_df['actor'] == actor.strip()].values[0])

    return res


if __name__ == '__main__':
    ## Step 3: access to data lake
    # read tag file
    tagObject = client.get_object(
        Bucket=clientResponse['Buckets'][0]['Name'],
        Key='data lake/genome_tags.csv'
    )
    tags = pd.read_csv(tagObject['Body'])

    # read movie rating file
    # ratingObject = client.get_object(
    #     Bucket=clientResponse['Buckets'][0]['Name'],
    #     Key='data lake/rating.csv'
    # )
    rating = dd.read_csv("s3://aws-emr-resources-699444535296-us-east-1/data lake/rating.csv")
    # read movie file
    moviesObject = client.get_object(
        Bucket=clientResponse['Buckets'][0]['Name'],
        Key='data lake/movies.csv'
    )
    movie_full = pd.read_csv(moviesObject['Body'])[['id', 'title', 'genres', 'cast']]
    movieObject = client.get_object(
        Bucket=clientResponse['Buckets'][0]['Name'],
        Key='data lake/movie.csv'
    )
    movie = pd.read_csv(movieObject['Body'])

    print("Data Collection Complete!")

    ## Step 4: data warehousing
    # user dimension
    user_df = rating['userId'].unique().to_frame()

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
    timestamp = rating['timestamp'].unique()
    year = dd.to_datetime(timestamp, unit='ns').dt.year
    month = dd.to_datetime(timestamp, unit='ns').dt.month
    day = dd.to_datetime(timestamp, unit='ns').dt.day
    time_df = dd.concat([timestamp, year, month, day], axis=1)
    time_df.columns = ['timestamp', 'year', 'month', 'day']

    # movie dimension
    movie['title'] = movie['title'].apply(strip_year)
    df = pd.merge(left=movie, right=movie_full, left_on='title', right_on='title', how='left')
    movie_df = df[['movieId', 'title', 'genres_y', 'cast']].drop_duplicates('movieId')
    movie_df.rename(columns={'genres_y': 'genres'}, inplace=True)
    movie_df['genres'].loc[movie_df.genres.notnull()] = movie_df['genres'].loc[movie_df.genres.notnull()].apply(
        match_tag)
    # movie_df['cast'].loc[movie_df.cast.notnull()] = movie_df['cast'].loc[movie_df.cast.notnull()].apply(match_actor)

    # rating fact
    rating_df = rating.copy()
    print("Data Warehousing completed!")

    ## Step 5: prepare DAGs
    default_args = {
        'owner': 'udacity',
        'start_date': datetime.now(),
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_retry': False,
        'catchup': False
    }

    dag = DAG('udac_example_dag',
              default_args=default_args,
              description='Create Data Warehouse in DynamoDB and Recommendation System',
              schedule_interval='0 * * * *'
              )

    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    ## Step 6: create tables and import data
    # user table
    user_key_schema = [
        {
            'AttributeName': 'userId',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'userId',
            'KeyType': 'RANGE'
        }
    ]
    user_attribute_definitions = [
        {
            'AttributeName': 'userId',
            'AttributeType': 'N'
        }
    ]
    load_user_table = loadTableOperator(
        task_id='load_user_table',
        dag=dag,
        key_schema=user_key_schema,
        attribute_definitions=user_attribute_definitions,
        table_name="users",
        df=user_df,
        client=dynamodb,
        columns=user_df.columns,
    )

    # time table
    time_key_schema = [
        {
            'AttributeName': 'year',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'year',
            'KeyType': 'RANGE'
        }
    ]
    time_attribute_definitions = [
        {
            'AttributeName': 'year',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'month',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'day',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'timestamp',
            'AttributeType': 'S'
        }
    ]
    load_time_table = loadTableOperator(
        task_id='load_tag_table',
        dag=dag,
        key_schema=time_key_schema,
        attribute_definitions=time_attribute_definitions,
        table_name="time",
        df=time_df,
        client=dynamodb,
        columns=time_df.columns,
    )

    # tag table
    tag_key_schema = [
        {
            'AttributeName': 'tagId',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'tagId',
            'KeyType': 'RANGE'
        }
    ]
    tag_attribute_definitions = [
        {
            'AttributeName': 'tagId',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'tag',
            'AttributeType': 'S'
        }
    ]
    load_tag_table = loadTableOperator(
        task_id='load_tag_table',
        dag=dag,
        key_schema=tag_key_schema,
        attribute_definitions=tag_attribute_definitions,
        table_name="tags",
        df=tag_df,
        client=dynamodb,
        columns=tag_df.columns,
    )

    # actor table
    actor_key_schema = [
        {
            'AttributeName': 'actorId',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'actorId',
            'KeyType': 'RANGE'
        }
    ]
    actor_attribute_definitions = [
        {
            'AttributeName': 'actorId',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'actor',
            'AttributeType': 'S'
        }
    ]
    load_actor_table = loadTableOperator(
        task_id='load_actor_table',
        dag=dag,
        key_schema=actor_key_schema,
        attribute_definitions=actor_attribute_definitions,
        table_name="actors",
        df=actor_df,
        client=dynamodb,
        columns=actor_df.columns,
    )

    # movie table
    # movie_key_schema = [
    #     {
    #         'AttributeName': 'movieId',
    #         'KeyType': 'HASH'
    #     },
    #     {
    #         'AttributeName': 'movieId',
    #         'KeyType': 'RANGE'
    #     }
    # ]
    # movie_attribute_definitions = [
    #     {
    #         'AttributeName': 'movieId',
    #         'AttributeType': 'N'
    #     },
    #     {
    #         'AttributeName': 'title',
    #         'AttributeType': 'S'
    #     }
    # ]
    # load_movie_table = loadTableOperator(
    #     task_id='load_movie_table',
    #     dag=dag,
    #     key_schema=movie_key_schema,
    #     attribute_definitions=movie_attribute_definitions,
    #     table_name="movies",
    #     df=movie_df,
    #     client=dynamodb,
    #     columns=movie_df.columns,
    # )

    # rating table
    rating_key_schema = [
        {
            'AttributeName': 'userId',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'movieId',
            'KeyType': 'RANGE'
        }
    ]
    rating_attribute_definitions = [
        {
            'AttributeName': 'userId',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'movieId',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'rating',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'timestamp',
            'AttributeType': 'S'
        }
    ]
    load_rating_table = loadTableOperator(
        task_id='load_rating_table',
        dag=dag,
        key_schema=rating_key_schema,
        attribute_definitions=rating_attribute_definitions,
        table_name="rating",
        df=rating_df,
        client=dynamodb,
        columns=rating_df.columns,
    )

    end_operator = DummyOperator(task_id='End_execution', dag=dag)

    start_operator >> [load_user_table, load_time_table, load_tag_table, load_actor_table, load_movie_table,
                       load_rating_table] >> end_operator
    print("Pipeline Building Complete!")
