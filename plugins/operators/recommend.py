from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import numpy as np
import pandas as pd
from tqdm import tqdm

class recommendationOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 spark,
                 directory,
                 *args, **kwargs):
        super(recommendationOperator, self).__init__(*args, **kwargs)
        self.spark = spark
        self.directory = directory

    def execute(self, context):
        rating = self.spark.read.parquet(self.directory + "rating").toPandas()
        movies = self.spark.read.parquet(self.directory + "movies").toPandas()
        tags = self.spark.read.parquet(self.directory + "tags").toPandas()

        movie_profile = movies.copy()

        for genre in tags['tag'].unique().tolist():
            movie_profile[genre] = 0

        for i in range(len(movie_profile)):
            if movie_profile['genres'].iloc[i]:
                for g in movie_profile['genres'].iloc[i]:
                    movie_profile[g].iloc[i] = 1

        movie_profile = movie_profile.drop(columns=['title', 'genres']).set_index('movieId')

        # generate users' content
        user_x_movie = pd.pivot_table(rating, values='rating', index=['movieId'], columns=['userId'])
        user_x_movie.sort_index(axis=0, inplace=True)
        userIDs = user_x_movie.columns
        user_profile = pd.DataFrame(columns=movie_profile.columns)

        for i in tqdm(range(len(user_x_movie.columns))):
            working_df = movie_profile.mul(user_x_movie.iloc[:, i], axis=0)
            working_df.replace(0, np.NaN, inplace=True)
            user_profile.loc[userIDs[i]] = working_df.mean(axis=0)

        # apply TFIDF for similarity comparison
        df = movie_profile.sum()
        idf = (len(movies) / df).apply(np.log)  # log inverse of DF
        TFIDF = movie_profile.mul(idf.values)
        df_predict = pd.DataFrame()

        for i in tqdm(range(len(user_x_movie.columns))):
            working_df = TFIDF.mul(user_profile.iloc[i], axis=1)
            df_predict[user_x_movie.columns[i]] = working_df.sum(axis=1)

        self.spark.createDataFrame(user_profile).write.mode('overwrite').parquet(self.directory + "user_profile")
        self.spark.createDataFrame(df_predict).write.mode('overwrite').parquet(self.directory + "df_predict")
        self.spark.createDataFrame(movie_profile).write.mode('overwrite').parquet(self.directory + "movie_profile")
        self.spark.createDataFrame(TFIDF).write.mode('overwrite').parquet(self.directory + "TFIDF")
