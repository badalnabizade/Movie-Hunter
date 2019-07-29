import os, sys, logging
import numpy as np
from os.path import join
from subprocess import call
from pyspark.mllib.recommendation import ALS
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import FloatType, IntegerType


CLOUDSQL_INSTANCE_IP = sys.argv[1]
CLOUDSQL_DB_NAME = sys.argv[2]
CLOUDSQL_USER = sys.argv[3]
CLOUDSQL_PWD = sys.argv[4]
PROJECT_ID = sys.argv[5]

dst_URI = f'gs://{PROJECT_ID}/QUICK_RECO_ENGINE_OUTPUTS/'
df_file_name = 'pf_movie_ids.pkl'
V_matrix_file_name = 'Vt.npy'

conf = SparkConf().setAll([('spark.executor.memory', '13g'), ('spark.executor.cores', '3'),
                                   ('spark.driver.memory', '13g'), ('spark.executor.instances', '3'),
                                   ('spark.driver.cores', '3')])

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

TABLE_ITEMS = "MOVIES"
TABLE_RATINGS = "RATINGS"
TABLE_RECOMMENDATIONS = "RECOMMENDATIONS"

jdbcUrl = 'jdbc:mysql://%s:3306/%s?user=%s&password=%s' % \
                                            (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME, CLOUDSQL_USER, CLOUDSQL_PWD)

ratings = sqlContext.read.jdbc(url=jdbcUrl, table=TABLE_RATINGS)
# shuffle pairs and create 18 partitions with ratings dataframe that's distributed evenly across 3 machines.
# Then persist this dataframe.
ratings = ratings.repartition(18).persist()
logging.info('Dataframe is repartitioned')


class Recommender:
    def __init__(self, spark_session, sqlContext, jdbcUrl, table_name):
        self.spark_session = spark_session
        self.jdbcUrl = jdbcUrl
        self.sqlContext = sqlContext
        self.table_name = table_name
        self.data = self._process_data()
        self.columns = self.data.columns
        self.NEW_USERID = 610  # last user's id in ratings dataframe.
        self.rated_movies = dict()  # keys: ids of new users, values: corresponding movie ids. ==> {user_id: movie_ids}
        self.best_rank = 24
        self.best_iteration = 10
        self.regularization_parameter = 0.1
        self.model = ALS()  # ALS Matrix factorization model.

    def _process_data(self):

        raw_data = self.sqlContext.read.jdbc(url=self.jdbcUrl, table=self.table_name)
        cols = raw_data.columns
        if 'rating' in cols:  # if raw_data is ratings dataset
            data = raw_data.withColumn(cols[0], raw_data[cols[0]].cast(IntegerType()))  # convert dtype of userId to integer
            data = data.withColumn(cols[1], data[cols[1]].cast(IntegerType()))  # convert dtype of movieId to integer
            data = data.withColumn(cols[2], data[cols[2]].cast(FloatType()))  # convert dtype of rating to float
            # shuffle pairs and create 18 partitions with data that's distributed evenly across 3 machines. Then persist data.
            data = data.repartition(18).persist()
        else:
            data = raw_data.repartiton(18).persist()
        return data

    def add_new_user(self, movie_ids):
        """
        user_id: integer.
            id of new user
        movie_ids: list.
            list of user provided movie ids (string).
        """
        self.NEW_USERID += 1
        rated_movies = [(self.NEW_USERID, int(i), 5) for i in movie_ids]  # [(user id, movie id, rating), ...]
        # converting "rated_movies" list to RDD.
        new_user_ratings_rdd = self.spark_session.parallelize(rated_movies)
        # converting new_user_ratings_rdd to spark dataframe object.
        new_user_ratings_df = new_user_ratings_rdd.toDF(schema=self.columns)
        self.rated_movies[self.NEW_USERID] = movie_ids
        self.data = self.data.union(new_user_ratings_df)  # concatenating existing ratings data with new_user_ratings_df

    def train_model(self):
        # movielens dataset has explicit features. I could train algorithm that used for explicit features,
        # but for some reason (See: https://stackoverflow.com/questions/26213573/apache-spark-als-collaborative-filtering-results-they-dont-make-sense)
        # that algorithm doesn't give reasonable recommendations.
        model = ALS().trainImplicit(self.data.rdd.persist(), rank=self.best_rank, iterations=self.best_iteration,
                            lambda_=self.regularization_parameter)
        self.model = model

    def generate_recommendation(self, existing_user_id=None):
        data_rdd = self.data.rdd
        if existing_user_id:
            # Generates recommendation for particular user that already exists in ratings dataset.
            rated_movies_by_user = self.rated_movies[existing_user_id]  # list ids of rated movies by this user.
            unrated_movs = data_rdd.filter(lambda x: x[1] not in rated_movies_by_user) \
                .map(lambda s: (existing_user_id, s[1]))  # unrated movies
            raw_recommendations = self.model.predictAll(unrated_movs)

        else:
            # Generates recommendation for last user.
            new_id = self.NEW_USERID
            rated_movies_by_user = self.rated_movies[new_id]
            unrated_movs = data_rdd.filter(lambda x: x[1] not in rated_movies_by_user) \
                .map(lambda s: (new_id, s[1]))
            raw_recommendations = self.model.predictAll(unrated_movs)

        return raw_recommendations


engine = Recommender(sc, sqlContext, jdbcUrl, TABLE_RATINGS)

engine.train_model()
model = engine.model
logging.info('model has been trained.')

pf = model.productFeatures()  # Latent features of movies.
pdf = pf.toDF().toPandas().drop('_2', 1).rename(columns={'_1':'movieId'})  # dataframe of latent features of movies.

pdf.to_csv(df_file_name)
logging.info('product features dataframe is pickled.')

call(["hadoop", "fs", "-copyFromLocal", join(os.getcwd(), df_file_name), dst_URI])
logging.info('product features saved to GCS.')

# matrix that consists of rows of product features dataframe.
Vt = np.matrix(np.asarray(pf.values().collect())).astype('float16')
np.save(V_matrix_file_name, Vt)
logging.info('matrix is pickled.')

call(["hadoop", "fs", "-copyFromLocal", join(os.getcwd(), V_matrix_file_name), dst_URI])
logging.info('matrix saved to GCS')


