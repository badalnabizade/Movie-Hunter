import flask, pickle, os, json, requests, re, pymysql
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, session
import math
from time import time
import logging
import sys
import itertools
from math import sqrt
from operator import add
import findspark
findspark.init('/home/badal/frameworks/spark-2.4.2-bin-hadoop2.7')
from os.path import join, isfile, dirname
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType, IntegerType
from bs4 import BeautifulSoup
import requests
from engine import DataHandler, Recommender

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)


CLOUDSQL_INSTANCE_IP = 'localhost'
CLOUDSQL_DB_NAME = 'MOVIE_RECOMMENDATION'
CLOUDSQL_USER = 'root'
CLOUDSQL_PWD = 'Beden001998'

TABLE_ITEMS = "MOVIES"
TABLE_RATINGS = "RATINGS"
TABLE_RECOMMENDATIONS = "RECOMMENDATIONS"
TABLE_LINKS = 'LINKS'

jdbcUrl = 'jdbc:mysql://%s:3306/%s?user=%s&password=%s' % \
                                            (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME, CLOUDSQL_USER, CLOUDSQL_PWD)

titles = sqlContext.read.jdbc(url=jdbcUrl, table=TABLE_ITEMS)
ratings = sqlContext.read.jdbc(url=jdbcUrl, table=TABLE_RATINGS)
links = sqlContext.read.jdbc(url=jdbcUrl, table=TABLE_LINKS)

# class DataHandler():
#     def __init__(self, spark_session, table_name):
#         self.spark_session = sc
#         self.table_name = table_name
#         self.data = self._process_data()
#         self.columns = self.data.columns
#         self.NEW_USERID = self.data.agg({self.columns[0]: 'max'}).collect()[0][0]  # last user's id.
#         self.rated_movies = dict()
#
#     def _process_data(self):
#
#         raw_data = sqlContext.read.jdbc(url=jdbcUrl, table=self.table_name)
#         cols = raw_data.columns
#         if 'rating' in cols:
#             data = raw_data.withColumn(cols[0], raw_data[cols[0]].cast(IntegerType()))
#             data = data.withColumn(cols[1], data[cols[1]].cast(IntegerType()))
#             data = data.withColumn(cols[2], data[cols[2]].cast(FloatType()))
#         else:
#             data = raw_data
#         return data
#
#     def add_new_user(self, movie_ids):
#         """
#         movie_ids: list.
#             list of user provided movie ids.
#         """
#         self.NEW_USERID += 1
#         rated_movies = [(self.NEW_USERID, int(i), 5) for i in movie_ids]
#         new_user_ratings_rdd = sc.parallelize(rated_movies)
#         new_user_ratings_df = new_user_ratings_rdd.toDF(schema=self.columns)
#         self.rated_movies[self.NEW_USERID] = movie_ids
#         self.data = self.data.union(new_user_ratings_df)
#
# class Recommender(DataHandler):
#     def __init__(self, spark_session, table_name):
#         super().__init__(spark_session, table_name)
#         self.best_rank = None
#         self.best_iteration = None
#         self.regularization_parameter = None
#         self.model = ALS()
#
#     def train_test_split(self, train_ratio, valid_ratio, test_ratio, seed):
#         data_rdd = self.data.rdd
#         training_RDD, validation_RDD, test_RDD = data_rdd \
#             .randomSplit([train_ratio,
#                           valid_ratio,
#                           test_ratio], seed=seed)
#         return training_RDD, validation_RDD, test_RDD
#
#     def tune_model(self, ranks, iterations, regularization_parameter):
#         best_rank = -1
#         best_iter = -1
#         min_error = float('inf')
#         err = 0
#         seed = 8
#         errors = np.zeros(shape=(len(ranks)))
#         training_RDD, validation_RDD, test_RDD = self.train_test_split(6, 2, 2, seed)
#         validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
#         test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))
#
#         for rank in ranks:
#
#             model = ALS().train(training_RDD, rank, seed=seed, iterations=10,
#                                 lambda_=regularization_parameter)
#
#             predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
#             rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
#             error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())
#             errors[err] = error
#             err += 1
#             print('For rank {} the RMSE is {}'.format(rank, error))
#             if error < min_error:
#                 min_error = error
#                 best_rank = rank
#
#         print('The best model was trained with rank {}'.format(best_rank))
#         self.best_rank = best_rank
#         self.best_iteration = iterations
#         self.regularization_parameter = regularization_parameter
#
#     def train_model(self):
#         model = ALS().train(self.data.rdd, rank=self.best_rank, iterations=self.best_iteration,
#                             lambda_=self.regularization_parameter)
#         self.model = model
#
#     def generate_recommendation(self, num_movies, existing_user_id=None):
#         data_rdd = self.data.rdd
#         if existing_user_id:  # Generates recommendation for particlar user.
#             rated_movies_by_user = self.rated_movies[existing_user_id]
#             unrated_movs = data_rdd.filter(lambda x: x[1] not in rated_movies_by_user) \
#                 .map(lambda s: (existing_user_id, s[1]))
#             raw_recommendations = self.model.predictAll(unrated_movs)
#
#         else:  # Generates recommendation for last user.
#             new_id = self.NEW_USERID
#             rated_movies_by_user = self.rated_movies[new_id]
#             unrated_movs = data_rdd.filter(lambda x: x[1] not in rated_movies_by_user) \
#                 .map(lambda s: (new_id, s[1]))
#             raw_recommendations = self.model.predictAll(unrated_movs)
#
#         recommendations = raw_recommendations.distinct().takeOrdered(num_movies, key=lambda x: -x[2])
#         recommended_movie_ids = [str(recommendations[i][1]) for i in range(len(recommendations))]
#
#         return recommendations

app = Flask(__name__)
@app.route('/')
def home():
    data = titles.select("title").rdd.map(lambda r: r[0]).collect()
    return render_template('index1.html', data=data)

@app.route('/score', methods = ['POST'])
def recommendation():
    titles = pd.read_csv('/home/badal/Desktop/Movie Recommendation/Movielens data/movies.csv')
    # global movie_title_gen
    # global movie_link_gen
    # if request.method == 'POST':
        #get result from form and treat it
    input_json = request.form
    input_title_1 = input_json['first']
    input_title_2 = input_json['second']
    input_title_3 = input_json['third']
    input_title_4 = input_json['fourth']
    input_title_5 = input_json['fifth']


    input_list = [input_title_1,input_title_2,input_title_3,input_title_4,input_title_5]

    movie_ids = list()
    for i in input_list:
        movie_id = titles[titles['title'] == i]['movieId'].values[0]
        movie_ids.append(int(movie_id))

    engine = Recommender(sc, sqlContext, jdbcUrl, TABLE_RATINGS)
    engine.tune_model([24], iterations=10, regularization_parameter=0.1)
    engine.train_model()
    model = engine.model

    pf = model.productFeatures()
    Vt = np.matrix(np.asarray(pf.values().collect()))
    V_mult_V_T = np.dot(Vt, Vt.T)
    full_u = np.zeros(9724)

    movies_wth_indicies = pf.map(lambda x: x[0]).zipWithIndex()
    input_m_id_indicies = movies_wth_indicies.filter(lambda x: x[0] in movie_ids).map(lambda f: f[1]).collect()
    for i in input_m_id_indicies:
        full_u[i] = 5

    new_user_ratings = np.dot(full_u, V_mult_V_T)
    new_user_ratings = np.squeeze(np.asarray(new_user_ratings))
    recommended_movie_indicies = new_user_ratings.argsort()

    raw_recommendations = movies_wth_indicies \
        .filter(lambda x: x[1] in recommended_movie_indicies) \
        .map(lambda x: str(x[0])).collect()


# There are movies in data set that doesn't have working imdb page.
# if that movies occurs in recommendations, below code will ignore that movies.
# also extracts imdb url for each movie which has a working imdb page.
    tracker = 0
    recommended_movies = []    # Top 12 recommendations.
    imdb_links = []
    for movie_id in raw_recommendations:
        movie_imdb_id = str(links.filter(links.movieId == movie_id).select('imdbId').collect()[0][0])
        len_ = len(movie_imdb_id)
        id_str = ""
        while len_ < 7:
            id_str += "0"
            len_ += 1
        id_str += movie_imdb_id
        link = 'https://www.imdb.com/title/tt{}/'.format(id_str)

        r = requests.get(link)
        if r.status_code == 200:
            tracker += 1
            recommended_movies.append(movie_id)
            imdb_links.append(link)

        # I want to recommend 12 movies to user. That is why loop breaks if tracker is equal to 12.
        if tracker == 12:
            break
    #
    # schema = StructType([StructField("userId", StringType(), True), StructField("movieId", StringType(), True),
    #                      StructField("prediction", FloatType(), True)])
    #
    # # [START save top 12 recommendation]
    # dfToSave = sqlContext.createDataFrame(results, schema)
    # dfToSave.write.jdbc(url=jdbcUrl, table=TABLE_RECOMMENDATIONS, mode='overwrite')
    # print(results)
    #
    # connection = pymysql.connect('localhost',
    #                              user='root',
    #                              password='Beden001998',
    #                              db='MOVIE_RECOMMENDATION')
    #
    # cursor = connection.cursor()
    #
    # cursor.execute('SELECT m.title \
    #                     FROM RECOMMENDATIONS r \
    #                         JOIN MOVIES m \
    #                         ON r.movieId=m.id')
    #
    # recommended_movies = cursor.fetchall()
    # connection.close()

    # imdb_links = []
    # for i in recommended_movies:
    #     imdb_id = str(i[1])
    #     len_ = len(imdb_id)
    #     id_str = ""
    #     while len_ < 7:
    #         id_str += "0"
    #         len_ += 1
    #     id_str += imdb_id
    #     link = 'https://www.imdb.com/title/tt{}/'.format(id_str)
    #     imdb_links.append(link)

    # movie_title_gen = (i[0] for i in recommended_movies[1:])
    # movie_link_gen = (i for i in imdb_links[1:])

    img_links = list()
    for url in imdb_links:
        r = requests.get(url).text
        soup = BeautifulSoup(r)

        # image source
        for a in soup.find_all('div', attrs={'class': 'poster'}):
            img_links.append(a.find('img').get('src'))

    return render_template('index.html',
                           movie_1=recommended_movies[0], link_1=imdb_links[0], img_src_1 = img_links[0],
                           movie_2=recommended_movies[1], link_2=imdb_links[1], img_src_2 = img_links[1],
                           movie_3=recommended_movies[2], link_3=imdb_links[2], img_src_3 = img_links[2],
                           movie_4=recommended_movies[3], link_4=imdb_links[3], img_src_4 = img_links[3],
                           movie_5=recommended_movies[4], link_5=imdb_links[4], img_src_5 = img_links[4],
                           movie_6=recommended_movies[5], link_6=imdb_links[5], img_src_6 = img_links[5],
                           movie_7=recommended_movies[6], link_7=imdb_links[6], img_src_7 = img_links[6],
                           movie_8=recommended_movies[7], link_8=imdb_links[7], img_src_8 = img_links[7],
                           movie_9=recommended_movies[8], link_9=imdb_links[8], img_src_9 = img_links[8],
                           movie_10=recommended_movies[9], link_10=imdb_links[9], img_src_10 = img_links[9],
                           movie_11=recommended_movies[10], link_11=imdb_links[10], img_src_11 = img_links[10],
                           movie_12=recommended_movies[11], link_12=imdb_links[11], img_src_12 = img_links[11])

#
#
# @app.route('/next-movie', methods = ['POST'])
# def next_movie():
#     if request.method == 'POST':
#         next_item = request.form.get('next_item')
#         try:
#             return render_template('result.html',
#                                movie_1=next(movie_title_gen), link_1=next(movie_link_gen))
#         except StopIteration as end:
#             message = 'The End!'
#
#             return render_template('result.html', message=message)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
