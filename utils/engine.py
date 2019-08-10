import requests, pymysql, sys, logging
pymysql.install_as_MySQLdb()
from pyspark.mllib.recommendation import ALS
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType, IntegerType
from bs4 import BeautifulSoup

conf = SparkConf().setAll([('spark.executor.memory', '13g'), ('spark.executor.cores', '3'),
                                   ('spark.driver.memory', '13g'), ('spark.executor.instances', '3'),
                                   ('spark.driver.cores', '3')])

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

CLOUDSQL_INSTANCE_IP = sys.argv[1]
CLOUDSQL_DB_NAME = sys.argv[2]
CLOUDSQL_USER = sys.argv[3]
CLOUDSQL_PWD = sys.argv[4]

TABLE_ITEMS = "MOVIES"
TABLE_RATINGS = "RATINGS"
TABLE_RECOMMENDATIONS = "RECOMMENDATIONS"
TABLE_LINKS = 'LINKS'
TABLE_USER = 'USER'

jdbcUrl = 'jdbc:mysql://%s:3306/%s?user=%s&password=%s' % \
                                            (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME, CLOUDSQL_USER, CLOUDSQL_PWD)

links = sqlContext.read.jdbc(url=jdbcUrl, table=TABLE_LINKS)
user_table = sqlContext.read.jdbc(url=jdbcUrl, table=TABLE_USER)


class DataHandler:
    def __init__(self, spark_session, sqlContext, jdbcUrl, table_name):
        self.spark_session = spark_session
        self.jdbcUrl = jdbcUrl
        self.sqlContext = sqlContext
        self.table_name = table_name
        self.data = self._process_data()
        self.columns = self.data.columns
        self.NEW_USERID = None
        self.rated_movies = dict()  # keys: ids of new users, values: corresponding movie ids. ==> {user_id: movie_ids}

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

    def add_new_user(self, user_id, movie_ids):
        """
        user_id: integer.
            id of new user
        movie_ids: list.
            list of user provided movie ids.
        """
        self.NEW_USERID = user_id
        rated_movies = [(self.NEW_USERID, int(i), 5) for i in movie_ids]  # [(user id, movie id, rating), ...]
        # converting "rated_movies" list to RDD.
        new_user_ratings_rdd = self.spark_session.parallelize(rated_movies)
        # converting new_user_ratings_rdd to spark dataframe object.
        new_user_ratings_df = new_user_ratings_rdd.toDF(schema=self.columns)
        self.rated_movies[self.NEW_USERID] = movie_ids
        self.data = self.data.union(new_user_ratings_df)  # concatenating existing ratings data with new_user_ratings_df


class Recommender(DataHandler):
    def __init__(self, spark_session, sqlContext, jdbcUrl, table_name):
        super().__init__(spark_session, sqlContext, jdbcUrl, table_name)
        self.best_rank = 24
        self.best_iteration = 10
        self.regularization_parameter = 0.1
        self.model = ALS()  # ALS Matrix factorization model.

    def train_model(self):
        # movielens dataset has explicit features. I could train algorithm that used for explicit features,
        # but for some reason (See: https://stackoverflow.com/questions/26213573/apache-spark-als-collaborative-filtering-results-they-dont-make-sense)
        # that algorithm doesn't give reasonable recommendations.
        model = ALS().trainImplicit(self.data.rdd.persist(), rank=self.best_rank, iterations=self.best_iteration,
                            lambda_=self.regularization_parameter)
        self.model = model

    def generate_recommendation(self, existing_user_id=None, external_user_id=None):
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
            if external_user_id:
                raw_recommendations = self.model.predictAll(unrated_movs) \
                    .map(lambda x: (external_user_id, x[1], x[2]))
            else:
                raw_recommendations = self.model.predictAll(unrated_movs)

        return raw_recommendations


# "rows" variable represents infos of users which's id is not last user's id (610) in original ratings data set,
# which has already selected movies and corresponding recommendations hasn't been generated yet.
rows = user_table.select(user_table.id, user_table.movieIds).filter((user_table.id != 610) &
                                                                    (user_table.movieIds.isNotNull()) &
                                                                    (user_table.job_submited.isNull())).collect()
# connection to cloudSQL db.
connection = pymysql.connect(CLOUDSQL_INSTANCE_IP,
                             user=CLOUDSQL_USER,
                             password=CLOUDSQL_PWD,
                             db=CLOUDSQL_DB_NAME)

cursor = connection.cursor()

engine = Recommender(sc, sqlContext, jdbcUrl, TABLE_RATINGS)
for i in rows:
    user_id = i[0]
    movieIds = i[1]
    # update value of job_submited column with 'done' for users whose recommendations are generating now.
    cursor.execute("UPDATE USER SET job_submited = %s WHERE id = %s",
                   ('done', user_id))

    movieIds = [int(i) for i in movieIds.split(',')]
    # adding this users to recommendation engine.
    engine.add_new_user(user_id, movieIds)

connection.commit()
connection.close()

engine.train_model()
logging.info('model has been trained.')

schema_for_recos = StructType([StructField("userId", StringType(), True), StructField("movieId", StringType(), True),
                     StructField("prediction", FloatType(), True), StructField("imdbLink", StringType(), True),
                     StructField("imgLink", StringType(), True)])

for i in rows:
    user_id = i[0]

    # RDD that has pairs like (userId, movieId, predicted rating)
    raw_recommendations = engine.generate_recommendation(existing_user_id=user_id)

    tracker = 0
    results = []  # list of top 48 recommendations.
    # sorting raw recommendations by predicted rating in descending order.
    sorted_raw_recommendations = raw_recommendations.distinct().sortBy(lambda a: -a[2])
    for i in sorted_raw_recommendations.toLocalIterator():
        movie_id = i[1]
        # getting corresponding IMDb id of recommended movie.
        movie_imdb_id = str(links.filter(links.movieId == movie_id).select('imdbId').collect()[0][0])
        len_ = len(movie_imdb_id)
        id_str = ""
        while len_ < 7:
            id_str += "0"
            len_ += 1
        id_str += movie_imdb_id
        # IMDb link of recommended movie.
        link = 'https://www.imdb.com/title/tt{}/'.format(id_str)

        r = requests.get(link)
        if r.status_code == 200:
            tracker += 1
            r = requests.get(link).text
            soup = BeautifulSoup(r)

            # image source
            for a in soup.find_all('div', attrs={'class': 'poster'}):
                # image link of recommended movies.
                img_link = a.find('img').get('src')
                res = i + (link, img_link)
                results.append(res)

        # I want to recommend 48 movies to user. That is why loop breaks if tracker is equal to 49.
        if tracker == 49:
            break

    logging.info('writing to DB...')
    # START save top 48 recommendations to db.
    dfToSave = sqlContext.createDataFrame(results, schema_for_recos)
    dfToSave.write.jdbc(url=jdbcUrl, table=TABLE_RECOMMENDATIONS, mode='append')


