# import pymysql
#
#
# connection = pymysql.connect('localhost',
#                          user='root',
#                          password='Beden001998',
#                          db='MOVIE_RECOMMENDATION')
# name = 'eli'
# password = 'elishka'
# cursor = connection.cursor()
# cursor.execute('SELECT id FROM USER WHERE name = %s AND password = %s', (name, password))
# user_id = cursor.fetchall()[0][0]
# cursor.execute('SELECT m.title, r.imdbLink, r.imgLink \
#                 FROM QUICK_RECOMMENDATIONS r \
#                 JOIN MOVIES m \
#                 ON m.id=r.movieId WHERE r.userId=%s', user_id)
# row = cursor.fetchall()
# print(row)
# connection.close()
# print([int(i) for i in row[0][0].split(',')])
import pymysql
from pymysql import OperationalError
pymysql.install_as_MySQLdb()

# from engine import DataHandler, Recommender


# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
#
# CLOUDSQL_INSTANCE_IP = 'localhost'
# CLOUDSQL_DB_NAME = 'MOVIE_RECOMMENDATION'
# CLOUDSQL_USER = 'root'
# CLOUDSQL_PWD = 'Beden001998'
#
# TABLE_ITEMS = "MOVIES"
# TABLE_RATINGS = "RATINGS"
# TABLE_RECOMMENDATIONS = "RECOMMENDATIONS"
# TABLE_LINKS = 'LINKS'
#
# jdbcUrl = 'jdbc:mysql://%s:3306/%s?user=%s&password=%s' % \
#                                             (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME, CLOUDSQL_USER, CLOUDSQL_PWD)
#
# titles = sqlContext.read.jdbc(url=jdbcUrl, table=TABLE_ITEMS)
# ratings = sqlContext.read.jdbc(url=jdbcUrl, table=TABLE_RATINGS)
# links = sqlContext.read.jdbc(url=jdbcUrl, table=TABLE_LINKS)


#Connection to MySQL database
# connection = pymysql.connect('34.65.236.242',
#                          user='root',
#                          password='Beden001998',
#                          db='MOVIE_RECOMMENDATION')
from os import getenv

# connection = pymysql.connect('34.65.236.242',
#                          user='root',
#                          password='Beden001998',
#                          db='MOVIE_RECOMMENDATION')

CONNECTION_NAME = getenv(
  'INSTANCE_CONNECTION_NAME',
  'sparkrecommendationengine:europe-west6:sparkrecommendationengine')



DB_USER = getenv('MYSQL_USER', 'root')
DB_PASSWORD = getenv('MYSQL_PASSWORD', 'Beden001998')
DB_NAME = getenv('MYSQL_DATABASE', 'MOVIE_RECOMMENDATION')
HOST = getenv('INSTANCE_IP', '34.65.236.242')

mysql_config = {
  'host': HOST,
  'user': DB_USER,
  'password': DB_PASSWORD,
  'db': DB_NAME,
  'charset': 'utf8mb4',
  'autocommit': True
}
try:
    connection = pymysql.connect(**mysql_config)
except OperationalError:
    # If production settings fail, use local development ones
    mysql_config['unix_socket'] = f'/cloudsql/{CONNECTION_NAME}'
    connection = pymysql.connect(**mysql_config)

cursor = connection.cursor()

cursor.execute("SELECT * FROM USER")  # last user's id.
userId = cursor.fetchall()
print(userId)
connection.close()
