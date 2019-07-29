# import MySQLdb # for python 2.7 runtime
import pymysql # import this in python 3 runtime
from pymysql import OperationalError
import requests, csv, gcsfs
pymysql.install_as_MySQLdb()
from google.cloud import storage
from google.oauth2 import service_account
# import pandas as pd
import numpy as np
from flask import Flask, render_template, request, redirect, url_for, session
from bs4 import BeautifulSoup
from os import getenv
from flask_session import Session

sess = Session()
json_string = {'type': 'service_account',
 'project_id': 'sparkrecommendationengine',
 'private_key_id': '45d29f3b4e5007134f94d7816f47a2857f7ebf7a',
 'private_key': '-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0Q3lMvAn1sAtp\nGwJYaSCYOjxJ6fAl/0FWulRhhonkAbHy75ve+Bg9+B1Gx67u0UU4vwrD1IjOcFxK\n1nji19zz5ezUVhaRxt9reDDP6Zwy3rDjM1/bXkgdyxizyNfYSbCEOnanuOcp9HZu\nYblhIHxYzW00cqNuGu0R1KJZJrUyttGsp+W4fbHYXIdzFZaU0uYnx3/UfM7EwPxT\n/l9PPAybOpFLnlX48Fqm8wZDgVjPLHkADTd09Ls5h16J+A2vww52XpdsFBEHJCgP\nNfOd471oSkfT7oUUHveDHVCna4ZQMrm/AsXxruKk5Hf8tnr09GZ3arC6YIA2xXFW\nFmWa7Cl1AgMBAAECggEAB/ts5Z71+fEfsDnH8s5E6Ti67rLzNVuGMybxEZh5hqUk\nkhNWHZtyx1uzQC0ba+vMWuTqvx0sKagEDjRNCg5w9EPcJtLhEPdzcTjTQIGhrwb5\nYjYmKnqUSu1xXJSqKiOpM1G7a1Xetpbin6SoJsrfdrcx/L6OyhNbKAz4W2ZBY8tt\nfb6b71GDlOM7QYHtOY9hC48CZPULaV2BTmdK6P/6pyslRSCRfMlvqv5H4WbEi2Y6\nwWYsl7HLsstljM7Z/9xvdWP3roUuTv06M4HQ2/ICoF+C4fcbSNd6A92aTomi367e\nZzNROlrY3tZx3bKcqcfKsYMJTIujxYJAl3HPXH5DgQKBgQDvuFcBC4WMba1lNs9J\n9IhOXK5eXQa0UqvtO29pXwejBbGGUvTgbOFOCSsInHbGrUsCBNKmeHt2HNhfNgT2\noNehdIt+AJxMyeVwNckbsHXPiwFSXN2nV6kseFCWQwMLuOzJABpeULRrza7SJYMG\nR0i3M4Z5D9zULHl6S1+06ye+ZQKBgQDAgXOAeEPugvWxbnPuGttjfMInpQegJLmb\n5SoabvrAHCtR30UxU1ff8Q+LWpcDj+egTHsj9ssv42xz7h0vhOm4tgKkDQBtPoVc\nkfgw3ohX3XMDzVdT8ujtJrWZvYKkdwJcL36vdvs8/9lQFUMgT2q3cUIBtyJuXWSo\nByZMn77F0QKBgHMksS3AUgJ0v//IxuJ4AWzaWarbthexSgGmNcqAKdPlLTMe755I\nziPEhZYaSXb+z/OFS4VIg8zk6A07jkDEWy6jI0l/k5PSulEelJ2nMb9hvl3IW3NA\nyPKiS9sRDwp3ZczaLtA0paTOY/VurTO707KjnijSNLj5L9RNFgh5l1p9AoGBAI7M\nuEhZblL0lJsdCyRaidngBwpvkhuKgqEROs1G+/0cKEpgbCxt3abCZDyEY7eBvVmc\nwk/oV8tbUe0hekbwuouJgKX++w0OrtD/evONb+h13ka4wQoGDCE3cMjt2oZzva1y\nkfBJO4+BXsvYKeZC4y/W9RbeqczivLMkMpXufg6xAoGBAM044Log1l3+NJxLO9nD\ng74y/Rn6HXO4zeFgBhX4TmqVt+TfdFqZBOOmGSfRR0WsmBT8J8dAmK9mGtFKli0p\nXKzxZgkf69rE6CbS8m4VrNqR93jwTuHldKWE9xMVaUHMPyBP+QGFmJob1kAQipIH\neaEKUnDdFAWCygTfugaiExhD\n-----END PRIVATE KEY-----\n',
 'client_email': '641777981584-compute@developer.gserviceaccount.com',
 'client_id': '103355811924237683410',
 'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
 'token_uri': 'https://oauth2.googleapis.com/token',
 'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
 'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/641777981584-compute%40developer.gserviceaccount.com'}

fs = gcsfs.GCSFileSystem(project='sparkrecommendationengine', token=json_string)

# Connection to MySQL database
CONNECTION_NAME = getenv(
  'INSTANCE_CONNECTION_NAME',
  'sparkrecommendationengine:europe-west6:sparkrecommendationengine')

DB_USER = getenv('MYSQL_USER', 'root')
DB_PASSWORD = getenv('MYSQL_PASSWORD', 'Beden001998')
DB_NAME = getenv('MYSQL_DATABASE', 'MOVIE_RECOMMENDATION')
HOST = getenv('INSTANCE_IP', 'localhost')

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


app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'
app.config['SESSION_TYPE'] = 'memcache'

@app.route("/")
def signup():
    return render_template("index.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        name = request.form['uname']
        mail = request.form['mail']
        password = request.form['passw']
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM USER ORDER BY id DESC LIMIT 1")  # last user's id.
        userId = cursor.fetchall()[0][0] + 1
        cursor.execute('INSERT INTO USER (id,name,mail,password) VALUES (%s, %s, %s, %s)', (userId, name, mail, password))
        connection.commit()
        cursor.close()

    return render_template("login.html")


@app.route('/decision', methods=['GET', 'POST'])
def decision():

    name = request.form['uname']
    password = request.form['passw']
    cursor = connection.cursor()
    session['name'] = name
    session['password'] = password
    cursor.execute('SELECT name, password FROM USER WHERE name=%s AND password=%s', (name, password))
    row = cursor.fetchall()
    cursor.close()

    if len(row) > 0:  # if password and username are correct.
        cursor = connection.cursor()
        cursor.execute('SELECT movieIds FROM USER WHERE name=%s AND password=%s', (name, password))
        m_ids = cursor.fetchall()[0][0]
        cursor.close()

        if not m_ids:
            session['has_movie_ids'] = False
            cursor = connection.cursor()
            cursor.execute('SELECT title FROM MOVIES')
            raw_titles = cursor.fetchall()
            cursor.close()
            data = [raw_titles[i][0] for i in range(len(raw_titles))]
            return render_template('main.html', data=data)

        else:
            session['has_movie_ids'] = True
            return redirect(url_for('recommendation'))

    else:  # if password or user name is incorrect.
        return 'password or user name is incorrect'

@app.route('/quick_recommendations', methods=['POST', 'GET'])
def recommendation():
    # global movie_title_gen
    # global movie_link_gen
    # if request.method == 'POST':
    # get result from form and treat it
    cursor = connection.cursor()
    print(session['name'], session['password'], session['has_movie_ids'])

    if session['has_movie_ids'] == False:
        input_json = request.form
        input_title_1 = input_json['first']
        input_title_2 = input_json['second']
        input_title_3 = input_json['third']
        input_title_4 = input_json['fourth']
        input_title_5 = input_json['fifth']

        input_list = [input_title_1,input_title_2,input_title_3,input_title_4,input_title_5]
        movie_ids = list()
        print(input_list)
        # cursor.execute("SELECT movieIds FROM movies WHERE title = %s", i)
        for i in input_list:
            cursor.execute("SELECT id FROM MOVIES WHERE title = %s", i)
            movie_id = cursor.fetchall()[0][0]
            movie_ids.append(str(movie_id))

        cursor.execute("UPDATE USER SET movieIds = %s WHERE name = %s AND password = %s", ([','.join(movie_ids)],
                                                                                           session['name'],
                                                                                           session['password']))
        connection.commit()

        # submit pyspark job
        # manager = DataprocManager('sparkrecommendationengine', 'sparkrecommendationengine',
        #                           'sparkrecommendationengine',
        #                           '/home/badal/Desktop/Movie Recommendation/app/engine.py')
        #
        # if not manager.list_clusters_with_details() or manager.list_clusters_with_details() == 'DELETING':
        #     manager.wait_for_cluster_deletion()
        #     manager.create_cluster()
        #     time.sleep(5)
        #     manager.submit_pyspark_job()
        # else:
        #     manager.submit_pyspark_job()

    # else:
    #     cursor.execute('SELECT movieIds FROM USER where name=%s AND password=%s', (name, password))
    #     connection.commit()
    #     movie_ids = cursor.fetchall()
    #     movie_ids = [int(i) for i in movie_ids[0][0].split(',')]
    #     print(movie_ids)



#     engine = Recommender(sc, sqlContext, jdbcUrl, TABLE_RATINGS)
#     engine.add_new_user(movie_ids)
#     engine.tune_model([24], iterations=10, regularization_parameter=0.1)
#     engine.train_model()
#     raw_recommendations = engine.generate_recommendation()
#
#
# # There are movies in data set that doesn't have working imdb page.
# # if that movies occurs in recommendations, below code will ignore that movies.
# # also extracts imdb url for each movie which has a working imdb page.
#     tracker = 0
#     results = []    # Top 12 recommendations.
#     imdb_links = []
#     for i in raw_recommendations.distinct().sortBy(lambda a: -a[2]).toLocalIterator():
#         movie_id = i.product
#         movie_imdb_id = str(links.filter(links.movieId == movie_id).select('imdbId').collect()[0][0])
#         len_ = len(movie_imdb_id)
#         id_str = ""
#         while len_ < 7:
#             id_str += "0"
#             len_ += 1
#         id_str += movie_imdb_id
#         link = 'https://www.imdb.com/title/tt{}/'.format(id_str)
#
#         r = requests.get(link)
#         if r.status_code == 200:
#             tracker += 1
#             results.append(i)
#             imdb_links.append(link)
#
#         # I want to recommend 12 movies to user. That is why loop breaks if tracker is equal to 12.
#         if tracker == 12:
#             break
#
#     schema = StructType([StructField("userId", StringType(), True), StructField("movieId", StringType(), True),
#                          StructField("prediction", FloatType(), True)])
#
#     # [START save top 12 recommendation]
#     dfToSave = sqlContext.createDataFrame(results, schema)
#     dfToSave.write.jdbc(url=jdbcUrl, table=TABLE_RECOMMENDATIONS, mode='overwrite')
#     print(results)
    cursor.execute('SELECT id FROM USER WHERE name = %s AND password = %s', (session['name'],
                                                                                           session['password']))
    user_id = cursor.fetchall()[0][0]
    print(user_id)
    cursor.execute('SELECT m.title, r.imdbLink, r.imgLink \
                    FROM QUICK_RECOMMENDATIONS r \
                    JOIN MOVIES m \
                    ON m.id=r.movieId WHERE r.userId=%s', user_id)

    quick_recommendations = cursor.fetchall()
    print(quick_recommendations)
    if len(quick_recommendations) == 0:

        cursor.execute('SELECT movieIds FROM USER WHERE name=%s AND password=%s', (session['name'],
                                                                                           session['password']))
        m_ids = cursor.fetchall()
        m_ids = [int(i) for i in m_ids[0][0].split(',')]

        with fs.open('sparkrecommendationengine/QUICK_RECO_ENGINE_OUTPUTS/V_mult_V_T.npy', 'rb') as numpy_file:
            V_mult_V_T = np.load(numpy_file)

        # V_mult_V_T = np.load('/home/badal/Desktop/Movie Recommendation/Movielens data/V_mult_V_T.npy')
        full_u = np.zeros(V_mult_V_T.shape[0])

        # pf_movies = pd.read_csv('pf_movies.csv')
        # input_m_id_indicies = pf_movies.movieId.apply(lambda x: 1 if x in m_ids else np.nan).dropna().index

        # Since I am using app engine standard env, my code can't includes library that implemented
        # in a language other than Python such as pandas. So instead of pandas I have to use standard csv library.
        input_m_id_indicies = list()
        with fs.open('sparkrecommendationengine/QUICK_RECO_ENGINE_OUTPUTS/pf_movie_ids.pkl', mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for index, rows in enumerate(csv_reader):
                if int(rows['movieId']) in m_ids:
                    input_m_id_indicies.append(index)

        for i in input_m_id_indicies:
            full_u[i] = 5

        # new_user_ratings = np.dot(full_u, V_mult_V_T)
        new_user_ratings = np.dot(full_u, V_mult_V_T)
        new_user_ratings = np.squeeze(np.asarray(new_user_ratings))
        recommended_movie_indicies = new_user_ratings.argsort()[::-1]

        # raw_recommendations = pf_movies.loc[recommended_movie_indicies]['movieId'].astype(str).to_list()

        raw_recommendations = list()
        with fs.open('sparkrecommendationengine/QUICK_RECO_ENGINE_OUTPUTS/pf_movie_ids.pkl', mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for index, rows in enumerate(csv_reader):
                if index in recommended_movie_indicies:
                    raw_recommendations.append(str(rows['movieId']))

        tracker = 0
        recommended_movies = []  # Top 12 recommendations.
        imdb_links = []
        for movie_id in raw_recommendations:
            cursor.execute('SELECT imdbId FROM LINKS WHERE movieId = %s LIMIT 1', movie_id)
            movie_imdb_id = str(cursor.fetchall()[0][0])
            len_ = len(movie_imdb_id)
            id_str = ""
            while len_ < 7:
                id_str += "0"
                len_ += 1
            id_str += movie_imdb_id
            link = 'https://www.imdb.com/title/tt{}/'.format(id_str)

            r = requests.get(link)
            if r.status_code == 200 and int(movie_id) not in m_ids:
                tracker += 1
                recommended_movies.append(movie_id)
                imdb_links.append(link)

            # I want to recommend 12 movies to user. That is why loop breaks if tracker is equal to 12.
            if tracker == 12:
                break
        print('starting to scraping...')
        img_links = list()
        for url in imdb_links:
            r = requests.get(url).text
            soup = BeautifulSoup(r)

            # image source
            for a in soup.find_all('div', attrs={'class': 'poster'}):
                img_links.append(a.find('img').get('src'))

        user_ids = [user_id for _ in range(12)]
        for u_id, m_id, imdb_link, img_link in zip(user_ids, recommended_movies, imdb_links, img_links):
            cursor.execute("INSERT INTO QUICK_RECOMMENDATIONS (userId,movieId,imdbLink,imgLink) VALUES (%s, %s, %s, %s)",
                        (u_id, int(m_id), imdb_link, img_link))
            connection.commit()

    else:
    # cursor.execute('SELECT id FROM USER WHERE name = %s AND password = %s', (name, password))
    # user_id = cursor.fetchall()[0][0]
        print('taking data form DB...')
        cursor.execute('SELECT m.title, r.imdbLink, r.imgLink \
                        FROM QUICK_RECOMMENDATIONS r \
                        JOIN MOVIES m \
                        ON m.id=r.movieId WHERE r.userId=%s', user_id)
        row = cursor.fetchall()

        imdb_links = []
        img_links = []
        recommended_movies = []
        for i in row:
            recommended_movies.append(i[0])
            imdb_links.append(i[1])
            img_links.append(i[2])
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

    # img_links = list()
    # for url in imdb_links:
    #     r = requests.get(url).text
    #     soup = BeautifulSoup(r)
    #
    #     # image source
    #     for a in soup.find_all('div', attrs={'class': 'poster'}):
    #         img_links.append(a.find('img').get('src'))

    return render_template('quick_recos.html',
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


if __name__ == "__main__":

    app.run(host="0.0.0.0", port=8000, debug=True)