import requests, csv, gcsfs, pymysql, json, logging
from pymysql import OperationalError
import numpy as np
from flask import Flask, render_template, request, redirect, url_for, session
from bs4 import BeautifulSoup
from werkzeug import generate_password_hash, check_password_hash
from os import getenv
pymysql.install_as_MySQLdb()


with open('./credentials.json', 'r') as file:
    credentials = json.load(file)

project_id = getenv('PROJECT_ID')
CONNECTION_NAME = getenv('CloudSQL_Connection_Name')
DB_USER = getenv('CloudSQL_User')
DB_PASSWORD = getenv('CloudSQL_Pass')
DB_NAME = getenv('CloudSQL_DB_Name')
HOST = getenv('CloudSQL_IP')

# Connection to MySQL database
mysql_config = {
    'host': HOST,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'db': DB_NAME,
    'charset': 'utf8mb4',
}

try:
    connection = pymysql.connect(**mysql_config)
except OperationalError:
    # If production settings fail, use local development ones
    mysql_config['unix_socket'] = '/cloudsql/{}'.format(CONNECTION_NAME)
    connection = pymysql.connect(**mysql_config)

# Google Cloud file system.
fs = gcsfs.GCSFileSystem(project=project_id, token=credentials)


def relative_sort(a, b):
    """
    :param a: list_a
    :param b: list_b
    :return: Sorts elements of list_a to have the same relative order as elements of list_b
    """
    dct = {x: i for i, x in enumerate(b)}
    items_in_a = [x for x in a if x in dct]
    items_in_a.sort(key=dct.get)
    it = iter(items_in_a)
    return [next(it) if x in dct else None for x in a]


app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'
app.config['SESSION_TYPE'] = 'memcached'


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        # user inputs from register form.
        name = request.form['uname']
        mail = request.form['mail']
        password = request.form['passw']

        cursor = connection.cursor()
        cursor.execute("SELECT id FROM USER ORDER BY id DESC LIMIT 1")  # last user's id.
        userId = cursor.fetchall()[0][0] + 1  # New user id to assign new registered user.

        # get user names from db.
        cursor.execute('SELECT name FROM USER')
        user_names = cursor.fetchall()
        user_names = [name[0] for name in user_names]

        if name not in user_names:  # if user name is available
            h_password = generate_password_hash(password)  # hashed password.
            # Inserting user informations to USER table.
            cursor.execute('INSERT INTO USER (id,name,mail,password) VALUES (%s, %s, %s, %s)',
                           (userId, name, mail, h_password))
            connection.commit()
            cursor.close()
        else:  # if user name is not available
            return 'this username is not available'
    return render_template("login.html")


@app.route('/decision', methods=['GET', 'POST'])
def decision():
    # user inputs from login form.
    name = request.form['uname']
    password = request.form['passw']

    # Adding name and password to flask session.
    session['name'] = name

    cursor = connection.cursor()
    cursor.execute('SELECT name, password FROM USER WHERE name=%s', name)
    row = cursor.fetchall()  # user names and corresponding hashed passwords
    cursor.close()

    if check_password_hash(row[0][1], password) and name == row[0][0]:  # if password and username are correct.
        cursor = connection.cursor()
        cursor.execute('SELECT movieIds FROM USER WHERE name=%s', name)
        m_ids = cursor.fetchall()[0][0]  # movie ids selected by user.
        cursor.close()

        if not m_ids:  # If user hasn't select movies yet.
            cursor = connection.cursor()
            cursor.execute('SELECT title FROM MOVIES')
            raw_titles = cursor.fetchall()
            cursor.close()
            data = [raw_titles[i][0] for i in range(len(raw_titles))]  # list of titles of all movies in MOVIES table.
            # I will use this movie titles as drop down values in main.html
            return render_template('main.html', data=data)

        else:
            # if user has already selected movies, quick recommendations are generated. Show recommendations page.
            return redirect(url_for('recommendation'))

    else:  # if password or user name is incorrect.
        return 'password or user name is incorrect'


@app.route('/quick_recommendations', methods=['POST', 'GET'])
def recommendation():
    cursor = connection.cursor()
    cursor.execute('SELECT movieIds FROM USER WHERE name=%s', session['name'])  # selected movie ids by current user.
    has_movie_ids = cursor.fetchall()
    if not has_movie_ids[0][0]: # if user doesn't have movie ids, then we are in main.html
        # obtaining titles of selected movies by current user.
        input_json = request.form
        input_title_1 = input_json['first']
        input_title_2 = input_json['second']
        input_title_3 = input_json['third']
        input_title_4 = input_json['fourth']
        input_title_5 = input_json['fifth']

        input_list = [input_title_1,input_title_2,input_title_3,input_title_4,input_title_5]
        movie_ids = list()

        for i in input_list:
            cursor.execute("SELECT id FROM MOVIES WHERE title = %s", i)
            movie_id = cursor.fetchall()[0][0]  # movie ids of selected movies.
            movie_ids.append(str(movie_id))

        cursor.execute("UPDATE USER SET movieIds = %s WHERE name = %s", ([','.join(movie_ids)],
                                                                         session['name']))
        connection.commit()

    cursor.execute('SELECT id FROM USER WHERE name = %s', session['name'])
    user_id = cursor.fetchall()[0][0]  # current user's id.
    cursor.execute('SELECT m.title, r.imdbLink, r.imgLink \
                    FROM QUICK_RECOMMENDATIONS r \
                    JOIN MOVIES m \
                    ON m.id=r.movieId WHERE r.userId=%s', user_id)

    quick_recommendations = cursor.fetchall()  # quick recommendations for corresponding user.

    if len(quick_recommendations) == 0: # if quick recommendations aren't generated already.
        cursor.execute('SELECT movieIds FROM USER WHERE name=%s', session['name'])
        m_ids = cursor.fetchall() # movie ids selected by current user.
        m_ids = [int(i) for i in m_ids[0][0].split(',')]
        # getting Vt matrix generated by quick_recommendation.py file from GCS. See: ./utils/quick_recommendations.py
        with fs.open('sparkrecommendationengine/QUICK_RECO_ENGINE_OUTPUTS/Vt.npy', 'rb') as numpy_file:
            Vt = np.load(numpy_file)

        V_mult_V_T = np.dot(Vt, Vt.T).astype('float16')

        # This array will represents current user's ratings for all movies.
        # For now I will initilalize it with zeros.
        full_u = np.zeros(V_mult_V_T.shape[0])
        input_m_id_indicies = list()  # list that contains indices of user selected movies.

        # getting pf_movie_ids.pkl file from GCS. This file generated by quick_recommendations.py
        # See: ./utils/quick_recommendations.py
        # Since I am using app engine standard env, my code can't includes library that implemented
        # in a language other than Python such as pandas. So instead of pandas I will use standard csv library.
        with fs.open('sparkrecommendationengine/QUICK_RECO_ENGINE_OUTPUTS/pf_movie_ids.pkl', mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            movie_id_dct = {}  # movie_id_dict ==> {index: movie_id}
            for rows in csv_reader:
                movie_id_dct[int(rows[''])] = rows['movieId']
                if int(rows['movieId']) in m_ids:
                    input_m_id_indicies.append(int(rows['']))

        # update values of full_u array with 5 where corresponding indices equals to indices of user select movies.
        for i in input_m_id_indicies:
            full_u[i] = 5

        new_user_ratings = np.dot(full_u, V_mult_V_T)  # this array assigns scores for each movies.
        new_user_ratings = np.squeeze(np.asarray(new_user_ratings))
        recommended_movie_indicies = new_user_ratings.argsort()[::-1]  # sorting indices of scores in descending order.

        sorted_csv_indices = relative_sort(movie_id_dct.keys(), recommended_movie_indicies)  # See docstring of relative_sort function.
        raw_recommendations = [movie_id_dct[i] for i in sorted_csv_indices]  # movie ids of corresponding sorted indices

        tracker = 0
        recommended_movies = []  # Top 12 recommendations.
        imdb_links = []
        for movie_id in raw_recommendations:
            cursor.execute('SELECT imdbId FROM LINKS WHERE movieId = %s LIMIT 1', movie_id)
            movie_imdb_id = str(cursor.fetchall()[0][0]) # imdb id of recommended movie
            len_ = len(movie_imdb_id)
            id_str = ""
            while len_ < 7:
                id_str += "0"
                len_ += 1
            id_str += movie_imdb_id
            link = 'https://www.imdb.com/title/tt{}/'.format(id_str)

            r = requests.get(link)
            # if imdb link of recommended movie exsists and this movie is not alreadt selected by user
            if r.status_code == 200 and int(movie_id) not in m_ids:
                tracker += 1
                recommended_movies.append(movie_id)
                imdb_links.append(link)

            # I want to recommend 12 movies to user. That is why loop breaks if tracker is equal to 12.
            if tracker == 12:
                break

        img_links = list()
        for url in imdb_links:
            r = requests.get(url).text
            soup = BeautifulSoup(r)

            # image source
            for a in soup.find_all('div', attrs={'class': 'poster'}):
                img_links.append(a.find('img').get('src'))

        user_ids = [user_id for _ in range(12)]

        for u_id, m_id, imdb_link, img_link in zip(user_ids, recommended_movies, imdb_links, img_links):
            cursor.execute("INSERT INTO QUICK_RECOMMENDATIONS (userId,movieId,imdbLink,imgLink) VALUES (%s,%s,%s,%s)",
                        (u_id, int(m_id), imdb_link, img_link))
            connection.commit()

    else: # if quick recommendations have been generated
        # Obtaining those recommendations from db.
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


@app.route('/best_movies')
def best_movies():
    return render_template('best_movies.html')


@app.route('/personal_recos', methods=['POST', 'GET'])
def personal_recos():
    cursor = connection.cursor()
    cursor.execute('SELECT id FROM USER WHERE name = %s', session['name'])
    user_id = cursor.fetchall()[0][0] # User if of current user.

    cursor.execute('SELECT m.title, r.imdbLink, r.imgLink \
                    FROM RECOMMENDATIONS r \
                    JOIN MOVIES m \
                    ON m.id=r.movieId WHERE r.userId=%s', user_id)
    # Corresponding recommendations. These recommendations are generated by engine.py file. See: ./utils/engine.py
    recommendations = cursor.fetchall()

    if len(recommendations) == 0:  # if recommendations haven't been generated already
        # Notify to user.
        return render_template('notify_user.html', name=session['name'].capitalize())

    else:
        imdb_links = []
        img_links = []
        recommended_movies = []
        for i in recommendations:
            recommended_movies.append(i[0])
            imdb_links.append(i[1])
            img_links.append(i[2])

        return render_template('personal_recos.html',
                               movie_1=recommended_movies[0], link_1=imdb_links[0], img_src_1=img_links[0],
                               movie_2=recommended_movies[1], link_2=imdb_links[1], img_src_2=img_links[1],
                               movie_3=recommended_movies[2], link_3=imdb_links[2], img_src_3=img_links[2],
                               movie_4=recommended_movies[3], link_4=imdb_links[3], img_src_4=img_links[3],
                               movie_5=recommended_movies[4], link_5=imdb_links[4], img_src_5=img_links[4],
                               movie_6=recommended_movies[5], link_6=imdb_links[5], img_src_6=img_links[5],
                               movie_7=recommended_movies[6], link_7=imdb_links[6], img_src_7=img_links[6],
                               movie_8=recommended_movies[7], link_8=imdb_links[7], img_src_8=img_links[7],
                               movie_9=recommended_movies[8], link_9=imdb_links[8], img_src_9=img_links[8],
                               movie_10=recommended_movies[9], link_10=imdb_links[9], img_src_10=img_links[9],
                               movie_11=recommended_movies[10], link_11=imdb_links[10], img_src_11=img_links[10],
                               movie_12=recommended_movies[11], link_12=imdb_links[11], img_src_12=img_links[11],
                               movie_13=recommended_movies[12], link_13=imdb_links[12], img_src_13=img_links[12],
                               movie_14=recommended_movies[13], link_14=imdb_links[13], img_src_14=img_links[13],
                               movie_15=recommended_movies[14], link_15=imdb_links[14], img_src_15=img_links[14],
                               movie_16=recommended_movies[15], link_16=imdb_links[15], img_src_16=img_links[15],
                               movie_17=recommended_movies[16], link_17=imdb_links[16], img_src_17=img_links[16],
                               movie_18=recommended_movies[17], link_18=imdb_links[17], img_src_18=img_links[17],
                               movie_19=recommended_movies[18], link_19=imdb_links[18], img_src_19=img_links[18],
                               movie_20=recommended_movies[19], link_20=imdb_links[19], img_src_20=img_links[19],
                               movie_21=recommended_movies[20], link_21=imdb_links[20], img_src_21=img_links[20],
                               movie_22=recommended_movies[21], link_22=imdb_links[21], img_src_22=img_links[21],
                               movie_23=recommended_movies[22], link_23=imdb_links[22], img_src_23=img_links[22],
                               movie_24=recommended_movies[23], link_24=imdb_links[23], img_src_24=img_links[23],
                               movie_25=recommended_movies[24], link_25=imdb_links[24], img_src_25=img_links[24],
                               movie_26=recommended_movies[25], link_26=imdb_links[25], img_src_26=img_links[25],
                               movie_27=recommended_movies[26], link_27=imdb_links[26], img_src_27=img_links[26],
                               movie_28=recommended_movies[27], link_28=imdb_links[27], img_src_28=img_links[27],
                               movie_29=recommended_movies[28], link_29=imdb_links[28], img_src_29=img_links[28],
                               movie_30=recommended_movies[29], link_30=imdb_links[29], img_src_30=img_links[29],
                               movie_31=recommended_movies[30], link_31=imdb_links[30], img_src_31=img_links[30],
                               movie_32=recommended_movies[31], link_32=imdb_links[31], img_src_32=img_links[31],
                               movie_33=recommended_movies[32], link_33=imdb_links[32], img_src_33=img_links[32],
                               movie_34=recommended_movies[33], link_34=imdb_links[33], img_src_34=img_links[33],
                               movie_35=recommended_movies[34], link_35=imdb_links[34], img_src_35=img_links[34],
                               movie_36=recommended_movies[35], link_36=imdb_links[35], img_src_36=img_links[35],
                               movie_37=recommended_movies[36], link_37=imdb_links[36], img_src_37=img_links[36],
                               movie_38=recommended_movies[37], link_38=imdb_links[37], img_src_38=img_links[37],
                               movie_39=recommended_movies[38], link_39=imdb_links[38], img_src_39=img_links[38],
                               movie_40=recommended_movies[39], link_40=imdb_links[39], img_src_40=img_links[39],
                               movie_41=recommended_movies[40], link_41=imdb_links[40], img_src_41=img_links[40],
                               movie_42=recommended_movies[41], link_42=imdb_links[41], img_src_42=img_links[41],
                               movie_43=recommended_movies[42], link_43=imdb_links[42], img_src_43=img_links[42],
                               movie_44=recommended_movies[43], link_44=imdb_links[43], img_src_44=img_links[43],
                               movie_45=recommended_movies[44], link_45=imdb_links[44], img_src_45=img_links[44],
                               movie_46=recommended_movies[45], link_46=imdb_links[45], img_src_46=img_links[45],
                               movie_47=recommended_movies[46], link_47=imdb_links[46], img_src_47=img_links[46],
                               movie_48=recommended_movies[47], link_48=imdb_links[47], img_src_48=img_links[47])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
