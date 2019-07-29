from flask import Flask,render_template,flash, redirect,url_for,session,logging,request


app = Flask(__name__)

@app.route("/")
def test():
   return render_template('index.html')


if __name__ == "__main__":

   app.run(debug=True)