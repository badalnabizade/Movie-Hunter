# Serverless Movie Recommmendation System
### Application is online in [here](http://sparkrecommendationengine.appspot.com/)
#### Logic behind system:
* User signs up
* main.py assigns a new id to registered user. 
* main.py writes user's id, name, mail and hashed password to USER table.

* User signs in
* Apps checks whether user has selected movies in order to get recommendations.

* If logged in user hasn't selected movies yet in order to get recommendation, app forwards user to movie selection section.
** After user selects movies, app obtains corresponding movie ids and adds these movie ids to current user in USER table.
** Then app checks app checks whether quick recommendations are generated for this user
** if generated, app forwards user to quick recommendations section.

* If logged in user has already selected movies, app checks whether quick recommendations are generated for this loged-in user
* 


* After user selects movies, app obtains corresponding movie ids and adds these movie ids to current user in USER table. 
* App checks whether quick recommendations are generated for currently loged-in user
If quick recommendations are generated already, app obtains 
