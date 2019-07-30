# Serverless Movie Recommmendation System
### Application is online in [here](http://sparkrecommendationengine.appspot.com/)
#### Logic behind system:
* 1. User signs up
* 1.1. main.py assigns a new id to registered user. 
* 1.2. main.py writes user's id, name, mail and hashed password to USER table.

* 2. User signs in
* 2.1. Apps checks whether user has selected movies in order to get recommendations.

* 2.2. If logged in user hasn't selected movies yet in order to get recommendation, app forwards user to movie selection section.
* 2.2.1. After user selects movies, app obtains corresponding movie ids and adds these movie ids to current user in USER table.
* 2.2.2. Then app checks app checks whether quick recommendations are generated for this user
* 2.2.3. If those are generated, app forwards user to quick recommendations section.
* 2.2.4. Otherwise, app generates 12 quick recommendations on the fly, then directs user to quick recommendations section and show those recommendations.   

* 2.3. If logged in user has already selected movies, app checks whether quick recommendations are generated for this loged-in user
* 


* After user selects movies, app obtains corresponding movie ids and adds these movie ids to current user in USER table. 
* App checks whether quick recommendations are generated for currently loged-in user
If quick recommendations are generated already, app obtains 
