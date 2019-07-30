# Serverless Movie Recommmendation System
## Application is online in [here](http://sparkrecommendationengine.appspot.com/)
### How it Works:
* 1 User signs up
* 1.1. main.py assigns a new id to registered user. 
* 1.2. main.py writes user's id, name, mail and hashed password to USER table.

* 2 User signs in
* 2.1. Apps checks whether user has selected movies in order to get recommendations.

* 2.2. If logged in user hasn't selected movies yet in order to get recommendation, app forwards user to movie selection section.
* 2.2.1. After user selects movies, app obtains corresponding movie ids and adds these movie ids to current user in USER table.
* 2.2.2. Then app checks whether quick recommendations are generated for this user
* 2.2.3. If those are generated, app forwards user to quick recommendations section.
* 2.2.4. Otherwise, app generates 12 quick recommendations on the fly, writes this recommendations to QUICK_RECOMMENDATIONS table with corresponding user's id,  then app directs user to quick recommendations section and show those recommendations. __For more detail, See: ./utils/quick_recommendations and ./app/main.py__
 
* 2.3. If logged in user has already selected movies, app repeats step 2.2.2, 2.2.3 and 2.2.4 (but doesn't overwrite to QUICK_RECOMMENDATIONS table).

* 2.4. In every 15 minutes Google Cloud Scheduler runs dataproc_manager.py 
* 2.4.1. run_job function in dataproc_manager.py checks whether users have selected movies in USER table and whether spark job for these users has been submited to cluster. 
* 2.4.1.1. After that, run_job function creates cluster and submits engine.py file to this cluster for users who have selected movies and whose spark job hasn't been submited already.    
* 2.4.1.2. engine.py file selects users from USER TABLE who have selected movies and whose spark job hasn't been submited to cluster. Then generates recommendations for these users, changes value of "job_submited" column to 'done' for these users in USER table. After generating recommendations process is finished, engine.py writes results to RECOMMENDATION table. __For more details, See: ./utils/engine.py__   
* 2.4.2. If there is no user whose spark job hasn't been submited to cluster, run_job function does nothing. 

