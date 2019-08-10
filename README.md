# Serverless Movie Recommmendation System
## Application is online in [here](http://sparkrecommendationengine.appspot.com/)
### How it Works:
* 1 User signs up
* 1.1. [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L77-L79) assigns a new id to registered user. 
* 1.2. [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L86-L94) writes user's id, name, mail and hashed password to USER table.

* 2 User signs in
* 2.1. [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L118-L125) checks whether user has selected movies in order to get recommendations.

* 2.2. If logged in user hasn't selected movies yet in order to get recommendation, [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L118-L125) forwards user to movie selection section (main.html).
* 2.2.1. After user selects movies, [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L150-L159) obtains corresponding movie ids and adds these movie ids to current user in USER table.
* 2.2.2. Then [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L168-L170) checks whether quick recommendations are generated for this user
* 2.2.3. If those are generated, [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/06b7e7092bc1a88460971844224494ac0c773136/app/main.py#L249-L265) forwards user to quick recommendations section (quick_recos.html)
* 2.2.4. Otherwise, [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/06b7e7092bc1a88460971844224494ac0c773136/app/main.py#L170-L247) generates 12 quick recommendations on the fly, writes this recommendations to QUICK_RECOMMENDATIONS table with corresponding user's id,  then directs user to quick recommendations section (quick_recos.html) and show those recommendations. __For more detail, See: [quick_reco_engine.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/utils/quick_reco_engine.py) and [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/app/main.py)__
 
* 2.3. If logged in user has already selected movies, main.py repeats step 2.2.2, 2.2.3 and 2.2.4 (but doesn't overwrite to QUICK_RECOMMENDATIONS table).

* 2.4. In every 15 minutes Google Cloud Scheduler runs [dataproc_manager.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/utils/dataproc_manager.py) 
* 2.4.1. [run_job](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/utils/dataproc_manager.py#L197-L202) function in dataproc_manager.py checks whether users have selected movies in USER table and whether spark job for these users has been submited to cluster. 
* 2.4.1.1. After that, [run_job function](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/utils/dataproc_manager.py#L203-L211) creates cluster and submits engine.py file to this cluster for users who have selected movies and whose spark job hasn't been submited already.    
* 2.4.1.2. engine.py file selects users from USER TABLE who have selected movies and whose spark job hasn't been submited to cluster. Then generates 48 recommendations for these users, changes value of "job_submited" column to 'done' for these users in USER table. After generating recommendations process is finished, engine.py writes results to RECOMMENDATION table. __For more details, See: [engine.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/utils/engine.py)__   
* 2.4.2. If there is no user whose spark job hasn't been submited to cluster, run_job function does nothing.

* 2.5. While recommendations are under generating process, main.py shows notify.html to users who want to see recommendations. If this process is finished, main.py directs user to recommendations.html and show recommendations.

**Note 1:** step 2.2.4. might take up to 1 minute.<br/>
**Note 2:** generating recommendations might take up to 20 minutes.<br/>
**Note 3:** movies in best_movies.html are obtained from [Notebook.ipynb](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/Notebook.ipynb)
