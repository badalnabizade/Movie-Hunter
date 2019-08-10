# NOTE: OPTIMEZE Best movies section in NOTEBOOK, especially proc_data_for_model function.

# Serverless Movie Recommmendation System
## Application is online in [here](http://sparkrecommendationengine.appspot.com/)
### How it Works:
* 1 User signs up
* 1.1. [main.py assigns a new id to registered user](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L77-L79). 
* 1.2. [main.py writes user's id, name, mail and hashed password to USER table](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L86-L94).

* 2 User signs in
* 2.1. [main.py checks whether user has selected movies in order to get recommendations](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L118-L125).

* 2.2. If logged in user hasn't selected movies yet in order to get recommendation, [main.py forwards user to movie selection section](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L118-L125) (main.html).
* 2.2.1. After user selects movies, [main.py obtains corresponding movie ids and adds these movie ids to current user in USER table](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L150-L159).
* 2.2.2. Then [main.py checks whether quick recommendations are generated for this user](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/app/main.py#L168-L170)
* 2.2.3. If those are generated, [main.py forwards user to quick recommendations section](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/06b7e7092bc1a88460971844224494ac0c773136/app/main.py#L249-L265) (quick_recos.html)
* 2.2.4. Otherwise, [main.py generates 12 quick recommendations on the fly, writes this recommendations to QUICK_RECOMMENDATIONS table with corresponding user's id,  then directs user to quick recommendations section (quick_recos.html) and show those recommendations](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/06b7e7092bc1a88460971844224494ac0c773136/app/main.py#L170-L247). __For more detail, See: [quick_reco_engine.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/utils/quick_reco_engine.py) and [main.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/app/main.py)__
 
* 2.3. If logged in user has already selected movies, main.py repeats step 2.2.2, 2.2.3 and 2.2.4 (but doesn't overwrite to QUICK_RECOMMENDATIONS table).

* 2.4. [In every 15 minutes Google Cloud Scheduler runs dataproc_manager.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/utils/dataproc_manager.py) 
* 2.4.1. [run_job function in dataproc_manager.py checks whether users have selected movies in USER table and whether spark job for these users has been submited to cluster](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/utils/dataproc_manager.py#L197-L202). 
* 2.4.1.1. After that, [run_job function creates cluster and submits engine.py file to this cluster for users who have selected movies and whose spark job hasn't been submited already](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/b54349ee9f153773a8be4ec3cb14ac844f99f5bb/utils/dataproc_manager.py#L203-L211).    
* 2.4.1.2. [engine.py file selects users from USER TABLE who have selected movies and whose spark job hasn't been submited to cluster. Then generates 48 recommendations for these users, changes value of "job_submited" column to 'done' for these users in USER table](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/70a3a797a0e7918de1c40762d2bbe112a95cf20a/utils/engine.py#L119-L192). After generating recommendations process is finished, [engine.py writes results to RECOMMENDATION table](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/70a3a797a0e7918de1c40762d2bbe112a95cf20a/utils/engine.py#L194-L197). __For more details, See: [engine.py](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/utils/engine.py)__   
* 2.4.2. If there is no user whose spark job hasn't been submited to cluster, run_job function does nothing.

* 2.5. While recommendations are under generating process, [main.py shows notify.html to users who want to see recommendations](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/06b7e7092bc1a88460971844224494ac0c773136/app/main.py#L298-L300). If this process is finished, [main.py directs user to recommendations.html and show recommendations.](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/06b7e7092bc1a88460971844224494ac0c773136/app/main.py#L302-L311)

**Note 1:** step 2.2.4. might take up to 1 minute.<br/>
**Note 2:** generating recommendations might take up to 20 minutes.<br/>
**Note 3:** movies in best_movies.html are obtained from [Notebook.ipynb](https://github.com/badalnabizade/MovieHunter-Recommendation-Engine/blob/master/Notebook.ipynb)
