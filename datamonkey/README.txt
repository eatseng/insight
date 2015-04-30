#DataMonkey
===========================

###ConferenceViewer data pipeline and ETL system.

####Data Ingestion:

#####Data source for Data Monkey comes from the following places:
*EventBrite API
*Twitter Streaming API
*Flickr API
*Instagram API (depreciated)

######Data from Eventbrite API is fed daily into Amazon Redshift using jobs.insighthouse.eb_loads.eb_daily_events.py 
######Data from Flickr API is fed hourly into Redshift using ETL script from job.insighthouse.etl_scripts.photo_timely.py
######Data from Twitter Streaming goes through several transfromation and is explained in details below

