Mycroft
=======
Mycroft is an orchestrator that coordinates MRJob, S3, and Redshift to automatically perform light transformations on daily log data.  Just specify a cluster, schema version, s3 path, and start date, and Mycroft will watch S3 for new data, transforming and loading data without user action.  Mycroft's web interface can be used to monitor the progress of in-flight data loading jobs, and to pause, resume, cancel or delete existing jobs.  Mycroft will notify via email when new data is successfully loaded or if any issues arise.  It also provides tools to automatically generate schemas from log data, and even manages the expiration of old data as well as vacuuming and analyzing data.

Getting Started with Mycroft
----------------------------
A comprehensive [Quickstart](https://docs.google.com/document/u/0/d/14PgUJI5fHKm_NReuNxn_ij68zrSET_JiBCwY-Ktu2q8/pub) is available for getting up and running.  Just clone the repository and follow the guide to create a Redshift cluster and the AWS services that Mycroft depends on, start running the Mycroft service, and then load some simple example JSON data.
