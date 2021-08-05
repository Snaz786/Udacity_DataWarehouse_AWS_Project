INTRODUCTION:- SPARKIFY DATAWAREHOUSE ON AWS

The purpose of this project is to build an ETL pipeline ETL pipeline for a database hosted on Redshift that will be able to extract song and log data from an AWS S3 bucket, process the data using Spark and load the data back into s3 as a set of dimensional tables in spark parquet files. This will help analysts to continue finding insights on what their users are listening to.

DATABASE SCHEMA:-

I have created one fact table, SONGPLAYS and four dimensional tables( users, songs, artists and time). This follows the star schema principle which will help the analysts to find the insights they are looking for.

Star Schema model means that it has one Fact Table having business data, and supporting Dimension Tables. The Fact Table answers one of the key questions: what songs users are listening to. 

DB schema is the following:-

https://github.com/Snaz786/Udacity_DataWarehouse_AWS_Project/blob/master/Output_Analytics/Analytics%20Table.JPG

 
 
Staging Tables

1) Staging_events: event data telling what users have done (columns: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)

2) Staging_songs: song data about songs and artists (columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)



PROJECT DATASET:-

The datasets used are retrieved from the S3 bucket and are in the JSON format.

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data

Song data example:   {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


Log data:-

log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

ETL PIPELINE:-

This sparkify datawarehouse project builds an ETL pipeline (Extract, Transform, Load) to create the DB and tables in AWS Redshift cluster, fetch data from JSON files stored in AWS S3, process the data, and insert the data to AWS Redshift DB.
The data gets that gets extracted will need to be transformed to to fit the data model in the target destination tables. For instance the source data for timestamp is in unix forrmat and that will need to be converted to timestamp from which the year, month, day, hour values etc can be extracted which will fit in the target database table schema.

PREREQUISITE:-

Set your AWS access and secret key in the config file.
IAM role with AWS service as Redshift-Customizable and permissions set to S3 read only access.
Create AWS cluster on Redshift by CLI or console.I choosed CLI method to learn the commands.
AWS Redshift set-up
AWS Redshift is used in ETL pipeline as the DB solution. Used set-up in the Project-3 is as follows:

Cluster: 4x dc2.large nodes
Location: US-West-2 (as Project-3's AWS S3 bucket)


EXECUTION:-

1) Execute the script to drop tables, create new dimensionaltable and fact tables and insert queries, by running :create_tables.py

2) Execute the ETL pipeline script by running: python etl.py

3) Validate the output in AWS redshift cluster query editor.


Snapshot of counts of Analytics Tables on AWS redshift cluster.
https://github.com/Snaz786/Udacity_DataWarehouse_AWS_Project/blob/master/Output_Analytics/Output_Redshift.jpeg


1) Artists Table

Get count of rows in Artists table:10025 records

2) Songplays Table

Get count of rows in Songplays table:320 records

3) Songs Table

Get count of rows in Songs table:14896 records

4) Users Table
Get count of rows in Users table:104 records

5) Time Table
 Get count of rows in Iime table:6813 records

6) Staging_Songs Table
 Get count of rows in Staging Songs:14986 records


 
 
