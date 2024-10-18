create database spotify_db;

-- create or replace storage integration s3_init
--     TYPE= EXTERNAL_STAGE
--     STORAGE_PROVIDER = S3
--     ENABLED = TRUE
--     STORAGE_AWS_ROLE_ARN = "arn:aws:iam:xxxxxxx:role/snowflake-s3-connection"
--     STORAGE_ALLOWED_LOCATION = ('s3://spotify-daily-data-project')
--         COMMENT = 'Creating connection to S3'
        


DESC integration s3_init;

CREATE OR REPLACE file format csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header =1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

CREATE OR REPLACE stage spotify_stage
URL= 's3://spotify-daily-data-project/transformed_data'
STORAGE_INTEGRATION = s3_init
FILE_FORMAT = csv.fileformat

LIST @spotify_stage/songs;

CREATE OR REPLACE table tbl_album(
album_id STRING,
name STRING,
release_date DATE,
total_tracks INT,
url STRING
)

CREATE OR REPLACE TABLE tbl_artists (
artist_id STRING,
name STRING,
url STRING)

CREATE OR REPLACE TABLE tbl_songs(
song_id STRING,
song_name STRING,
duration_ms INT,
url STRING,
popularity INT,
song_added DATE,
album_id STRING,
artist_id STRING)
    
Select * from tbl_songs;


--copying the data from s3 to the table album
 COPY INTO tbl_songs
 FROM @spotify_stage/songs/songs_transformed_2024-03-08/run-170987654434-part-r-0000;


--create snowpipe for automation

CREATE OR REPLACE SCHEMA pipe;

CREATE OR REPLACE pipe pipe.tbl_songs_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.public.tbl_songs --whenever a new file is uploaded, this will auto-ingest the data from the songs folder
FROM @spotify_db.public.spotify_stage/songs/;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_artists_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.public.tbl_artists --whenever a new file is uploaded, this will auto-ingest the data from the songs folder
FROM @spotify_db.public.spotify_stage/artist/;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_album_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.public.tbl_album --whenever a new file is uploaded, this will auto-ingest the data from the songs folder
FROM @spotify_db.public.spotify_stage/album/;


--we will get the arn from the notification channel of the pipe and then
--go to s3 bucket and create a event notification - SQS Queue and add the arn there
--so whenever new data get added from the cloud watch trigger and get into transformed bucket via lambda and glue (for transformation) the pipe of the snowflake will take the data and store it in the respective tables.

--shows the status of the pipe, running stage and what ingestion it got and any 
--error can also be tackled.
SELECT SYSTEM$PIPE_STATUS('pipe.tbl_songs_pipe');



         