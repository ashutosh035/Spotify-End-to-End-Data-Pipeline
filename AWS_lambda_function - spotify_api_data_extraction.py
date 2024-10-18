import json
import os
#We have to manually upload the spotipy 
#library to the lamba layers as we can't install it using the 
#pip install, so layers will do this.
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
#boto3 package to communicate with AWS S3 and we can load the data into bucket from the API extraction.
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    # to get the client id from the environmental variables
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    # uri is the playlist id, which has to be extracted from the url
    playlist_uri = playlist_link.split("/")[-1]
    
    data = sp.playlist_tracks(playlist_uri)
    
    # the package boto3 is used to connect between aws environments
    client = boto3.client('s3')
    #file name to be stored into the S3
    file_name = "spotify_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket='spotify-etl-project-saheen',
        #key= path where to store the data
        Key='raw_data/to_be_processed/' + file_name,
        #the data to be dump, being into json string.
        Body=json.dumps(data)                           #json.dumps() will convert the subset of a (data) objects into a json string.
        )
    #adding the glue client to the data extract after the lambda extract
    #using the boto3 library to trigger the code

    glue = boto3.client("glue")
    gluejobname="spotify_tranformation_job"

    try:
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName = gluejobname, RunId= runId['JobRunId'])
        print("Job Status: ",status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)

    #now this will add a job on the glue platform from the s3 using thr boto3 trigger library and then
    #glue will transform the data as coded into table in the s3 bucket and then from there the snowflake pipe will get
    #data from the s3 into DB and from there we can query the table to get the insights which then get vizualized using PowerBI.
    #we will add the trigger on the spotify data extract lambda function using the eventbridge-cloudwatch events on top to 
    #automate the whole thing and it gets trigger on daily basis, updating the data and tables for real-time analysis.