import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data):
    album_list = []
    for record in data['items']:
        album_id = record['track']['album']['id']
        album_name = record['track']['album']['name']
        album_release_date = record['track']['album']['release_date']
        album_total_tracks = record['track']['album']['total_tracks']
        album_url = record['track']['album']['external_urls']['spotify']
    
        album_elements = {'album_id':album_id, 'album_name':album_name, 'release_date':album_release_date, 
                          'total_tracks':album_total_tracks, 'url':album_url}
        album_list.append(album_elements)
    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_id = artist['id']
                    artist_name = artist['name']
                    artist_url = artist['external_urls']['spotify']
                    
                    artist_elements= {'artist_id': artist_id, 'artist_name': artist_name, 'external_url': artist_url}
                    
                    artist_list.append(artist_elements)
    return artist_list
            
def track(data):
    track_list = []
    for row in data['items']:
        track_id = row['track']['id']
        track_name = row['track']['name']
        track_duration = row['track']['duration_ms']
        track_popularity = row['track']['popularity']
        track_added = row['added_at']
        track_url = row['track']['external_urls']['spotify']
        album_id = row['track']['album']['id']
        artist_id = row['track']['artists'][0]['id']
        
        track_elements = {'track_id': track_id, 'track_name':track_name, 'track_duration' : track_duration, 
                         'track_popularity':  track_popularity, 'track_added': track_added, 'track_url' : track_url,
                         'album_id': album_id, 'artist_id': artist_id
                         }
        
        track_list.append(track_elements)
    return track_list


def lambda_handler(event, context):
    s3 = boto3.client('s3')                                             # creating the object s3
    Bucket = "spotify-etl-project-saheen"
    Key = "raw_data/to_be_processed/"
    
    spotify_key = []
    spotify_data = []
    for file in s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':                           # Proceeds onlt if it is a .json file
            response = s3.get_object(Bucket = Bucket, Key = file_key)   # response gets the object info
            content = (response['Body'])                                # the body of the object is assigned to 'content'
            jsonobject = json.loads(content.read())                     # jsonobject will have the content in json format
            spotify_key.append(file_key)
            spotify_data.append(jsonobject)
            
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        track_list = track(data)
        
        
        # Converting the data lists to DataFrame
        df_album_list = pd.DataFrame(album_list)
        df_artist_list = pd.DataFrame(artist_list)
        df_track_list = pd.DataFrame(track_list)
        
        # Converting the object type to datetime format
        df_album_list['release_date'] = pd.to_datetime(df_album_list['release_date'])
        df_track_list['track_added'] = pd.to_datetime(df_track_list['track_added'])
        
        # Deleting Duplicate Records
        df_album_list = df_album_list.drop_duplicates(subset=['album_id'])
        df_artist_list = df_artist_list.drop_duplicates(subset=['artist_id'])
        
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        df_album_list.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = album_key, Body = album_content)
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        df_artist_list.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = artist_key, Body = artist_content)
        
        track_key = "transformed_data/track_data/track_transformed_" + str(datetime.now()) + ".csv"
        track_buffer = StringIO()
        df_track_list.to_csv(track_buffer, index=False)
        track_content = track_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = track_key, Body = track_content)
        
        
    s3_resource = boto3.resource('s3')
    for key in spotify_key:
        copy_source = {
            'Bucket':Bucket,
            'Key':key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()