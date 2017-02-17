import dataiku
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
import pandas as pd
import requests
import json

# Recipe inputs
artist_dataset_name = get_input_names_for_role('artist_list')[0]
artists = dataiku.Dataset(artist_dataset_name)
artists_df = artists.get_dataframe()

input_api_key = get_recipe_config()['api_key']
input_id_col = get_recipe_config()['artist_ids']

track_df = pd.DataFrame(columns=['track_name','track_id','artist_name', 'artist_id','album_name','track_rating','num_favourite','lyrics_id','spotify_id',
                                 'lyrics_list', 'explicit', 'instrumental'])
#track_df = pd.DataFrame(columns=['track_name','track_id','artist_name', 'artist_id','album_name','track_rating','track_length','num_favourite','lyrics_id','spotify_id'])
#track_df = pd.DataFrame(columns=['track_id','track_name','artist_name', 'artist_id','album_name','track_rating','num_favourite','lyrics_id','spotify_id'])

name_col = []
id_col = []
artist_name_col = []
artist_id_col = []
album_col = []
rating_col = []
length_col = []
favourite_col = []
lyrics_col = []
spotify_col = []
lyrics_text_col = []
explicit_col = []
instrumental_col = []

class Track:
    
    def __init__(self, track_name, track_id, artist, artist_id, album_name, track_rating, track_length, num_favourite, lyrics_id, track_spotify_id):
        self.track_name = track_name
        self.track_id = track_id
        self.artist = artist
        self.artist_id = artist_id
        self.album_name = album_name
        self.track_rating = track_rating
        self.track_length = track_length
        self.num_favourite = num_favourite
        self.lyrics_id = lyrics_id
        self.track_spotify_id = track_spotify_id
        
    def printTrack(t):
        print "Track name: %s" %t.track_name
        print "Track id: %s" %t.track_id
        print "Artist name %s" %t.artist
        print "Artist id %s" %t.artist_id
        print "Album name: %s" %t.album_name
        print "Track rating: %s" %t.track_rating
        print "Tracks length %s" %t.track_length
        print "Num favourite %s" %t.num_favourite
        print "Lyrics id %s" %t.lyrics_id
        print "Spotify id %s" %t.track_spotify_id
        
parsed_track_list = []

def build_request_string(page_num, artistID, apiKey):
    endpoint = 'http://api.musixmatch.com/ws/1.1/'
    method = 'track.search'
    query_string = 'f_artist_id='+str(artistID)+'&page_size=100&f_has_lyrics=1'

    request_string = endpoint+method+'?apikey='+apiKey+'&'+query_string+'&page='+str(page_num)
    return request_string

onlyID = artists_df[input_id_col]

for artistId in onlyID:
    again = True
    page_num=1
    while again:
        request_string = build_request_string(page_num, artistId, input_api_key)
        print 'Starting request %s' %str(page_num)
        print 'Sended request: %s' %request_string
        contents = requests.get(request_string)
        print 'Answer code %s' %contents.status_code
        d = json.loads(contents.content)
        track_list = d['message']['body']['track_list']
        if len(track_list) == 0:
            again = False
        else:
            for track in track_list:
                parsed_track_list.append(Track(track['track']['track_name'], 
                                               track['track']['track_id'],
                                               track['track']['artist_name'],
                                               track['track']['artist_id'],
                                               track['track']['album_name'],
                                               track['track']['track_rating'],
                                               track['track']['track_length'],
                                               track['track']['num_favourite'],
                                               track['track']['lyrics_id'],
                                               track['track']['track_spotify_id']
                                               ))
            page_num +=1
            print 'Complete request %s' %request_string
            
    for t in parsed_track_list:
        name_col.append(t.track_name)
        id_col.append(t.track_id)
        artist_name_col.append(t.artist)
        artist_id_col.append(t.artist_id)
        album_col.append(t.album_name)
        rating_col.append(t.track_rating)
        length_col.append(t.track_length)
        favourite_col.append(t.num_favourite)
        lyrics_col.append(t.lyrics_id)
        spotify_col.append(t.track_spotify_id)
    parsed_track_list = []

track_df.track_name = name_col
track_df.track_id = id_col
track_df.artist_name = artist_name_col
track_df.artist_id = artist_id_col
track_df.album_name = album_col
track_df.track_rating = rating_col
track_df.track_length = length_col
track_df.num_favourite = favourite_col
track_df.lyrics_id = lyrics_col
track_df.spotify_id = spotify_col

def build_lyrics_request(apiKey, trackId):
    endpoint = 'http://api.musixmatch.com/ws/1.1/'
    method = 'track.lyrics.get'
    query_string = 'track_id='+str(trackId)

    request_string = endpoint+method+'?apikey='+str(apiKey)+'&'+query_string
    
    return request_string

print "Starting lyrics request"
lyrics_list = []
explicit_list = []
instrumental_list = []

track_request_num = 1
for track_id in id_col:
    request_string = build_lyrics_request(input_api_key, track_id)
    print 'Starting track request %s' %str(track_request_num)
    print 'Sended track request: %s' %str(track_request_num)
    contents = requests.get(request_string)
    print 'Answer code %s' %contents.status_code
    d = json.loads(contents.content)
    
    lyrics_object = d['message']['body']['lyrics']
    lyrics_list.append(lyrics_object['lyrics_body'])
    explicit_list.append(lyrics_object['explicit'])
    instrumental_list.append(lyrics_object['instrumental'])
    
    track_request_num +=1
    
track_df.lyrics_list = lyrics_list
track_df.explicit = explicit_list
track_df.instrumental = instrumental_list

# Recipe outputs
tracks_output_name = get_output_names_for_role('artists_tracks')[0]
tracks = dataiku.Dataset(tracks_output_name)
tracks.write_with_schema(track_df)
