'''
Created on: Nov 24, 2019

Author: Napoleon de Mesa

Version: 1.0
'''
import requests
import sys
import json
import pandas as pd

def _envget():
    file_contents = []
    credentials = {}
    try:
        with open('./credentials/secret.txt') as file:
            for line in file:
                file_contents.append(line.strip())
            credentials['TWITCH_CLIENT_ID'] = file_contents[0]
    except Exception as e:
        print(e)

    return credentials

def main():
    overwatch_raw_data = {'user_name': [], 'user_id': [], 'broadcast_id': [], 'stream_start_time': [], 'viewer_count': []}
    credentials = _envget()
    try:
        session = requests.Session()
        session.headers.update({'Client-ID': credentials['TWITCH_CLIENT_ID']})
    except Exception as e:
        print(e)
    try:
        top_overwatch_streams_first_page = fetch_top_overwatch_streams(session)
        populate_dictionary(top_overwatch_streams_first_page[0], overwatch_raw_data)
        top_overwatch_streams_second_page = fetch_top_overwatch_streams(session, top_overwatch_streams_first_page[1])
        populate_dictionary(top_overwatch_streams_second_page[0], overwatch_raw_data)
        overwatch_stream_data = pd.DataFrame.from_dict(overwatch_raw_data)
        print(overwatch_stream_data)
    except Exception as e:
        print(e)

def populate_dictionary(overwatch_streams, overwatch_raw_data):
    for stream in overwatch_streams:
        overwatch_raw_data['user_name'].append(stream['user_name'])
        overwatch_raw_data['user_id'].append(stream['user_id'])
        overwatch_raw_data['broadcast_id'].append(stream['id'])
        overwatch_raw_data['stream_start_time'].append(stream['started_at'])
        overwatch_raw_data['viewer_count'].append(stream['viewer_count'])

    return overwatch_raw_data

def fetch_top_overwatch_streams(session, cursor=None):
    twitch_game_response = session.get('https://api.twitch.tv/helix/streams', params = {
        'game_id': 488552,
        'first':  100,
        'after': cursor
    })
    twitch_game_response = twitch_game_response.json()
    overwatch_streams = list(twitch_game_response.values())[0]
    starting_cursor = list(twitch_game_response.values())[1]
    
    return overwatch_streams, starting_cursor

if __name__ == '__main__':
    main()