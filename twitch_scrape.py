'''
Created on: Nov 24, 2019

Author: Napoleon de Mesa

Version: 1.0
'''
import requests
import sys
import json
import datetime
import pandas as pd
from sqlalchemy import create_engine, VARCHAR, BIGINT, INT, DateTime

PULL_TIME = datetime.datetime.now()
CLEAN_TIME = PULL_TIME.strftime('%Y-%m-%d %H:%M:%S')
YEAR = PULL_TIME.strftime('%Y')
MONTH = PULL_TIME.strftime('%m')

def _envget():
    file_contents = []
    credentials = {}
    try:
        with open('./credentials/secret.txt') as file:
            for line in file:
                file_contents.append(line.strip())
            credentials['TWITCH_CLIENT_ID'] = file_contents[0]
            credentials['TWITCH_CLIENT_SECRET'] = file_contents[1]
            credentials['DB_USER'] = file_contents[2]
            credentials['DB_PASS'] = file_contents[3]
            credentials['DB_HOST'] = file_contents[4]
            credentials['DB_PORT'] = file_contents[5]
            credentials['DB_NAME'] = file_contents[6]
    except Exception as e:
        print(e)

    return credentials


def check_auth(credentials, session):
        twitch_game_response = session.post('https://id.twitch.tv/oauth2/token', params = {
            'client_id': credentials['TWITCH_CLIENT_ID'],
            'client_secret': credentials['TWITCH_CLIENT_SECRET'],
            'grant_type': 'client_credentials'
        })
        twitch_game_response = twitch_game_response.json()
        access_token = twitch_game_response['access_token']

        return access_token


def populate_dictionary(streams, stream_raw_data):
    for stream in streams:
        stream_start_time = str(stream['started_at'][:10]) + ' ' + str(stream['started_at'][11:19])
        stream_raw_data['id'].append(str(stream['id'] + '-' + CLEAN_TIME))
        stream_raw_data['user_id'].append(stream['user_id'])
        stream_raw_data['user_name'].append(stream['user_name'])
        stream_raw_data['game_id'].append(stream['game_id'])
        stream_raw_data['game_name'].append(stream['game_name'])
        stream_raw_data['broadcast_id'].append(stream['id'])
        stream_raw_data['stream_title'].append(stream['title'])
        stream_raw_data['viewer_count'].append(stream['viewer_count'])
        stream_raw_data['stream_start_time'].append(stream_start_time)
        stream_raw_data['pull_time'].append(CLEAN_TIME)

    return stream_raw_data

def fetch_top_overwatch_streams(credentials, session, access_token, cursor=None):
    # Overwatch: 488552, Apex: 511224
    twitch_stream_response = session.get('https://api.twitch.tv/helix/streams', 
                                        headers = {
                                            'Authorization': 'Bearer ' + access_token, 
                                            'Client-ID': credentials['TWITCH_CLIENT_ID']}, 
                                        params = {
                                            'game_id': 488552,
                                            'first':  100,
                                            'after': cursor
                                        })
    twitch_stream_response = twitch_stream_response.json()
    streams = twitch_stream_response['data']
    if 'pagination' not in twitch_stream_response:
        return streams, None
    cursor = twitch_stream_response['pagination']['cursor']

    return streams, cursor


def clear_data(stream_raw_data):
    stream_raw_data['id'].clear()
    stream_raw_data['user_id'].clear()
    stream_raw_data['user_name'].clear()
    stream_raw_data['game_id'].clear()
    stream_raw_data['game_name'].clear()
    stream_raw_data['broadcast_id'].clear()
    stream_raw_data['stream_title'].clear()
    stream_raw_data['viewer_count'].clear()
    stream_raw_data['stream_start_time'].clear()
    stream_raw_data['pull_time'].clear()

    return stream_raw_data


def write_overwatch_data(credentials, overwatch_stream_data):
    table_name = f'overwatch_data_{YEAR}_{MONTH}'
    dtypes = {
        'id': VARCHAR(255),
        'user_id': BIGINT(),
        'user_name': VARCHAR(255),
        'game_id': BIGINT(),
        'game_name': VARCHAR(255),
        'broadcast_id': BIGINT(),
        'stream_title': VARCHAR(511),
        'viewer_count': BIGINT(),
        'stream_start_time': DateTime(),
        'pull_time': DateTime()
    }
    try:
        engine = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(credentials['DB_USER'], 
                                                                              credentials['DB_PASS'], 
                                                                              credentials['DB_HOST'], 
                                                                              credentials['DB_PORT'], 
                                                                              credentials['DB_NAME']))
        overwatch_stream_data.to_sql(table_name, 
                                     con = engine, 
                                     if_exists = 'append', 
                                     index = False, 
                                     dtype = dtypes)
        add_primary_key(engine, table_name)
    except Exception as e:
        print(e)

def add_primary_key(engine, table_name):
    try:
        with engine.connect() as con:
            con.execute(f'ALTER TABLE `{table_name}` ADD PRIMARY KEY(`id`);')
    except Exception as e:
        print(e)


def main():
    credentials = _envget()
    session = requests.Session()
    stream_raw_data = {'id':[],
                       'user_id': [], 
                       'user_name': [], 
                       'game_id': [], 
                       'game_name': [],
                       'broadcast_id': [], 
                       'stream_title': [], 
                       'viewer_count': [], 
                       'stream_start_time': [], 
                       'pull_time': []}
    try:
        access_token = check_auth(credentials, session)
        cursor = ''
        while cursor is not None:
            streams, cursor = fetch_top_overwatch_streams(credentials, session, access_token, cursor)
            data = populate_dictionary(streams, stream_raw_data)
            stream_data = pd.DataFrame.from_dict(data)
            write_overwatch_data(credentials, stream_data)
            stream_raw_data = clear_data(data)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()