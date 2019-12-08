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
            credentials['DB_USER'] = file_contents[1]
            credentials['DB_PASS'] = file_contents[2]
            credentials['DB_HOST'] = file_contents[3]
            credentials['DB_PORT'] = file_contents[4]
            credentials['DB_NAME'] = file_contents[5]
    except Exception as e:
        print(e)

    return credentials
def write_overwatch_data(credentials, overwatch_stream_data):
    table_name = 'overwatch_data_{}_{}'.format(YEAR, MONTH)
    dtypes = {
        'id': VARCHAR(255),
        'user_name': VARCHAR(255),
        'user_id': BIGINT(),
        'broadcast_id': BIGINT(),
        'stream_title': VARCHAR(511),
        'viewer_count': BIGINT(),
        'stream_start_time': DateTime(),
        'pull_time': DateTime()
    }
    try:
        engine = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(credentials['DB_USER'], credentials['DB_PASS'], credentials['DB_HOST'], credentials['DB_PORT'], credentials['DB_NAME']))
        overwatch_stream_data.to_sql(table_name, con = engine, if_exists = 'append', index = False, dtype = dtypes)
        add_primary_key(engine, table_name)
        print('Added primary key to table.')
    except Exception as e:
        print(e)

def add_primary_key(engine, table_name):
    try:
        with engine.connect() as con:
            con.execute('ALTER TABLE `{}` ADD PRIMARY KEY(`id`);'.format(table_name))
    except Exception as e:
        print(e)
    

def main():
    overwatch_raw_data = {'id':[], 'user_name': [], 'user_id': [], 'broadcast_id': [], 'stream_title': [], 'viewer_count': [], 'stream_start_time': [], 'pull_time': []}
    credentials = _envget()
    try:
        session = requests.Session()
        session.headers.update({'Client-ID': credentials['TWITCH_CLIENT_ID']})
    except Exception as e:
        print(e)
    
    try:
        print('Fetching first page of the top Overwatch streams...')
        top_overwatch_streams_first_page = fetch_top_overwatch_streams(session)
        populate_dictionary(top_overwatch_streams_first_page[0], overwatch_raw_data)
        print('Fetching second page of the top Overwatch streams...')
        top_overwatch_streams_second_page = fetch_top_overwatch_streams(session, top_overwatch_streams_first_page[1])
        populate_dictionary(top_overwatch_streams_second_page[0], overwatch_raw_data)
        overwatch_stream_data = pd.DataFrame.from_dict(overwatch_raw_data)
        print('Writting to database...')
        write_overwatch_data(credentials, overwatch_stream_data)
        print('success!')
    except Exception as e:
        print(e)

def populate_dictionary(overwatch_streams, overwatch_raw_data):
    for stream in overwatch_streams:
        stream_start_time = str(stream['started_at'][:10]) + ' ' + str(stream['started_at'][11:19])
        overwatch_raw_data['id'].append(str(stream['id'] + '-' + CLEAN_TIME))
        overwatch_raw_data['user_name'].append(stream['user_name'])
        overwatch_raw_data['user_id'].append(stream['user_id'])
        overwatch_raw_data['broadcast_id'].append(stream['id'])
        overwatch_raw_data['stream_title'].append(stream['title'])
        overwatch_raw_data['viewer_count'].append(stream['viewer_count'])
        overwatch_raw_data['stream_start_time'].append(stream_start_time)
        overwatch_raw_data['pull_time'].append(CLEAN_TIME)

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

    return overwatch_streams, starting_cursor['cursor']

if __name__ == '__main__':
    main()