'''
Created on: Nov 24, 2019

Author: Napoleon de Mesa

Version: 1.0
'''
import requests
import sys
import json

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
    credentials = _envget()
    try:
        s = requests.Session()
        s.headers.update({'Client-ID': credentials['TWITCH_CLIENT_ID']})
    except Exception as e:
        print(e)
    try:
        twitch_game_response = s.get('https://api.twitch.tv/helix/streams', params = {
            'game_id': 488552,
            'first':  100
        })
    except Exception as e:
        print(e)

    twitch_game_response = twitch_game_response.json()
    overwatch_streams = list(twitch_game_response.values())[0]
    for stream in overwatch_streams:
        print('---------- New Stream ----------')
        print('Username: ' + str(stream['user_name']))
        print('User ID: ' + str(stream['user_id']))
        print('Broadcast ID: ' + str(stream['id']))
        print('Started streaming at: ' + str(stream['started_at']))
        print('Viewer Count: ' + str(stream['viewer_count']))

if __name__ == '__main__':
    main()