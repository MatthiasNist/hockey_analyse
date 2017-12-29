# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 15:56:26 2017

@author: Matthias
"""

import pandas as pd
import numpy as np
import requests
import io
from pymongo import MongoClient 

class GetData(object):
    
    def __init__(self, *args, **kwargs):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.coll_player = self.client.hockey.players
        
    def get_player_data(self):

        url = "https://d9kjk42l7bfqz.cloudfront.net/short/inc/players_search_list.csv"
        
        headers = {
            'Cache-Control': "no-cache",
            'Postman-Token': "b1c405a9-acbd-48e0-9817-0b866f4cf77d"
            }
        
        response = requests.request("GET", url, headers=headers)
        
        df_players = pd.read_csv(io.StringIO(response.text), names = ['shortcut', 'name', 'active_period', 'active_flag'])
        
        df_players['active_period'] = df_players['active_period'].apply(
                lambda x: str(x) + '-' + str(x) if len(x.split('-')) == 1 
                else str(x))
        df_players['first_name'] = df_players['name'].apply(lambda x: 
            x.split(' ')[0])
        df_players['last_name'] = df_players['name'].apply(lambda x: 
            x.split(' ')[1])
        df_players['begin_active'] = df_players['active_period'].apply(
                lambda x: int(x.split('-')[0]))    
        df_players['end_active'] = df_players['active_period'].apply(
                lambda x: int(x.split('-')[1]))
        
        for element in df_players.to_dict('rec'):
            self.coll_player.insert_one({
                    'shortcut': element['shortcut'],
                    'first_name': element['first_name'],
                    'last_name': element['last_name'],
                    'active_flag': element['active_flag'],
                    'begin_active': element['begin_active'],
                    'end_active': element['end_active']
                    }
                    )
            
        print 'inserted'
            
        cur = self.coll_player.find({'end_active': {'$lte': 2000}
                                     })
        
        print 'found'
            
        for i in cur:
            print i
            
                
        pass
        #print(response.text)
        

if __name__ == '__main__':
    test = GetData()
    test.get_player_data()