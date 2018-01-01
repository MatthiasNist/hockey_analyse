# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 15:56:26 2017

@author: Matthias
"""

import sys
sys.path.append('C:\Users\Matthias\Documents\Python Scripts\hockey_analysis')
import pandas as pd
import numpy as np
import requests
import io
from pymongo import MongoClient 
import sys
from bs4 import BeautifulSoup
import datetime as dt

import re

import urllib2


class GetData(object):
    
    def __init__(self, *args, **kwargs):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.coll_players = self.client.hockey.players
        self.coll_games = self.client.hockey.games
        #self.coll_games.delete_many({})
        self.url_raw = 'https://www.hockey-reference.com/play-index/pgl_finder.cgi?request=1&match=game&rookie=N&age_min=0&age_max=99&is_playoffs=N&group_set=single&player_game_min=1&player_game_max=9999'
        
    def main(self, start_time, end_time, position, letter = None):
        
        players = self.players(start_time, end_time, letter)
        players_count = players.count()
        print 'starting to collect data for ' + str(players_count) + ' players'
        
        for pro, player in enumerate(players):
            self.collect_game_data(player_shortcut=player['shortcut'], position=position)
            if pro%10 == 0:
                print '===== PROGRESS IS ' + str(float(pro)/players_count*100) + '% ====='
        
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
        df_to_write = df_players[['shortcut', 'first_name', 'last_name',
                                  'active_flag', 'begin_active', 'end_active']]      
        _src = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d')
        self.coll_players.delete_many({})
        self.write_to_mongo(data = df_to_write, coll = self.coll_players, source = _src)
        
# =============================================================================
#         for element in df_players.to_dict('rec'):
#             self.coll_player.insert_one({
#                     'shortcut': element['shortcut'],
#                     'first_name': element['first_name'],
#                     'last_name': element['last_name'],
#                     'active_flag': element['active_flag'],
#                     'begin_active': element['begin_active'],
#                     'end_active': element['end_active']
#                     }
#                     ) '$regex': '^'+letter
# =============================================================================
            
        print 'inserted'
    
    def players(self, start_time, end_time, letter):
            
        query = {
                'begin_active': {'$gte': start_time},
                'end_active': {'$lte': end_time}
                }
        
        if letter:
            query.update({'shortcut': {'$regex': '^'+letter}})
            
        cur = self.coll_players.find(query)#'khabini01'
                                             
        return cur

    def get_game_data(self, url_raw):
        
        url = url_raw
        soup = BeautifulSoup(urllib2.urlopen(url).read())     
        table = soup.find('tbody')
        if not table:
            return pd.DataFrame({})  
        print url
        res = []
        row = []
        header = self.get_game_header(url)
        pro = 0
        offset = False
             
        while pro == 0 or offset:           
            if offset:
                url = url_raw+'&offset='+str(pro)
                soup = BeautifulSoup(urllib2.urlopen(url).read())     
                table = soup.find('tbody')
                offset = False   
            for tr in table.find_all('tr'):
                for td in tr.find_all(re.compile('^t')):
                    row.append(td.text)
                if row[0] == 'Rk':
                    row = []
                    continue        
                res.append(row)
                pro = int(row[0])
                row = []                
                #print pro
                if pro%300 == 0:
                    offset = True
                    print str(pro) + ' records already found...continuing...'
                    break
        df = pd.DataFrame(res, columns = header)
        df.drop([''], axis = 1, inplace = True)
        return df
    
# offset= 300 
# =============================================================================
#     https://www.hockey-reference.com/play-index/pgl_finder.cgi?request=1&match=game&rookie=N&age_min=0&age_max=99&is_playoffs=N&group_set=single&player_game_min=1&player_game_max=9999&pos=G&player=bironma01
# =============================================================================
    
    def get_game_header(self, url):
        
        soup = BeautifulSoup(urllib2.urlopen(url).read())
        soup.findAll('table')[0].thead.findAll('tr')
 
        for row in soup.findAll('table')[0].thead.findAll('tr'):
            header = []
            for column_name in range(0, 24, 1):
                if  row.find_all('th', {"class": " over_header center"}):
                    break
                try:
                    header_col = row.findAll('th')[column_name].contents[0]
                    header.append(header_col)
                except IndexError:
                    header.append('')
                    continue
        #print header
        return header
        
        

    def write_to_mongo(self, data, coll, source):
        data['_src'] = source  

        if coll.name == 'players':
            coll.delete_many({})
            coll.insert_many(data.to_dict('rec'))
        else:              
            for rec in data.to_dict('rec'):
                query = {'shortcut': rec['shortcut'], 'Date': rec['Date']}
    # =============================================================================
    #             if coll.name == 'games':
    #                 query.update({'Date': rec['Date']})
    # =============================================================================
                coll.update_one(query, {'$set': rec}, upsert = True)
        cur = coll.find({})
        count = cur.count()
        print str(data.shape[0]) + ' records inserted in collection: [' + coll.name +'] - now there are ' + str(count) + ' records'
        
    def collect_game_data(self, player_shortcut, position):
        
# =============================================================================
#         &rookie=N
#         &age_min=0
#         &age_max=99
#         &player=zmoledo01
#         &is_playoffs=N
#         &group_set=single
#         &player_game_min=1
#         &player_game_max=9999
#         &pos=S
# =============================================================================
        
        url = self.url_raw + '&pos=' + position + '&player=' + player_shortcut
        print 'searching for data for player ' + str(player_shortcut)
        df_games = self.get_game_data(url)
        if df_games.empty:
            print 'no data found for player ' + player_shortcut + ' on position ' + position
            return
        df_games['shortcut'] = player_shortcut
        _src = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d')
        self.write_to_mongo(data = df_games, coll = self.coll_games, source = _src)
        
        
        
        

if __name__ == '__main__':
    test = GetData()
    test.get_player_data()
    #url = "https://www.hockey-reference.com/play-index/pgl_finder.cgi?request=1&match=game&rookie=N&age_min=0&age_max=99&player=greisth01&is_playoffs=N&group_set=single&series_game_min=1&series_game_max=7&team_game_min=1&team_game_max=84&player_game_min=1&player_game_max=9999&game_type%5B%5D=R&game_type%5B%5D=OT&game_type%5B%5D=SO&pos=G&game_month=0&order_by=goals_against_avg"
    #test.get_game_data(url=url)
    test.main(start_time = 1995, end_time = 2017, position = 'G')