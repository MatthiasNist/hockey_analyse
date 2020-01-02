# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 15:56:26 2017

@author: Matthias
"""

# test#####

import sys

sys.path.append('/home/matthias/Documents/Python Scripts/hockey_analysis')
import pandas as pd
import requests
import io
from pymongo import MongoClient
from bs4 import BeautifulSoup
import datetime as dt

import re
from urllib import urlopen
import psycopg2


# import urllib2


class GetData(object):

    def __init__(self, *args, **kwargs):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.coll_players = self.client.hockey.players
        self.coll_games = self.client.hockey.games
        # self.coll_games.delete_many({})
        self.url_raw = 'https://www.hockey-reference.com/play-index/pgl_finder.cgi?request=1&match=game&rookie=N' \
                       '&age_min=0&age_max=99&is_playoffs=N&group_set=single&player_game_min=1&player_game_max=9999'

    def main(self, start_time, end_time, position, letter=None):

        players = self.players(start_time, end_time, letter)
        players_count = players.count()
        print('starting to collect data for ' + str(players_count) + ' players')

        for pro, player in enumerate(players):
            if player['shortcut'] == 'aebisda01':
                print("stop")
            exists = self.coll_games.find_one({'shortcut': player['shortcut']})
            if exists:
                print('Player ' + player['shortcut'] + ' already exists')
                continue
            self.collect_game_data(player_shortcut=player['shortcut'], position=position)
            if pro % 10 == 0:
                print('===== PROGRESS IS ' + str(float(pro) / players_count * 100) + '% =====')

    def get_player_data(self):

        url = "https://d9kjk42l7bfqz.cloudfront.net/short/inc/players_search_list.csv"
        response = requests.request("GET", url)
        df_players = pd.read_csv(io.StringIO(response.text), sep=',',
                                 names=['shortcut', 'name', 'active_period', 'active_flag', "Unnamed: 1", "Unnamed: 2",
                                        "Unnamed: 3", "Unnamed: 4", "games_played", "Unnamed: 6"],
                                 dtype={"shortcut": str, "name": str, "active_period": str, "active_flag": str,
                                        "games_played": int})
        print(df_players.head(50).to_string())
        print(df_players.columns)
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
        self.write_to_mongo(data=df_to_write, coll=self.coll_players, source=_src)

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

        print('inserted')

    def players(self, start_time, end_time, letter):

        query = {
            'begin_active': {'$gte': start_time},
            'end_active': {'$lte': end_time}
        }

        if letter:
            query.update({'shortcut': {'$regex': '^' + letter}})

        cur = self.coll_players.find(query)  # 'khabini01'

        return cur

    def get_game_data(self, url_raw):

        url = url_raw
        soup = BeautifulSoup(urlopen(url).read())
        table = soup.find('tbody')
        if not table:
            return pd.DataFrame({})
        print(url)
        res = []
        row = []
        header = self.get_game_header(url)
        pro = 0
        offset = False

        while pro == 0 or offset:
            if offset:
                url = url_raw + '&offset=' + str(pro)
                soup = BeautifulSoup(urlopen(url).read())
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
                # print pro
                if pro % 300 == 0:
                    offset = True
                    print(str(pro) + ' records already found...continuing...')
                    break
        df = pd.DataFrame(res, columns=header)  # hier ist der Fehler, len(header) = 24, len(row) = 23
        df.drop([''], axis=1, inplace=True)
        return df

    # offset= 300
    # =============================================================================
    #     https://www.hockey-reference.com/play-index/pgl_finder.cgi?request=1&match=game&rookie=N&age_min=0&age_max
    #     =99&is_playoffs=N&group_set=single&player_game_min=1&player_game_max=9999&pos=G&player=bironma01
    # =============================================================================

    def get_game_header(self, url):

        soup = BeautifulSoup(urlopen(url).read(), features="html.parser")
        soup.findAll('table')[0].thead.findAll('tr')

        for row in soup.findAll('table')[0].thead.findAll('tr'):
            header = []
            for column_name in range(0, 23, 1):
                if row.find_all('th', {"class": " over_header center"}):
                    break
                try:
                    header_col = row.findAll('th')[column_name].contents[0]
                    header.append(header_col)
                except IndexError:
                    header.append('')
                    continue
        # print header
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
                coll.update_one(query, {'$set': rec}, upsert=True)
        cur = coll.find({})
        count = cur.count()
        print(str(data.shape[0]) + ' records inserted in collection: [' + coll.name + '] - now there are ' + str(
            count) + ' records')

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
        print('searching for data for player ' + str(player_shortcut))
        df_games = self.get_game_data(url)
        if df_games.empty:
            print('no data found for player ' + player_shortcut + ' on position ' + position)
            return
        df_games['shortcut'] = player_shortcut
        _src = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d')
        self.write_to_mongo(data=df_games, coll=self.coll_games, source=_src)

    def write_to_csv(self):

        cur = self.coll_games.find({})
        df = pd.DataFrame(list(cur))
        df.to_csv('/home/matthias/PyCharmProjects/hockey_analyse/data_games.csv')

    def transfer_mdb_psql(self, tables, password):
        """
        "transfers data from Mongo-DB to postgreSQL
        :param tables: table which to be to transfered
        :param password: password to postgreSQL
        :return:
        """

        for table in tables:
            try:
                connector = psycopg2.connect(
                    "dbname='hockey' user='postgres' host='127.0.0.1' password = %(password)s" % {
                        "password": password})
                if table == "players":
                    cur = self.coll_players.find()
                    df = pd.DataFrame(cur)
                    data_seq = [l for l in df.to_dict('rec')]
                    with connector.cursor() as cur:  # Context-Manager commited bzw. macht rollback im Falle eines
                        # Errrors
                        cur.execute('truncate table %(table)s' % {"table": table})
                        cur.executemany(
                            """INSERT INTO players(active_flag,first_name, last_name, shortcut, _src, end_active, 
                            begin_active) VALUES (%(active_flag)s, %(first_name)s, %(last_name)s, %(shortcut)s, 
                            %(_src)s, %(end_active)s, %(begin_active)s)""",
                            data_seq)
                elif table == "games":
                    cur = self.coll_games.find()
                    df = pd.DataFrame(cur)
                    data_seq = [l for l in df.to_dict('rec')]
                    with connector.cursor() as cur:  # Context-Manager commited bzw. macht rollback im Falle eines
                        # Errrors
                        cur.execute('truncate table %(table)s' % {"table": table})
                        cur.executemany(
                            """INSERT INTO players(Date,shortcut, A, Age, EV GA, G, GA, GAA, Opp, PIM, 
                            PP GA, PTS, Pos, Rk, SA, SH GA, SO, SV, SV%, TOI, Tm, _src) VALUES (%(Date)s, 
                            %(shortcut)s, %(A)s, %(Age)s, %(EV GA)s, %(G)s, %(GA)s, %(GAA)s, 
                            %(Opp)s, %(PIM)s, %(PP GA)s, %(Pos)s, %(Rk)s, %(SA)s, %(SH GA)s, %(SO)s, %(SV)s, %(SV%)s, 
                            %(TOI)s, %(Tm)s, %(_src)s)""",
                            data_seq)
            except Exception as e:
                print(e)
                del df
            finally:
                if connector is not None:
                    connector.close()

                # self.coll_players = self.client.hockey.players
                # self.coll_games = self.client.hockey.games


if __name__ == '__main__':
    # get_data = GetData()
    # get_data.transfer_mdb_psql(["players"], password='')

    import string

    letters = list(string.ascii_lowercase)
    data_instance = GetData()
    data_instance.get_player_data()
    url = "https://www.hockey-reference.com/play-index/pgl_finder.cgi?request=1&match=game&rookie=N&age_min=0&age_max" \
          "=99&player=greisth01&is_playoffs=N&group_set=single&series_game_min=1&series_game_max=7&team_game_min=1" \
          "&team_game_max=84&player_game_min=1&player_game_max=9999&game_type%5B%5D=R&game_type%5B%5D=OT&game_type%5B" \
          "%5D=SO&pos=G&game_month=0&order_by=goals_against_avg"
    data_instance.get_game_data(url_raw=url)
    for letter in letters:
        print("=================================" + letter)
        while True:
            try:
                data_instance.main(start_time=1980, end_time=2018, position="G", letter=letter)
            except:
                continue
            break
    data_instance.write_to_csv()
