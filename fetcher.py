#!/usr/bin/python3

import time
import requests
import json
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

#TODO: Write functions to compare what is in the DB to what the API returns.
#TODO: Write functions to update or insert based on presence in the DB
#TODO: Define column lengths to ensure compatability with mysql.
     

#Globals
engine = create_engine('sqlite:///games.db')
Base = declarative_base()
API_URL = "http://store.steampowered.com/api/appdetails/"
LIMIT = 250

class Game(Base):
    __tablename__ = 'games'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    init_price = Column(Integer)
    final_price = Column(Integer)

    def __repr__(self):
        return "<Game(id='%s', name='%s', initial_price='%s', final_price='%s')>" % (self.id, self.name, self.init_price, self.final_price)

def build_list():
    URL = "http://api.steampowered.com/ISteamApps/GetAppList/v2"
    response = requests.get(URL)
    game_list = response.json()["applist"]["apps"]
    return game_list

def name_matcher(appid, master_list):
    for game in master_list:
        if int(appid) == game['appid']:
            return game['name']

def fetchdump(appids, master_list):
    for applist in appids:
        Session = sessionmaker(bind=engine)
        session = Session()
        params = {
            "appids": ",".join(applist),
            "filters": "price_overview"
        }
        response = requests.get(API_URL, params=params)
        try:
            data = response.json()
        except:
            print("Error requesting data for the following ids: {} \n continuing.".format(", ".join(applist)))
            continue
        for game in data:
            if data[game]["success"] is True and data[game]["data"]:
                init_price = data[game]["data"]["price_overview"]["initial"]
                final_price = data[game]["data"]["price_overview"]["final"]
                name = name_matcher(game, master_list)
                game_obj = Game(id=game, name=name, init_price=init_price, final_price=final_price)
                session.merge(game_obj)
            else:
                print("ID {} is false for game: {}".format(game, name_matcher(game,master_list)))
            try:
                session.commit()
            except IntegrityError as err:
                print("Error updating DB! {}".format(err))
        print("Sleeping 30 seconds until the next batch")
        time.sleep(30)

def chunker(l, n):
    """Yield successive n-sized chunks from list l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

def main():
    master_list = build_list()
    #generate appid list but keep the master_list in tact to know the mapping of appid to game name.
    appids = []
    for game in master_list:
        appids.append(str(game["appid"]))
    appids = list(chunker(appids, LIMIT))
    Base.metadata.create_all(engine)
    fetchdump(appids, master_list)

if __name__ == "__main__":
    main()
