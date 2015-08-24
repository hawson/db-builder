#!/usr/bin/python3

import requests
import json
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

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
    game_list = dict()
    URL = "http://api.steampowered.com/ISteamApps/GetAppList/v2"
    response = requests.get(URL)
    game_list = json.loads(response.text)
    return game_list

def fetchdump(appids, master_list):
    if len(appids) > LIMIT:
        print("Error: Do not submit more than %d items per query!" % LIMIT)
        return
    #TODO: Make sure we were fed a list of strings, not list of ints
    Session = sessionmaker(bind=engine)
    session = Session()
    params = {
        "appids": "," . join(appids),
        "filters": "price_overview"
    }
    response = requests.get(API_URL, params=params)
    data = json.loads(response.text)
    for game in data:
        if data[game]["success"] is True:
            if data[game]["data"]:
                init_price = data[game]["data"]["price_overview"]["initial"]
                final_price = data[game]["data"]["price_overview"]["final"]
                name = master_list["applist"]["apps"][int(game)]["name"]
                game_obj = Game(id=game, name=name, init_price=init_price, final_price=final_price)
                session.merge(game_obj)
        else:
            print("Non success for %s : %s" % (game, master_list["applist"]["apps"][int(game)]["name"]))
    try:
        session.commit()
    except IntegrityError as err:
        print("Error updating DB! %s" % err)

def main():
    master_list = build_list()
    appids = []
    #TODO: Split into chunks of LIMIT
    for app in master_list["applist"]["apps"][:LIMIT]:
        appids.append(str(app["appid"]))
    Base.metadata.create_all(engine)
    fetchdump(appids, master_list)

if __name__ == "__main__":
    main()
