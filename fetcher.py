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

class Game(Base):
    __tablename__ = 'games'
    id = Column(Integer, primary_key=True)
    init_price = Column(Integer)
    final_price = Column(Integer)

    def __repr__(self):
        return "<Game(id='%s', initial_price='%s', final_price='%s')>" % (self.id, self.init_price, self.final_price)

def build_list():
    game_list = dict()
    URL = "http://api.steampowered.com/ISteamApps/GetAppList/v2"
    response = requests.get(URL)
    game_list = json.loads(response.text)
    return game_list

def fetchdump(appids):
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
                game_obj = Game(id=game, init_price=init_price, final_price=final_price)
                session.add(game_obj)
        else:
            print("ID %s is false" % game)
    try:
        session.commit()
    except IntegrityError as err:
        print("Error updating DB! %s" % err)

def main():
    master_list = build_list()
    #TEST: first 20 in list, earliest ID's. There has got to be a better way to do this!
    apps = ""
    for app in master_list["applist"]["apps"][:20]:
        junk = " " + str(app["appid"])
        apps += junk
    appids = apps.split(' ')
    Base.metadata.create_all(engine)
    fetchdump(appids)

if __name__ == "__main__":
    main()
