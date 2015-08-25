#!/usr/bin/python3

import time
import requests
import json
import datetime
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

#Globals
engine = create_engine('sqlite:///games.db')
Base = declarative_base()
API_URL = "http://store.steampowered.com/api/appdetails/"
LIMIT = 250

class Blacklist(Base):
    __tablename__ = 'blacklist'
    id = Column(Integer, primary_key=True)

class Game(Base):
    __tablename__ = 'games'
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    last_update = Column(DateTime, onupdate=datetime.datetime.utcnow)
    init_price = Column(Integer, default=0)
    final_price = Column(Integer, default=0)
    lowest_price = Column(Integer, default=0)
    highest_price = Column(Integer, default=0)

    def __repr__(self):
        return "<Game(id='{}', name='{}', last_update='{}', initial_price='{}', final_price='{}')>".format(self.id, self.name, self.last_update, self.init_price, self.final_price)

def dump_db(session):
    dump = {}
    for g in session.query(Game).all():
        dump[g.id] = g.name
        #print("{} - {}".format(g.name, g.id))
    output = json.dumps(dump)
    return output

def build_list():
    URL = "http://api.steampowered.com/ISteamApps/GetAppList/v2"
    response = requests.get(URL)
    game_list = response.json()["applist"]["apps"]
    return game_list

def name_matcher(appid, master_list):
    for game in master_list:
        if int(appid) == game['appid']:
            return game['name']

def query_db(session, game):
    try:
        result = session.query(Game).filter_by(id=game).one()
        print("{}".format(result))
        return True
    except MultipleResultsFound as e:
        print("{}".format(e))
        return False
    except NoResultFound as e:
        print("Couldn't find ID {} : {}".format(game, e))
        return False

def insert_db():
    return False

def update_db():
    return False

def fetchdump(session, appids, master_list):
    for applist in appids:
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
                blacklist_obj = Blacklist(id=game)
                session.merge(blacklist_obj)
            try:
                session.commit()
            except IntegrityError as err:
                print("Error updating DB! {}".format(err))
        print("Sleeping 30 seconds until the next batch")
        try:
            time.sleep(30)
        except KeyboardInterrupt:
            exit(1)

def chunker(l, n):
    """Yield successive n-sized chunks from list l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

def main():
    #generate appid list but keep the master_list intact to know the mapping of appid to game name.
    master_list = build_list()
    appids = []
    for game in master_list:
        appids.append(str(game["appid"]))
    appids = list(chunker(appids, LIMIT))
    #Make sure db tables exist
    Base.metadata.create_all(engine)
    #Instantiate db handles
    Session = sessionmaker(bind=engine)
    session = Session()
    json_db = dump_db(session)
    fetchdump(session, appids, master_list)

if __name__ == "__main__":
    main()
