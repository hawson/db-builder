#!/usr/bin/python3
'''
Description: Fetches a list of all the Steam ID's and checks the 
current pricing of each game
'''
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

#DB Table descriptions
class Blacklist(Base):
    __tablename__ = 'blacklist'
    id = Column(Integer, primary_key=True)

    def __repr__(self):
        return "<Blacklist(id='{}')>".format(self.id)

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

#Dumps the game database
def dump_game_db(session):
    dump = {}
    for g in session.query(Game).all():
        dump[g.id] = g.name
        #print("{} - {}".format(g.name, g.id))
    output = json.dumps(dump)
    return output

#Builds list of all the possible Steam ID's
def build_list():
    URL = "http://api.steampowered.com/ISteamApps/GetAppList/v2"
    response = requests.get(URL)
    game_list = response.json()["applist"]["apps"]
    return game_list

#Maps the Steam ID to an actual name
def name_matcher(appid, master_list):
    for game in master_list:
        if int(appid) == game['appid']:
            return game['name']

#Queries the game DB
def query_db(session, game):
    try:
        result = session.query(Game).filter_by(id=game).one()
        return result
    except MultipleResultsFound as e:
        print("{}".format(e))
        return False
    except NoResultFound as e:
        print("Couldn't find ID {} : {}".format(game, e))
        return False

#Builds a list of all the blacklist ID's (Those that have no price)
def build_blacklist(session):
    blacklist = []
    for black in session.query(Blacklist).all():
        blacklist.append(black.id)
    return blacklist

#Updates the game DB
def update_db(session, game, field, value):
    try:
        session.query(Game).filter_by(id=game).update({field: value})
    except:
        print("Unknown error occured updating the DB!")

#Main routine for fetching the current price per game
def fetchdump(session, appids, master_list):
    blacklist = build_blacklist(session)
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
            if int(game) in blacklist:
                print("Skipping {} due to blacklist".format(game))
                continue
            if data[game]["success"] is True and data[game]["data"]:
                init_price = data[game]["data"]["price_overview"]["initial"]
                final_price = data[game]["data"]["price_overview"]["final"]
                name = name_matcher(game, master_list)
                game_obj = Game(id=game, name=name, init_price=init_price, final_price=final_price)
                price_check = query_db(session, game)
                if price_check:
                    if price_check.final_price != final_price:
                        update_db(session, game, "final_price", final_price)
                else:
                    session.add(game_obj)
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

#Routine for splitting up the queries into chunks of a certain limit
def chunker(l, n):
    """Yield successive n-sized chunks from list l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

#Main method (starting point)
def main():
    master_list = build_list()
    appids = []
    for game in master_list:
        appids.append(str(game["appid"]))
    appids = list(chunker(appids, LIMIT))
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    #json_game_db = dump_game_db(session)
    fetchdump(session, appids, master_list)

if __name__ == "__main__":
    main()
