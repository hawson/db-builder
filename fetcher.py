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
LIMIT = 200
SLEEPER = 10

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
    except MultipleResultsFound:
        print("Error, multiple entries found for ID: {}".format(game))
        return False
    except NoResultFound:
        print("No results found for ID: {}".format(game))
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

# Split a problematic list in half to try and identify the bad ID for blacklisting.
def list_split(session, applist, master_list):
    """Receives a list, splits in half, resends the list of two lists into fetchdump()"""
    newapplist = [applist[::2], applist[1::2]]
    fetchdump(session, newapplist, master_list)
    return

#Main routine for fetching the current price per game
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
            print("Error requesting data for the following ids: {} \n continuing after splitting them up and retrying.".format(", ".join(applist)))
            if len(applist) <= 1:
                print("ID {} is false for game: {}".format(game, name_matcher(game,master_list)))
                blacklist_obj = Blacklist(id=game)
                session.add(blacklist_obj)
            else:
                list_split(session, applist, master_list)
            continue
        for game in data:
            if data[game]["success"] is True and data[game]["data"]:
                init_price = data[game]["data"]["price_overview"]["initial"]
                final_price = data[game]["data"]["price_overview"]["final"]
                name = name_matcher(game, master_list)
                curtime = datetime.datetime.utcnow()
                game_obj = Game(id=game, name=name, init_price=init_price, final_price=final_price, lowest_price=final_price, highest_price=init_price, last_update=curtime)
                price_check = query_db(session, game)
                if price_check:
                    if price_check.final_price != final_price:
                        update_db(session, game, "final_price", final_price)
                    if price_check.lowest_price > final_price:
                        update_db(session, game, "lowest_price", final_price)
                    if price_check.highest_price < final_price:
                        update_db(session, game, "highest_price", final_price)
                else:
                    session.add(game_obj)
            else:
                print("ID {} is false for game: {}".format(game, name_matcher(game,master_list)))
                blacklist_obj = Blacklist(id=game)
                session.add(blacklist_obj)
            try:
                session.commit()
            except IntegrityError as err:
                print("Error updating DB! {}".format(err))
        print("Sleeping {} seconds until the next batch".format(SLEEPER))
        try:
            time.sleep(SLEEPER)
        except KeyboardInterrupt:
            exit(1)

#Routine for splitting up the queries into chunks of a certain limit
def chunker(l, n):
    """Yield successive n-sized chunks from list l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

#Main method (starting point)
def main():
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    blacklist = build_blacklist(session)
    master_list = build_list()
    appids = []
    for game in master_list:
        if game["appid"] not in blacklist:
            appids.append(str(game["appid"]))
        else:
            print("Skipping ID {}:{} because it is blacklisted".format(game["appid"], game["name"]))
    appids = list(chunker(appids, LIMIT))
    #json_game_db = dump_game_db(session)
    fetchdump(session, appids, master_list)

if __name__ == "__main__":
    main()
