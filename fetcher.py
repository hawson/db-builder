#!/usr/bin/env python3
'''
Description: Fetches a list of all the Steam ID's and checks the 
current pricing of each game
'''
import time
import requests
import json
import datetime
import random
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
    last_price_change = Column(DateTime)
    init_price = Column(Integer, default=0)
    final_price = Column(Integer, default=0)
    lowest_price = Column(Integer, default=0)
    highest_price = Column(Integer, default=0)

    def __repr__(self):
        return "<Game(id='{}', name='{}', last_price_change='{}', initial_price='{}', final_price='{}')>".format(self.id, self.name, self.last_price_change, self.init_price, self.final_price)

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


# Returns list of game ids (ints) stored in the database.
def games_with_data(session):
    return [ g[0] for g in session.query(Game.id,Game.name).all() ]


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
        print("No local results found for ID {}. Updating DB".format(game))
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
# Arguments:
#   a DB session object
#   a list of game IDs to query
#   a FULL LIST OF ALL GAMES EVAR  (yuck)
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
            print("Error requesting data for the following ids: {} \n continuing after splitting them up and retrying".format(", ".join(applist)))
            if len(applist) <= 1:
                print("ID {} : {} has invalid json, updating blacklist".format(game, name_matcher(game,master_list)))
                blacklist_obj = Blacklist(id=game)
                session.add(blacklist_obj)

            else:
                list_split(session, applist, master_list)
            continue

        for game in data:
            if data[game]["success"] is True and data[game]["data"]:
                print("ID {:>6} : Updating prices on {}".format(game, name_matcher(game,master_list)))
                init_price = data[game]["data"]["price_overview"]["initial"]
                final_price = data[game]["data"]["price_overview"]["final"]
                name = name_matcher(game, master_list)
                curtime = datetime.datetime.utcnow()
                price_check = query_db(session, game)

                if price_check:
                    # If found in DB...
                    if price_check.final_price != final_price:
                        update_db(session, game, "final_price", final_price)
                        update_db(session, game, "last_price_change", curtime)
                    if price_check.lowest_price > final_price:
                        update_db(session, game, "lowest_price", final_price)
                    if price_check.highest_price < final_price:
                        update_db(session, game, "highest_price", final_price)

                else:
                    # not found, so add it.
                    game_obj = Game(id=game, name=name, init_price=init_price, final_price=final_price, lowest_price=final_price, highest_price=init_price, last_price_change=curtime)
                    session.add(game_obj)

            elif data[game]["success"] is True and not data[game]["data"]:
                print("ID {:>6} : F2P or demo: {} (updating blacklist)".format(game, name_matcher(game,master_list)))
                blacklist_obj = Blacklist(id=game)
                session.add(blacklist_obj)

            else:
                #No price data yet, check again at later date
                print("ID {:>6} : Lacks price data upstream (skipping): {}".format(game, name_matcher(game,master_list)))
                continue

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

    # Fetch list of dicts objects from Steam (game ID/name pairs)
    master_list = build_list()
    all_game_ids = [ game['appid'] for game in master_list ]

    # Build our current blacklist (list of Blacklist objects)
    blacklist = build_blacklist(session)


    # this is a simple list of game IDs (ints)
    appids = []

    # Build 'appids' list thusly:  master_list - blacklisted  (use sets instead?)
    for game in master_list:
        if game["appid"] not in blacklist:
            appids.append(str(game["appid"]))
        else:
            print("Skipping ID {:>6} : Blacklisted: {}".format(game["appid"], game["name"]))


    # Get list of game IDs for which we already have data
    games_w_data = games_with_data(session)

    # Build list of game IDs that lack data.
    games_wo_data = list(set(all_game_ids) - set(blacklist) - set(games_w_data))


    print("Games total: {}".format(len(master_list)))
    print("Games blacklisted: {}".format(len(blacklist)))
    print("Game appids (total - blacklist): {}".format(len(appids)))
    print("Games with data in DB: {}".format(len(games_w_data)))
    print("Games without data (total-DB): {}".format(len(games_wo_data)))


    # Shuffle, shuffle
    random.shuffle(games_w_data)
    random.shuffle(games_wo_data)

    ids_to_check = games_wo_data
    ids_to_check.extend(games_w_data)
    ids_to_check = list(map(str,ids_to_check))

    # Chunk the main master list
    ids_to_check = list(chunker(ids_to_check, LIMIT))

    #json_game_db = dump_game_db(session)
    fetchdump(session, ids_to_check, master_list)

if __name__ == "__main__":
    main()
