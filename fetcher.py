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
from sqlalchemy import desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.sql import func


#Globals
engine = create_engine('sqlite:///games.db')
Base = declarative_base()
API_URL = "http://store.steampowered.com/api/appdetails/"
LIMIT = 200
SLEEPER = 5

#DB Table descriptions
class Blacklist(Base):
    __tablename__ = 'blacklist'
    id = Column(Integer, primary_key=True)

    def __repr__(self):
        return "<Blacklist(id='{}')>".format(self.id)

class Game(Base):
    __tablename__ = 'games'
    id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False)
    last_update = Column(DateTime, nullable=False)

    def __repr__(self):
        return "<Game(id='{}', name='{}', last_update='{}')>".format(self.id, self.name, self.last_update)

class Prices(Base):
    __tablename__ = 'prices'
    game_id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, primary_key=True)
    init_price = Column(Integer, nullable=False)
    final_price = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Prices(gameid='{}', timestamp='{}', init_price='{}', final_price='{}'>" . format(self.gameid, self.timestamp, self.init_price, self.final_price)



#Dumps the game database (not including prices)
def dump_game_db(session):
    dump = {}
    for g in session.query(Game).all():
        dump[g.id] = g.name
        #print("{} - {}".format(g.name, g.id))
    output = json.dumps(dump)
    return output

#Builds list of all the possible Steam ID's
# returned as a list of dicts (decoded from JSON)
def build_list():
    URL = "http://api.steampowered.com/ISteamApps/GetAppList/v2"
    try:
        response = requests.get(URL)
    except:
        print("Failed to get a list of games from Steam!")
        return False

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

#Queries the DB "Game" table for a given ID
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


def last_price(session,gid):
    try:
        result = session.query(Prices).filter_by(game_id=gid).order_by(Prices.timestamp.desc()).one()
        return result

    except MultipleResultsFound:
        print("Error, multiple entries found for ID: {}".format(gid))
        return False

    except NoResultFound:
        print("No price history found for ID {}. Updating DB".format(gid))
        return False


#Builds a list of all the blacklist ID's (Those that have no price)
def build_blacklist(session):
    blacklist = []
    for black in session.query(Blacklist).all():
        blacklist.append(black.id)
    return blacklist


#Updates the game DB
def update_db(session, table, game, field, value):
    try:
        session.query(table).filter_by(id=game).update({field: value})
    except:
        print("Unknown error occured updating the DB!")

# Split a problematic list in half to try and identify the bad ID for blacklisting.
def list_split(session, applist, master_list):
    """Receives a list, splits in half, resends the list of two lists into fetchdump()"""
    newapplist = [applist[::2], applist[1::2]]
    fetchdump(session, newapplist, master_list)
    return


def dump_blacklist(session,master_list):
    blacklist=build_blacklist(session)

    for game in master_list:
        if game["appid"] in blacklist:
            print("Skipping ID {:>6} : Blacklisted: {}".format(game["appid"], game["name"]))



#Main routine for fetching the current price per game
# Arguments:
#   a DB session object
#   a list of game IDs to query
#   a FULL LIST OF ALL GAMES EVAR  (yuck)
def fetchdump(session, appids, master_list):

    all_game_ids = [ game['appid'] for game in master_list ]
    for applist in appids:

        params = {
            "appids": ",".join(applist),
            "filters": "price_overview"
        }

        params_str = '&'.join([ '='.join([x,params[x]]) for x in params.keys() ] )

        curtime = datetime.datetime.utcnow()
        print("Fetching URL {}". format(''.join(list([API_URL,params_str]))))

        try:
            response = requests.get(API_URL, params=params)
        except Exception as e:
            print("Exception occured: {}".format(type(e).__name__))
            continue


        try:
            data = response.json()
        except Exception as e:
            print("Error requesting data for the following ids: {} \n continuing after splitting them up and retrying".format(", ".join(applist)))
            print("Error type is [{}]".format(type(e).__name__))

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

                init_price  = data[game]["data"]["price_overview"]["initial"]
                final_price = data[game]["data"]["price_overview"]["final"]
                name = name_matcher(game, master_list)

                game_found = query_db(session, game)

                if not game_found:
                    # not found, so add it.
                    game_obj = Game(id=game, name=name, last_update=curtime)
                    session.add(game_obj)

                # If we have a price, compare it to the last one.
                # If different, add a new record, otherwise just update
                # the TS on the existing one.
                last_price_found = last_price(session, game)

                # if the current prices match the last one, just update the TS
                if last_price_found and (last_price_found.final_price == final_price) and (last_price_found.init_price == init_price):
                    update_db(session, Prices, game, "last_price_change", curtime)
                else:
                    price_obj = Prices(game_id=game, final_price=final_price, init_price=init_price, timestamp=curtime)
                    session.add(price_obj)


            # We can "successfully" get nothing when asking about prices. This covers demos, F2P games, etc
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

        print_stats(session,master_list)
        print("Sleeping {} seconds until the next batch".format(SLEEPER))

        try:
            time.sleep(SLEEPER)
        except KeyboardInterrupt:
            exit(1)


def print_stats(session,master_list):

    all_game_ids = [ game['appid'] for game in master_list ]
    blacklist = build_blacklist(session)
    games_w_data = games_with_data(session)
    games_wo_data = list(set(all_game_ids) - set(blacklist) - set(games_w_data))

    print("Games total: {}".format(len(master_list)))
    print("Games blacklisted: {}".format(len(blacklist)))
    print("Games with data in DB: {}".format(len(games_w_data)))
    print("Games without data (total-DB): {}".format(len(games_wo_data)))
    print("Total games to check: {}".format(len(games_wo_data)+len(games_w_data)))



#Routine for splitting up the queries into chunks of a certain limit
def chunker(l, n):
    """Yield successive n-sized chunks from list l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]


def get_ids_to_check(session, master_list):
    all_game_ids = [ game['appid'] for game in master_list ]

    # Build our current blacklist (list of Blacklist objects)
    blacklist = build_blacklist(session)

    # Get list of game IDs for which we already have data
    games_w_data = games_with_data(session)

    # Build list of game IDs that lack data.
    games_wo_data = list(set(all_game_ids) - set(blacklist) - set(games_w_data))

    print_stats(session,master_list)

    # Shuffle, shuffle
    random.shuffle(games_w_data)
    random.shuffle(games_wo_data)

    ids_to_check = games_wo_data
    ids_to_check.extend(games_w_data)
    ids_to_check = list(map(str,ids_to_check))

    # Chunk the main master list
    return list(chunker(ids_to_check, LIMIT))




#Main method (starting point)
def main():

    # Setup DB connection
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Fetch list of dicts objects from Steam (game ID/name pairs)
    master_list = build_list()

    if not master_list:
        print("exiting!")
        exit(1)


    #json_game_db = dump_game_db(session)

    ids_to_check = get_ids_to_check(session, master_list)
    fetchdump(session, ids_to_check, master_list)

if __name__ == "__main__":
    main()
