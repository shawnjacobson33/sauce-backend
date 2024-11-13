import threading
from typing import Optional

from app.backend.database import db, GAMES_COLLECTION_NAME
from app.backend.data_collection.utils.definitions import IN_SEASON_LEAGUES

# instantiate a cursor that points to the games collection for use of Games class.
games_cursor = db.MongoDB.fetch_collection(GAMES_COLLECTION_NAME)


def update_teams_dictionary(game: dict, data_struct: dict, game_id: str = None):
    # get the home and away team objects
    away_team, home_team = game['away_team'], game['home_team']
    # create a new dictionary that holds useful data for LinesRetrievers
    game_data = {
        'id': str(game['_id']) if not game_id else game_id,
        'info': f"{away_team.get('full_name', away_team.get('abbr_name'))} @ {home_team.get('full_name', home_team.get('abbr_name'))}"
    }
    # give each team name type its id as a pair and update the dictionary
    data_struct[away_team['id']] = data_struct[home_team['id']] = game_data


def get_structured_docs(docs: list[dict]) -> dict:
    # use abbreviated names as keys
    structured_docs = dict()
    # for each team
    for doc in docs:
        # add the data from the document to the data structure
        update_teams_dictionary(doc, structured_docs)

    # return the structured documents
    return structured_docs


def structure_data() -> dict:
    # initialize a dictionary to hold all the data partitioned
    partitioned_data = dict()
    # for each partition in the partitions predicated upon the cursor name
    for partition in IN_SEASON_LEAGUES:
        # filter by league or sport and don't include the batch_id
        filtered_docs = games_cursor.find({'league': partition if 'NCAA' not in partition else 'NCAA'})
        # structure the documents and data based upon whether its markets or subjects data
        partitioned_data[partition] = get_structured_docs(filtered_docs)

    # return the fully structured and partitioned data
    return partitioned_data


class Games:
    _games: dict = structure_data()  # Gets the data stored in the database
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_games(cls, league: Optional[str] = None):
        # gets the data for the inputted partition
        return cls._games.get(league) if league else cls._games

    @classmethod
    def update_games(cls, game: dict):
        with cls._lock1:
            # create a unique identifier (primary key) to filter on
            filter_condition = {
                'away_team.id': game['away_team']['id'],
                'home_team.id': game['home_team']['id'],
                'game_time': game['game_time']
            }
            # don't want to keep inserting duplicates/and game info can change...so perform an upsert using the filter
            result = games_cursor.update_one(filter_condition, {"$set": game}, upsert=True)
            # if there was no match for the game in the database...we have a unique game
            if result.upserted_id:
                # update the games data structure by partition with the inputted game
                update_teams_dictionary(game, cls._games[game['league']], str(result.upserted_id))


    @classmethod
    def size(cls, source_name: str = None) -> int:
        # if bookmaker is inputted
        if source_name:
            # get the lines associated with that bookmaker
            lines = cls._games.get(source_name, "")
            # return the number of lines they have
            return len(lines)

        # gets the total amount of betting lines stored
        return sum(len(value) for value in cls._games.values())

    @classmethod
    def store_games(cls) -> None:
        list_of_games_objs = [game for games in cls._games.values() for game in games]
        db.MongoDB.fetch_collection(GAMES_COLLECTION_NAME).insert_many(list_of_games_objs)
