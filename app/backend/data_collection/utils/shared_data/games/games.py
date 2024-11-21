import threading
from typing import Optional
from collections import defaultdict

from app.backend.database import db, GAMES_COLLECTION_NAME
from app.backend.data_collection.utils.definitions import IN_SEASON_LEAGUES

# instantiate a cursor that points to the games collection for use of Games class.
games_cursor = db.MongoDB.fetch_collection(GAMES_COLLECTION_NAME)


def get_game_info(away_team: dict, home_team: dict) -> str:
    return f"{away_team.get('full_name', away_team.get('abbr_name'))} @ {home_team.get('full_name', home_team.get('abbr_name'))}"


def update_games_dictionary(games: dict, game_data: dict) -> None:
    # get the ids
    ids = game_data['ids']
    # create a new dictionary that holds useful data for LinesRetrievers
    game_data = {
        'id': ids['game_id'],
        'info': game_data['info'],
        'box_score_url': game_data['box_score_url']
    }
    # give each team name type its id as a pair and update the dictionary
    games[ids['away_team_id']] = games[ids['home_team_id']] = game_data


def get_structured_docs(docs: list[dict]) -> dict:
    # use abbreviated names as keys
    games = dict()
    # unique games -- for replicated data
    game_counter = defaultdict(int)
    # for each team
    for doc in docs:
        # get the home and away team objects
        away_team, home_team = doc['away_team'], doc['home_team']
        # get game info
        game_info = get_game_info(away_team, home_team)
        # check if the game isn't already stored
        if not game_counter[game_info]:
            # store ids for game, away and home team
            game_data = {
                'ids': {
                'game_id': str(doc['_id']),
                'away_team_id': away_team['id'],
                'home_team_id': home_team['id']
            },
                'info': game_info,
                'box_score_url': doc['box_score_url']
            }
            # add the data from the document to the data structure
            update_games_dictionary(games, game_data)

        # track this game as visited
        game_counter[game_info] += 1

    # return the structured documents
    return games


def get_games() -> defaultdict:
    # initialize a dictionary to hold all the data partitioned
    partitioned_data = defaultdict(dict)
    # for each partition in the partitions predicated upon the cursor name
    for partition in IN_SEASON_LEAGUES:
        # filter by league or sport and don't include the batch_id
        filtered_docs = games_cursor.find({'league': partition})
        # structure the documents and data based upon whether its markets or subjects data
        partitioned_data[partition] = get_structured_docs(filtered_docs)

    # return the fully structured and partitioned data
    return partitioned_data


class Games:
    """
    _games: {
        'NBA': {
            '1239asd09' ( team id ): {
                'id': 123123,
                'info': 'BOS @ BKN',
                'box_score_url': 'NBA_20241113_BOS@BKN'
            }
            ...
        }
        ...
    }

    """
    _games: defaultdict[str, dict] = get_games()  # Gets the data stored in the database
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_games(cls, league: Optional[str] = None) -> dict:
        # gets the data for the inputted partition
        return cls._games.get(league) if league else cls._games

    @classmethod
    def get_game(cls, league: str, team_id: str) -> Optional[dict]:
        # returns a game associated with the team id passed
        if games := Games.get_games(league):
            return games.get(team_id)

    @classmethod
    def update_games(cls, game: dict) -> int:
        with cls._lock1:
            # get the home and away team objects
            away_team, home_team = game['away_team'], game['home_team']
            # create a unique identifier (primary key) to filter on
            filter_condition = {
                'away_team.id': away_team['id'],
                'home_team.id': home_team['id'],
                'game_time': game['game_time']
            }
            # don't want to keep inserting duplicates/and game info can change...so perform an upsert using the filter
            result = games_cursor.update_one(filter_condition, {"$set": game}, upsert=True)
            # if there was no match for the game in the database...we have a unique game
            if result.upserted_id:
                # store ids for game, away and home team
                game_data = {
                    'ids': {
                        'game_id': str(result.upserted_id),
                        'away_team_id': away_team['id'],
                        'home_team_id': home_team['id']
                    },
                    'info': get_game_info(away_team, home_team),
                    'box_score_url': game.get('box_score_url')
                    # TODO: Store game time and move the storage of any active games into this class again?
                }
                # filter the games data structure
                league_filtered_games = cls._games[game['league']]
                # update the games data structure by partition with the inputted game
                update_games_dictionary(league_filtered_games, game_data)
                # this represents a count for new games
                return 1

            # no new games
            return 0

    @classmethod
    def size(cls, source_name: str = None) -> float:
        # if bookmaker is inputted
        if source_name:
            # get the lines associated with that bookmaker
            lines = cls._games.get(source_name, "")
            # return the number of lines they have
            return len(lines)

        # gets the total amount of betting lines stored
        return games_cursor.count_documents({})

    @classmethod
    def store_games(cls) -> None:
        list_of_games_objs = [game for games in cls._games.values() for game in games]
        games_cursor.insert_many(list_of_games_objs)
