import random
from datetime import datetime, timedelta

from app.ds.in_mem.main import Redis


r = Redis()
LEAGUES = ['NBA', 'NFL', 'NHL', 'MLB', 'WNBA', 'NCAA']


def generate_random_league():
    return random.choice(LEAGUES)


def generate_random_matchups(league: str) -> tuple:
    teams = r.teams.getteams(league)
    away_teams = random.sample(teams, k=int(len(teams) / 2))
    for away_team in away_teams:
        rand_idx = random.randint(0, len(teams) - 1)
        home_team = teams.pop(rand_idx)
        yield away_team, home_team


def generate_random_games():
    r.games.flush()
    league = generate_random_league()
    for away_team, home_team in generate_random_matchups(league):
        game_time = datetime.now() + timedelta(seconds=random.randint(15, 30))
        mapping = {
            "info": f'{league}_{game_time.strftime('%Y%m%d')}_{away_team}@{home_team}',
            "game_time": game_time
        }
        r.games.store(league, mapping)




def test_store_and_get_game():
    """
    Test storing and retrieving a game.
    """
    league = generate_random_league()
    combined_teams = generate_random_teams(league)
    for away_team, home_team in combined_teams:
        game_time = datetime.now() + timedelta(seconds=random.randint(15, 30))
        mapping = {
            "info": f'{league}_{game_time.strftime('%Y%m%d')}_{away_team}@{home_team}',
            "game_time": game_time
        }
        r.games.store(league, mapping)

        # Retrieve the game from the system
        game = r.games.getgame(league, r.teams.getteamid(league, away_team))
        assert game, f"Failed to retrieve game for {away_team} vs {home_team}"

        print(f"Retrieved game: {game['info']} in league {league} at {game_time}")


def test_get_games_for_league():
    """
    Test retrieving all games for a specific league.
    """
    league = generate_random_league()
    games = r.games.getgames(league)

    assert isinstance(games, list), "Expected a list of games"
    assert len(games) > 0, "No games found for the league"
    print(f"Retrieved {len(games)} games for league {league}.")


def test_get_live_games():
    """
    Test retrieving live games for a specific league.
    """
    league = generate_random_league()
    live_games = r.games.getlivegames(league)

    if live_games:
        assert isinstance(live_games, set), "Expected a set of live games"
        print(f"Retrieved {len(live_games)} live games for league {league}.")
    else:
        print(f"No live games found for league {league}.")



