import pytest
import random
from datetime import datetime, timedelta

from app.cache.in_mem import Redis


r = Redis()

LEAGUES = ['NBA', 'NFL', 'NHL', 'MLB', 'WNBA', 'NCAA']


def generate_random_matchups(league: str) -> tuple:
    teams = r.teams.getteams(league)
    away_teams = random.sample(teams, k=int(len(teams) / 2))
    for away_team in away_teams:
        rand_idx = random.randint(0, len(teams) - 1)
        home_team = teams.pop(rand_idx)
        yield away_team, home_team


def generate_random_games():
    for league in LEAGUES:
        for away_team, home_team in generate_random_matchups(league):
            game_time = datetime.now() + timedelta(seconds=random.randint(15, 30))
            mapping = {
                "info": f'{league}_{game_time.strftime('%Y%m%d')}_{away_team}@{home_team}',
                "game_time": game_time
            }
            r.games.store(league, mapping)


@pytest.fixture
def setup_environment():
    r.games.flushall()
    generate_random_games()


def test_store(setup_environment):
    """
    Test storing and retrieving a game.
    """
    for league in LEAGUES:
        for t_id in r.teams.getteamids(league):
            game = r.games.getgame(league, t_id)
            assert game, f"Failed to retrieve game for {game['info']}"


def test_store_unique_games(setup_environment):
    """
    Ensure no duplicate games are stored for a league.
    """
    for league in LEAGUES:
        games = r.games.getgames(league)
        unique_game_ids = {game['info'] for game in games}
        assert len(unique_game_ids) == len(games), f"Duplicate games found in league {league}"


def test_flushall(setup_environment):
    """
    Test that all games are deleted after flushall.
    """
    r.games.flushall()
    for league in LEAGUES:
        games = r.games.getgames(league)
        assert not games, f"Games still exist in league {league} after flushall"

