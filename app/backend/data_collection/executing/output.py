import json
import os

from app.backend.data_collection.bookmakers import ODDSSHOPPER_NOVEL_BOOKMAKERS, PROPPROFESSOR_NOVEL_BOOKMAKERS
from app.backend.data_collection import utils as dc_utils


def nest(data: dict):
    nested_dict = dict()
    # Process each tuple key
    for keys, value in data.items():
        current_level = nested_dict
        for key in keys[:-1]:  # Traverse all except the last key
            current_level = current_level.setdefault(key, {})
        current_level[keys[-1]] = value  # Set the final key to the value

    return nested_dict


def get_file_path(entity_type: str, is_pending: bool) -> str:
    # get a customizable file path
    file_path = f'utils/reports/{"problem" if is_pending else "relevant"}/{entity_type}.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # return file path
    return file_path


def save_relevant_leagues_to_file() -> None:
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='leagues', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(dc_utils.RelevantData.get_relevant_leagues(), f, indent=4, default=str)


def save_relevant_markets_to_file() -> None:
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='markets', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(nest(dc_utils.RelevantData.get_relevant_markets()), f, indent=4, default=str)


def save_relevant_subjects_to_file() -> None:
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='subjects', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(nest(dc_utils.RelevantData.get_relevant_subjects()), f, indent=4, default=str)


def save_relevant_teams_to_file() -> None:
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='teams', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(nest(dc_utils.RelevantData.get_relevant_teams()), f, indent=4, default=str)


def save_relevant_games_to_file():
    # create a custom file path to store the betting lines sample
    file_path = 'utils/reports/relevant/games.json'
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(dc_utils.RelevantGames.get_relevant_games(), f, indent=4, default=str)


def save_problem_markets_to_file() -> None:
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='markets', is_pending=True)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(nest(dc_utils.ProblemData.get_problem_markets()), f, indent=4, default=str)


def save_problem_subjects_to_file():
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='subjects', is_pending=True)
    # open the pending subjects file
    with open(file_path, 'w') as f:
        # save the pending subjects to the file, in pretty print mode
        json.dump(nest(dc_utils.ProblemData.get_problem_subjects()), f, indent=4, default=str)


def save_problem_teams_to_file():
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='teams', is_pending=True)
    # open the pending subjects file
    with open(file_path, 'w') as f:
        # save the pending subjects to the file, in pretty print mode
        json.dump(nest(dc_utils.ProblemData.get_problem_teams()), f, indent=4, default=str)


def save_betting_lines_to_file():
    # create a custom file path to store the betting lines sample
    file_path = 'utils/reports/betting_lines.json'
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(nest(dc_utils.BettingLines.get_lines()), f, indent=4, default=str)


def save_games_to_file():
    # create a custom file path to store the betting lines sample
    file_path = 'utils/reports/games.json'
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(nest(dc_utils.Games.get_games()), f, indent=4, default=str)


def save_box_scores_to_file():
    # create a custom file path to store the betting lines sample
    file_path = 'utils/reports/box_scores.json'
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(dc_utils.BoxScores.get_box_scores(), f, indent=4, default=str)


def save_data_to_files() -> None:
    # save the valid markets,teams, leagues and subjects that were found in the database to a file to be evaluated
    save_relevant_leagues_to_file()
    save_relevant_markets_to_file()
    save_relevant_subjects_to_file()
    save_relevant_teams_to_file()
    save_relevant_games_to_file()
    # save the pending markets, teams and subjects that were not found in the database to a file to be evaluated
    save_problem_markets_to_file()
    save_problem_subjects_to_file()
    save_problem_teams_to_file()
    # save the sample data of betting lines to a file for inspection
    save_betting_lines_to_file()
    save_games_to_file()
    save_box_scores_to_file()


def output_source_stats(retriever: dc_utils.Retriever, time_taken: float) -> None:
    # Because OddsShopper isn't actually a bookmaker, but a tool that holds other bookmaker's odds
    if retriever.name in {'OddsShopper', 'PropProfessor'}:
        # for every bookmaker that they offer
        for bookmaker_name in {*ODDSSHOPPER_NOVEL_BOOKMAKERS, *PROPPROFESSOR_NOVEL_BOOKMAKERS}:
            # output the amount of lines collected from each bookmaker they offer and the time taken for the whole job.
            print(f'[{bookmaker_name}]: {dc_utils.BettingLines.size(bookmaker_name=bookmaker_name)}, {round(time_taken, 3)}s')
    else:
        # otherwise just output for the inputted bookmaker plug
        print(f'[{retriever.name}]: {retriever}, {round(time_taken, 3)}s')