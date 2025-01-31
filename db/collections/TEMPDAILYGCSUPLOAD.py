import json
from datetime import datetime


from db import dev_db
from db.collections.utils import GCSUploader


def restructure_betting_lines_data(betting_lines: list[dict]):
    """
    Restructures betting lines data for storage.

    Args:
        betting_lines (list[dict]): The list of betting lines.
    """
    try:
        for betting_line in betting_lines:
            final_stat = betting_line.pop('live_stat', None)
            betting_line['final_stat'] = final_stat

            game = betting_line.pop('game')
            betting_line['game_id'] = game['_id']

    except Exception as e:
        raise Exception(f'Failed to restructure betting lines data: {e}')


def prepare_betting_lines_for_upload(betting_lines: list[dict]) -> str:
    """
    Prepares betting lines data for upload to Google Cloud Storage.

    Args:
        betting_lines (list[dict]): The list of betting lines.

    Returns:
        str: The betting lines data as a JSON string.
    """
    try:
        restructure_betting_lines_data(betting_lines)
        betting_lines_json = json.dumps(betting_lines)
        return betting_lines_json

    except Exception as e:
        raise Exception(f'Failed to prepare betting lines for upload: {e}')


def get_projection_for_gcs() -> dict:
    """
    Retrieves the projection for Google Cloud Storage upload.

    Returns:
        dict: The projection dictionary.
    """
    return {
        'url': 0, 'metrics': 0, 'subject': 0, 'bookmaker': 0, 'date': 0, 'label': 0,
        'league': 0, 'market': 0, 'market_domain': 0
    }


async def store_in_gcs():
    """
    Stores completed betting lines in Google Cloud Storage.
    """
    projection = get_projection_for_gcs()  # Todo: Temporary Fix
    if betting_lines := await dev_db.betting_lines.get_betting_lines({}, projection=projection):  # Todo: Temporary Fix
        betting_lines_json = prepare_betting_lines_for_upload(betting_lines)  # Todo: Temporary Fix
        blob_name = f"{datetime.now().strftime('%Y-%m-%d')}.json"
        uploader = GCSUploader(bucket_name='betting-lines')
        uploader.upload(blob_name, betting_lines_json)


if __name__ == '__main__':
    import asyncio

    asyncio.run(store_in_gcs())