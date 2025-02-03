from collections import defaultdict

import pymongo
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import InvalidOperation

from db.base_collection import BaseCollection
from db.collections.utils.stat_dicts import GameStatsDict, PlayerStatsDict


class BettingLines(BaseCollection):
    """
    A class to manage betting lines in the database.

    Attributes:
        db (AsyncIOMotorDatabase): The database connection.
        collection (AsyncIOMotorDatabase.collection): The betting lines collection.
        completed_betting_lines_collection (AsyncIOMotorDatabase.collection): The completed betting lines collection.
        gcs_uploader (GCSUploader): The Google Cloud Storage uploader.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initializes the BettingLines class with the given database connection.

        Args:
            db (AsyncIOMotorDatabase): The database connection.
        """
        super().__init__('betting_lines', db)

        self.completed_betting_lines_collection = self.db['completed_betting_lines']


    @staticmethod
    def _get_most_recent_stream_record(stream: list[dict]):
        """
        Retrieves the most recent record from the stream.

        Args:
            stream (list[dict]): The stream of records.

        Returns:
            dict: The most recent record.
        """
        most_recent_stream_record = stream[-1]
        for k, v in most_recent_stream_record.items():
            if isinstance(v, list):
                most_recent_stream_record[k] = v[-1]

        return most_recent_stream_record


    async def get_betting_lines(self, *args, **kwargs) -> list[dict]:
        """
        Retrieves betting lines based on the given arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            list[dict]: The betting lines.
        """
        try:
            if kwargs.pop('prev_batch', None):

                most_recent_betting_lines = []
                async for betting_line in self.collection.find(*args, **kwargs):
                    most_recent_betting_lines.append(
                        {**{k: v for k, v in betting_line.items() if k != 'stream'},
                         **self._get_most_recent_stream_record(betting_line['stream'])}
                    )

                return most_recent_betting_lines

            if kwargs.pop('completed', None):

                return await self.completed_betting_lines_collection.find(*args, **kwargs).to_list()

            return await self.collection.find({}).to_list()


        except Exception as e:
            raise Exception(f'Failed to get betting lines (args: {*args, kwargs}) : {e}')

    @staticmethod
    def _create_doc(line: dict) -> dict:
        """
        Creates a new betting line document.

        Args:
            line (dict): The betting line data.

        Returns:
            dict: The new betting line document.
        """

        custom_structured_fields = {'metrics', 'batch_timestamp', 'collection_timestamp', 'line', 'odds', 'ev_formula'}

        return {
            **{k: v for k, v in line.items() if k not in custom_structured_fields},
            'metrics': BettingLines._create_new_metrics_tracker(line['metrics']),
            'stream': [ BettingLines._create_new_record(line) ],
        }

    @staticmethod
    def _create_new_record(collected_betting_line: dict) -> dict:
        """
        Creates a new record for the betting line stream.

        Args:
            collected_betting_line (dict): The betting line data.

        Returns:
            dict: The new record.
        """
        record = {
            'batch_timestamp': collected_betting_line['batch_timestamp'],
            'collection_timestamp': collected_betting_line['collection_timestamp'],
            'lines': [ collected_betting_line['line'] ],  # one betting line could have multiple options for "numeric lines" to bet on
            'odds': [ collected_betting_line['odds'] ],  # same reason as above
        }

        return record

    @staticmethod
    def _create_new_metrics_tracker(collected_betting_lines_metrics: dict) -> dict:
        """
        Creates a new metrics tracker for the betting line.

        Args:
            collected_betting_lines_metrics (dict): The betting line metrics.

        Returns:
            dict: The new metrics tracker.
        """
        return {
            'ev': [ collected_betting_lines_metrics['ev'] ],
            'tw_prb': [ collected_betting_lines_metrics['tw_prb'] ]
        }

    async def store_betting_lines(self, collected_betting_lines: list[dict]) -> int:
        """
        Stores betting lines in the database.

        Args:
            collected_betting_lines (list[dict]): The list of betting lines to store.
        """
        # try:
        if len(collected_betting_lines) == 0:
            raise ValueError("No betting lines to store!")

        collected_betting_line_update_queries = defaultdict(lambda: defaultdict(dict))
        # collected_betting_line_insert_docs = defaultdict(dict)

        # gets all stored betting lines to limit network requests
        # stored_betting_lines = {
        #     betting_line['_id']: {k: v for k, v in betting_line.items() if k != '_id'}
        #     for betting_line in await self.collection.find({}).to_list()
        # }

        # first pass to get all updates and inserts
        for collected_betting_line in collected_betting_lines:

            # frequently accessed data
            collected_betting_line_id = collected_betting_line['_id']
            collected_betting_line_num_line = collected_betting_line['line']
            collected_betting_line_odds = collected_betting_line['odds']

            collected_betting_line_metrics = collected_betting_line['metrics']
            collected_betting_line_ev = collected_betting_line_metrics['ev']
            collected_betting_line_tw = collected_betting_line_metrics['tw_prb']

            # # does the betting line already exist in the db?
            # if stored_betting_line := stored_betting_lines.get(collected_betting_line_id):

                # # frequently accessed data
                # stream = stored_betting_line['stream']
                # most_recent_record = stream[stream[-1]['last_update_idx']]
                #
                # # check if data has updated or not
                # line_has_resurrected = (most_recent_record.get('status') == 'nf')
                # line_or_odds_moved = (
                #         line_has_resurrected or (
                #         (collected_betting_line_num_line not in most_recent_record['lines']) or
                #         (collected_betting_line_odds not in most_recent_record['odds']))
                # )
                # metrics_changed = collected_betting_line_metrics != stored_betting_line['metrics']

            # set the update query based off of conditions above
            update_query = collected_betting_line_update_queries[collected_betting_line_id]

            # if line_or_odds_moved:

            # if this betting line has multiple numeric lines to bet on
            if update_push_query := update_query['$push']:

                # readability
                line_and_odds_are_different = (
                    (collected_betting_line_num_line not in
                    (update_push_stream_lines_query := update_push_query['stream']['lines'])) and # Todo: problem is here when one of the line and odds combos changed and one didn't
                    (collected_betting_line_odds not in
                    (update_push_stream_odds_query := update_push_query['stream']['odds']))
                )

                # don't want to add a duplicate -- important for scaling out to more collectors
                if line_and_odds_are_different:
                    update_push_stream_lines_query.append(collected_betting_line_num_line)
                    update_push_stream_odds_query.append(collected_betting_line_odds)

            else:
                # otherwise just add a new record to the stream
                update_push_query['stream'] = self._create_new_record(collected_betting_line)


            # if metrics_changed:

            # check if there are multiple numeric lines to bet on
            if update_set_query := update_query['$set']:

                # readability
                metrics_are_different = (
                    (collected_betting_line_ev not in
                    (update_set_ev_query := update_set_query['metrics']['ev'])) and
                    (collected_betting_line_tw not in
                    (update_set_tw_query := update_set_query['metrics']['tw_prb']))
                )

                # don't want to add a duplicate -- important for scaling out to more collectors
                if metrics_are_different:
                    update_set_ev_query.append(collected_betting_line_ev)
                    update_set_tw_query.append(collected_betting_line_tw)

            else:
                update_set_query['metrics'] = self._create_new_metrics_tracker(collected_betting_line_metrics)

            # else:
            #     # does a doc already exist for this line, if it does then there are multiple num lines to bet on.
            #     if doc := collected_betting_line_insert_docs[collected_betting_line_id]:
            #
            #         # for readability
            #         new_record = doc['stream'][0]
            #         line_and_odds_are_different = (
            #             collected_betting_line_num_line not in (new_record_lines := new_record['lines']) and
            #             collected_betting_line_odds not in (new_record_odds := new_record['odds'])
            #         )
            #
            #         # don't want to add a duplicate -- important for scaling out to more collectors
            #         if line_and_odds_are_different:
            #             new_record_lines.append(collected_betting_line_num_line)
            #             new_record_odds.append(collected_betting_line_odds)
            #
            #     else:
            #         collected_betting_line_insert_docs[collected_betting_line_id] = self._create_doc(collected_betting_line)

        # need to batch all inserts and updates for efficiency
        requests = []

        # create update operations
        for collected_betting_line_id, update_query in collected_betting_line_update_queries.items():
            update_op = pymongo.UpdateOne({'_id': collected_betting_line_id}, update_query, upsert=True)
            requests.append(update_op)

        # # create insert operations
        # for doc in collected_betting_line_insert_docs.values():
        #     insert_op = pymongo.InsertOne(doc)
        #     requests.append(insert_op)

        # # were any of the stored betting lines not collected?
        # for stored_betting_line_id, stored_betting_line_doc in stored_betting_lines.items():
        #
        #     # readability
        #     stored_betting_line_was_not_collected = (
        #         (stored_betting_line_id not in collected_betting_line_insert_docs.keys()) and
        #         (stored_betting_line_id not in collected_betting_line_update_queries.keys())
        #     )
        #
        #     if stored_betting_line_was_not_collected:
        #
        #         # frequently accessed
        #         stream = stored_betting_line_doc['stream']
        #
        #         # only need to add one empty record
        #         if len(stream[-1]) > 1:
        #
        #             # add a sentinel to the stream to indicate that the stored betting line was not collected
        #             stream.append({
        #                 'batch_timestamp': collected_betting_lines[0]['batch_timestamp'],
        #                 'status': 'nf',  # not found
        #                 'last_update_idx': stream[-1]['last_update_idx'],
        #             })
        #
        #             # add the update operation to the batch of requests
        #             update_op = pymongo.UpdateOne(
        #                 {'_id': stored_betting_line_id}, {'$set': { 'stream': stream } }
        #             )
        #             requests.append(update_op)

        await self.collection.bulk_write(requests)

        # except InvalidOperation as e:
        #     self.log_message(message=f'No betting lines to store: {e}', level='WARNING')
        #
        # except Exception as e:
        #     raise Exception(f'Failed to store betting lines: {e}')

    async def update_betting_line(self, betting_line_id: str, **kwargs):
        """
        Updates a betting line in the database.

        Args:
            betting_line_id (str): The unique ID of the betting line.
            **kwargs: The fields to update and along with other options.

        Returns:
            pymongo.UpdateOne: The update operation if return_op is True.
        """
        try:
            if kwargs.pop('return_op', False):
                return

            await self.collection.update_one({ '_id': betting_line_id }, { '$set': kwargs })

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to update betting line: {e}')

    @staticmethod
    def _get_query_for_betting_lines_by_game(box_score_dict: dict) -> dict:
        try:
            return {
                'game._id': box_score_dict['game_id']
            }

        except Exception as e:
            raise Exception(f'Failed to get query for betting lines by subject: {e}')

    async def update_live_stats(self, updated_live_games: dict, stored_box_score_docs: list[dict]) -> None:
        """
        Updates live stats for betting lines based on the given box scores.

        Args:
            updated_live_games (list[dict]): The list of updated live games.
            stored_box_score_docs (list[dict]): The list of box scores.
        """
        try:
            # Todo: !!!!!!!  LEFT OFF HERE !!!!!!!
            requests = []
            seen_games = set()
            irrelevant_games = set()

            for stored_box_score_doc in stored_box_score_docs:

                game_id = stored_box_score_doc['game_id']
                if game_id not in seen_games:
                    query = self._get_query_for_betting_lines_by_game(stored_box_score_doc)
                    game_filtered_betting_lines = await self.get_betting_lines(query)

                    if not game_filtered_betting_lines:
                        seen_games.add(game_id)
                        irrelevant_games.add(game_id)
                        self.log_message(message=f'_update_live_stats(): no betting lines for game {game_id}', level='INFO')
                        continue

                    # update for game lines
                    game_stats_dict = GameStatsDict(updated_live_games[game_id])
                    game_lines_market_domain_betting_lines = [
                        betting_line for betting_line in game_filtered_betting_lines
                        if betting_line['market_domain'] == 'Gamelines'
                    ]

                    self.log_message(
                        message=f'_update_live_stats(): found {len(game_lines_market_domain_betting_lines)} game lines for game {game_id}'
                        , level='INFO')

                    missed_markets = set()
                    for betting_line in game_lines_market_domain_betting_lines:
                        if stat_value := game_stats_dict.get(betting_line['market'],
                                                             betting_line['subject']):

                            update_op = await self.update_betting_line(
                                betting_line['_id'], return_op=True, live_stat=stat_value
                            )
                            requests.append(update_op)
                        else:
                            missed_markets.add(betting_line['market'])

                    # update for player props
                    player_stats_dict = PlayerStatsDict(stored_box_score_doc['box_score'])
                    player_prop_lines_market_domain_betting_lines = [
                        betting_line for betting_line in game_filtered_betting_lines
                        if betting_line['market_domain'] == 'PlayerProps'
                    ]

                    self.log_message(
                        message=f'In _update_live_stats found {len(player_prop_lines_market_domain_betting_lines)} player prop lines for {game_id}',
                        level='INFO')

                    for betting_line in player_prop_lines_market_domain_betting_lines:
                        if stat_value := player_stats_dict.get(betting_line['market']):

                            update_op = await self.update_betting_line(
                                betting_line['_id'], return_op=True, live_stat=stat_value
                            )
                            requests.append(update_op)
                        else:
                            missed_markets.add(betting_line['market'])


                    if missed_markets:
                        self.log_message(
                            message=f'In _update_live_stats could not get stat for markets in game {game_id}: {missed_markets}',
                            level='WARNING')

                    seen_games.add(game_id)

            if irrelevant_games:
                self.log_message(level='INFO', message=f'No betting lines for: {irrelevant_games}')

            await self.collection.bulk_write(requests)

        except InvalidOperation as e:
            self.log_message(message=f'No live stats to update: {e}', level='WARNING')

        except Exception as e:
            self.log_message(message=f'Failed to update live stats: {e}', level='EXCEPTION')

    async def store_completed_betting_lines(self, game_ids: list[dict] = None, in_gcs: bool = False) -> None:
        """
        Stores completed betting lines in the database and optionally in Google Cloud Storage.

        Args:
            game_ids (list[dict], optional): The list of game IDs to filter by.
            in_gcs (bool, optional): Whether to store the betting lines in Google Cloud Storage.
        """
        # Todo: ANY FINAL STATS TO ADD?, OUTPUT THE SIZE OF THE JSON FILE
        try:
            if game_ids:
                optimized_projection = "get_projection_for_gcs()"
                betting_lines_filtered_by_game = await self.get_betting_lines({
                    'game._id': {'$in': game_ids},
                }, optimized_projection)
                if betting_lines_filtered_for_live_stat := [
                    # only want to store betting lines that have a final box score stat
                    betting_line for betting_line in betting_lines_filtered_by_game if betting_line.get('live_stat')
                ]:
                    await self.completed_betting_lines_collection.insert_many(
                        betting_lines_filtered_for_live_stat)

                else:
                    self.log_message(
                        message=f'store_completed_betting_lines(): No completed betting lines to store because there are no live stats: {game_ids}',
                        level='WARNING')

                # await self.delete_betting_lines(betting_lines_filtered_by_game)

            if in_gcs:
                # await self._store_in_gcs()
                await self.completed_betting_lines_collection.delete_many({})

        except Exception as e:
            self.log_message(message=f'Failed to store completed betting lines: {e}', level='EXCEPTION')

    async def delete_betting_lines(self, betting_lines: list[dict] = None) -> None:
        """
        Deletes betting lines from the database.

        Args:
            betting_lines (list[dict], optional): The list of betting lines to delete.
        """
        try:
            if betting_lines:
                betting_line_ids = [betting_line['_id'] for betting_line in betting_lines]
                if await self.collection.delete_many({ '_id': { '$in': betting_line_ids } }):
                    self.log_message(
                        message=f'Deleted {len(betting_line_ids)} betting lines: {betting_line_ids}',
                        level='INFO')
                else:
                    raise Exception()

            elif betting_lines is None:
                if await self.collection.delete_many({}):
                    self.log_message(message='Deleted all betting lines', level='INFO')
                else:
                    raise Exception()

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to delete betting lines: {e}')
