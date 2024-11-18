import asyncio

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.logistics import utils as lg_utils
from app.backend.data_collection.logistics.games import utils as gm_utils


class RosterRetriever(dc_utils.Retriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self) -> None:
        # get the url for retrieving subjects
        url = lg_utils.get_url(self.source, 'subjects')
        # make a network request to extract all teams for which we want subjects
        if teams := dc_utils.Teams.get_teams(self.source.league):
            # initialize a list to hold requests to make
            tasks = list()
            # for each abbreviated and full name for each team
            for abbr, full in teams:
                # format the url with the names
                formatted_url = url.format(abbr, full)
                # add the request to tasks
                tasks.append(lg_utils.fetch(formatted_url, self._parse_subjects))

            await asyncio.gather(*tasks)

    def _parse_subjects(self, html_content, abbr: str, full: str):
        pass