
import scrapy
import cloudscraper


class BetOnlineSpider(scrapy.Spider):
    name = "betonline-spider"

    custom_settings = {
        'ITEM_PIPELINES': {
            'liveodds.pipelines.MongoDBPipeline': 300
        }
    }

    def start_requests(self):
        url = "https://api-offering.betonline.ag/api/offering/Sports/offering-by-league"
        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'gsetting': 'bolsassite',
            'origin': 'https://sports.betonline.ag',
            'priority': 'u=1, i',
            'referer': 'https://sports.betonline.ag/',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'utc-offset': '300',
        }
        json_data = {
            'Sport': 'baseball',
            'League': 'mlb',
            'ScheduleText': None,
            'filterTime': 0,
            'Period': 1,
        }

        scraper = cloudscraper.create_scraper()
        response = scraper.post(url=url, headers=headers, json=json_data)
        if response.status_code == 200:
            scrapy_response = scrapy.http.TextResponse(
                url=url,
                status=response.status_code,
                headers=response.headers,
                body=response.content,
                encoding='utf-8',
                meta={'req_headers': headers, 'scraper': scraper,
                      "sport": json_data['Sport'], "league": json_data['League']}
            )
            yield self.parse_game_ids(scrapy_response)
        else:
            self.logger.error(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_game_ids(self, response):
        data = response.json()

        league = data['GamesOffering']['League']
        for game in data['GamesDescription']:
            url = 'https://api-offering.betonline.ag/api/offering/sports/get-event'
            data = f'{{"sport":"{response.meta("sport")}","league":"{response.meta("league")}","gameID":{game["Game"]["GameId"]},"scheduleText":null}}'

            response = response.meta('scraper').post(url=url, headers=response.meta('req_headers'), data=data)
            if response.status_code == 200:
                scrapy_response = scrapy.http.TextResponse(
                    url=url,
                    status=response.status_code,
                    headers=response.headers,
                    body=response.content,
                    encoding='utf-8',
                    meta={'league': league}
                )
                yield self.parse_lines(scrapy_response)
            else:
                self.logger.error(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_lines(self, response):
        data = response.json()

        league = response.meta('league')
        for event in data['EventOffering']['ContestTypes']:
            for group in event['DescriptionGroup']:
                for contest in group['Contests']:
                    for contestant in contest['Contestants']:
                        last_updated = ""
                        market = contest['Description'] if not contest['ThresholdUnits'] else contest['ThresholdUnits']
                        start_datetime = contest['ContestDateTime']

                        subject = contestant['Name']
                        if 'by' in subject:
                            comps = subject.split(' by ')
                            subject = comps[0]
                            line = comps[1]





