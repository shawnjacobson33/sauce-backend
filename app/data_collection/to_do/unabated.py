import requests



# THIS GETS BOOKMAKER DATA
cookies = {
    'ajs_anonymous_id': '74b0e951-cfa0-47f9-bc1a-990376617790',
    '_gid': 'GA1.2.1404062397.1730758331',
    '_gat_UA-191825373-1': '1',
    '_ga': 'GA1.1.1125158927.1730758331',
    '_ga_492GH3V3GE': 'GS1.1.1730758330.1.1.1730759452.59.0.0',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    # 'cookie': 'ajs_anonymous_id=74b0e951-cfa0-47f9-bc1a-990376617790; _gid=GA1.2.1404062397.1730758331; _gat_UA-191825373-1=1; _ga=GA1.1.1125158927.1730758331; _ga_492GH3V3GE=GS1.1.1730758330.1.1.1730759452.59.0.0',
    'origin': 'https://unabated.com',
    'priority': 'u=1, i',
    'referer': 'https://unabated.com/',
    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'x-amz-cf-v-id': 'dNSob7rQOiJFE_59NDSm7tuZ6ed5MA_8w8CHVqr8tGSFYpgzEUqBuQ==',
}

response = requests.get('https://api-k.unabated.com/api/users/settings', cookies=cookies, headers=headers)


# THIS GETS ACTUAL BETTING LINES
cookies = {
    'ajs_anonymous_id': '74b0e951-cfa0-47f9-bc1a-990376617790',
    '_gid': 'GA1.2.1404062397.1730758331',
    '_gat_UA-191825373-1': '1',
    '_ga': 'GA1.1.1125158927.1730758331',
    '_ga_492GH3V3GE': 'GS1.1.1730758330.1.1.1730759452.59.0.0',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    # 'cookie': 'ajs_anonymous_id=74b0e951-cfa0-47f9-bc1a-990376617790; _gid=GA1.2.1404062397.1730758331; _gat_UA-191825373-1=1; _ga=GA1.1.1125158927.1730758331; _ga_492GH3V3GE=GS1.1.1730758330.1.1.1730759452.59.0.0',
    'origin': 'https://unabated.com',
    'priority': 'u=1, i',
    'referer': 'https://unabated.com/',
    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'x-amz-cf-v-id': 'dNSob7rQOiJFE_59NDSm7tuZ6ed5MA_8w8CHVqr8tGSFYpgzEUqBuQ==',
}

response = requests.get('https://api-k.unabated.com/api/markets/playerProps/changes', cookies=cookies, headers=headers)
asd = response.json()