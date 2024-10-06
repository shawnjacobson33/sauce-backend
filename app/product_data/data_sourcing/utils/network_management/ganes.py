import requests

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Referer': 'https://bv2-us.digitalsportstech.com/betbuilder?sb=betonline',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjE4OTYwNDQiLCJhcCI6IjEyMzgxNDYzODMiLCJpZCI6IjIzMTY4M2RmMDdhY2YxZDMiLCJ0ciI6IjYzODkyZGMzOTcyNGRkNDI0NTA1ZTQyNWJjYmU1NjEwIiwidGkiOjE3Mjc5OTI4MTA2ODh9fQ==',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'traceparent': '00-63892dc39724dd424505e425bcbe5610-231683df07acf1d3-01',
    'tracestate': '1896044@nr=0-1-1896044-1238146383-231683df07acf1d3----1727992810688',
}

params = {
    'sb': 'betonline',
    'gameId': '66b23043773a0decddbada6f',
    'statistic': 'Receiving%2520Yards',
}

response = requests.get('https://bv2-us.digitalsportstech.com/api/dfm/marketsByOu', params=params, headers=headers)
data = response.json()
print(response.status_code)
print(response.content)
