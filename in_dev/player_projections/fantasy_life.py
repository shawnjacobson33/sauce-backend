import requests


cookies = {
    '_cb': 'reVl4Cok-SDCcNB0W',
    '_cb_svref': 'https%3A%2F%2Fwww.google.com%2F',
    'ab.storage.deviceId.7bbdb005-3364-47c1-bc4a-51bc597b8471': 'g%3A02720ed1-7497-5aa8-8c10-abf5e3d87c36%7Ce%3Aundefined%7Cc%3A1722356720114%7Cl%3A1736663873750',
    'laravel_session': 'ynNLlQaSI5R4vr6HYujG9SJGyDr0ezfyF0rCdSpQ',
    '_ga': 'GA1.1.58895470.1736663874',
    '_gcl_au': '1.1.1533066235.1736663874',
    '_cq_duid': '1.1736663873.2k0mTByYytKMySvw',
    '_cq_suid': '1.1736663873.WuJwWbGnuqZWcYCO',
    '_pbjs_userid_consent_data': '3524755945110770',
    '_sharedid': '653136b0-2aa5-4c32-a357-d67e81f82564',
    '_fbp': 'fb.1.1736663874046.101436233892173944',
    'pbjs_fabrickId': '%7B%22fabrickId%22%3A%22E1%3Auq8Ezm4ej1kfIBK6AAj9gQSi0nsKFgn9N3rulx2hmwZWqGSVOnA4o4Vlbq4i4yWxLHWiaVCw9J58dpHpGlA9PY7WwY6ArqAsGnR28JlTtfTm9CY5gCAa8Dch58gLxyhd%22%7D',
    'FCNEC': '%5B%5B%22AKsRol_iqXgXhdsWWTW_uGuoXFY2OY_wulN65eGNaBLphCQ57p7XaVosV-MkVtsaaNOTLqWq2kW6oFflyIrQWOYWhPs7F9CbS0qRRhalQn7aOAdmiDadSKh90mj2cy-XHxcHDypoDKYAOWIXAWQZWek4tJhf5He_OA%3D%3D%22%5D%5D',
    '_chartbeat2': '.1736663873682.1736663930128.1.CJOtgtCtrvo6kn1z1B9c_yyB1MGXE.2',
    '_ga_NHVENXJDDR': 'GS1.1.1736663873.1.1.1736663930.3.0.648492465',
    'ab.storage.sessionId.7bbdb005-3364-47c1-bc4a-51bc597b8471': 'g%3Ae05069d9-07e9-8440-c84d-f05f9c6bf0a7%7Ce%3A1736665730166%7Cc%3A1736663873750%7Cl%3A1736663930166',
    'cto_bundle': 'Paz6r18zR0xkam1UZmJyNXNXNEt3TmVQc0g5azJ0aiUyQjZxU29NSXByNGk2ZzhFSUdMTFY3NzdMcTBVeXNNa2luZ0dCQlVnUHJWb214UmxBRXlXJTJCJTJCYTR6NEtaZGUyck9nYXhwWW5TZ25qSWNNejAlMkZQVFNWYUolMkJaYnlMaDhMVFRZalFCQ3F5V3lUTGUlMkJaMWs5MDlyejhBOFpTcWdkNnRrNHhEaWJmelFnZFlyeUt5ZmolMkJsdzdPRFMlMkZPTkVhS2tNUUl6Wld2RmRENDZOa3VUWXkzNWhyOUdpc01jQSUzRCUzRA',
    'cto_bidid': '2S-rjV9MQloxNjB4REFUbmlGUFZsOGZDV2RZQk1QcjB3S3RLSXh5WWdva08zSkVkMVlyV1U0V3lOcGk3RmY4TGxDZjdNQjZ1SDNBMFZuMmI4MndVZ2dOUjhTRWJMaFVZJTJCdlpwR3VxZTd0NUd1YzMwZzUwank3RGVtOG9Hc3RFcjEwa0w3WnJ0SVhtakVCRDVmMEJUQ05NZFdsZyUzRCUzRA',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    # 'cookie': '_cb=reVl4Cok-SDCcNB0W; _cb_svref=https%3A%2F%2Fwww.google.com%2F; ab.storage.deviceId.7bbdb005-3364-47c1-bc4a-51bc597b8471=g%3A02720ed1-7497-5aa8-8c10-abf5e3d87c36%7Ce%3Aundefined%7Cc%3A1722356720114%7Cl%3A1736663873750; laravel_session=ynNLlQaSI5R4vr6HYujG9SJGyDr0ezfyF0rCdSpQ; _ga=GA1.1.58895470.1736663874; _gcl_au=1.1.1533066235.1736663874; _cq_duid=1.1736663873.2k0mTByYytKMySvw; _cq_suid=1.1736663873.WuJwWbGnuqZWcYCO; _pbjs_userid_consent_data=3524755945110770; _sharedid=653136b0-2aa5-4c32-a357-d67e81f82564; _fbp=fb.1.1736663874046.101436233892173944; pbjs_fabrickId=%7B%22fabrickId%22%3A%22E1%3Auq8Ezm4ej1kfIBK6AAj9gQSi0nsKFgn9N3rulx2hmwZWqGSVOnA4o4Vlbq4i4yWxLHWiaVCw9J58dpHpGlA9PY7WwY6ArqAsGnR28JlTtfTm9CY5gCAa8Dch58gLxyhd%22%7D; FCNEC=%5B%5B%22AKsRol_iqXgXhdsWWTW_uGuoXFY2OY_wulN65eGNaBLphCQ57p7XaVosV-MkVtsaaNOTLqWq2kW6oFflyIrQWOYWhPs7F9CbS0qRRhalQn7aOAdmiDadSKh90mj2cy-XHxcHDypoDKYAOWIXAWQZWek4tJhf5He_OA%3D%3D%22%5D%5D; _chartbeat2=.1736663873682.1736663930128.1.CJOtgtCtrvo6kn1z1B9c_yyB1MGXE.2; _ga_NHVENXJDDR=GS1.1.1736663873.1.1.1736663930.3.0.648492465; ab.storage.sessionId.7bbdb005-3364-47c1-bc4a-51bc597b8471=g%3Ae05069d9-07e9-8440-c84d-f05f9c6bf0a7%7Ce%3A1736665730166%7Cc%3A1736663873750%7Cl%3A1736663930166; cto_bundle=Paz6r18zR0xkam1UZmJyNXNXNEt3TmVQc0g5azJ0aiUyQjZxU29NSXByNGk2ZzhFSUdMTFY3NzdMcTBVeXNNa2luZ0dCQlVnUHJWb214UmxBRXlXJTJCJTJCYTR6NEtaZGUyck9nYXhwWW5TZ25qSWNNejAlMkZQVFNWYUolMkJaYnlMaDhMVFRZalFCQ3F5V3lUTGUlMkJaMWs5MDlyejhBOFpTcWdkNnRrNHhEaWJmelFnZFlyeUt5ZmolMkJsdzdPRFMlMkZPTkVhS2tNUUl6Wld2RmRENDZOa3VUWXkzNWhyOUdpc01jQSUzRCUzRA; cto_bidid=2S-rjV9MQloxNjB4REFUbmlGUFZsOGZDV2RZQk1QcjB3S3RLSXh5WWdva08zSkVkMVlyV1U0V3lOcGk3RmY4TGxDZjdNQjZ1SDNBMFZuMmI4MndVZ2dOUjhTRWJMaFVZJTJCdlpwR3VxZTd0NUd1YzMwZzUwank3RGVtOG9Hc3RFcjEwa0w3WnJ0SVhtakVCRDVmMEJUQ05NZFdsZyUzRCUzRA',
    'origin': 'https://www.fantasylife.com',
    'priority': 'u=1, i',
    'referer': 'https://www.fantasylife.com/tools/nba-player-projections',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

json_data = {
    'columnFilters': {},
    'dataFilters': {},
    'apiFilters': {},
    'scoring': None,
    'initialFilters': {},
    'perPage': 25,
    'sort': '',
    'page': 1,
    'search': '',
    'direction': 1,
}

response = requests.post(
    'https://www.fantasylife.com/api/datatables/v2/table-contents/9d4f66b7-083d-41b0-ba2f-279afc4f5390/ajax',
    cookies=cookies,
    headers=headers,
    json=json_data,
)

asd = 123