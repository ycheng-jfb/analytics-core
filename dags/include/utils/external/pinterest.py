from base64 import b64encode
from dataclasses import dataclass

import requests


@dataclass
class Advertiser:
    advertiser_id: str
    advertiser_name: str

    @property
    def task_suffix(self):
        return f"{self.advertiser_name}_{self.advertiser_id}"


ADVERTISER_LIST = [
    Advertiser("549755823941", "fabkids"),
    Advertiser("549755814174", "justfab_na"),
    # Advertiser("549756020618", "justfab_uk"),  old account
    Advertiser("549756434410", "justfab_uk"),
    # Advertiser("549756437951", "fabletics_uk"),  old account
    Advertiser("549756048750", "fabletics_uk"),
    Advertiser("549755819129", "fabletics_us"),
    Advertiser("549756279276", "fabletics_ca"),
    Advertiser("549755823484", "shoedazzle"),
    Advertiser("549756786596", "savagex"),
    Advertiser("549757168366", "fabletics_de"),
    Advertiser("549757168354", "fabletics_fr"),
    Advertiser("549757168352", "fabletics_es"),
    Advertiser("549757168367", "justfab_de"),
    Advertiser("549757168353", "justfab_fr"),
    Advertiser("549757168351", "justfab_es"),
    Advertiser("549757168359", "savagex_uk"),
    Advertiser("549757168364", "savagex_de"),
    Advertiser("549757168356", "savagex_fr"),
    Advertiser("549757168361", "savagex_es"),
    Advertiser("549760729815", "justfab_eu"),
    Advertiser("549761337601", "fabletics_us_men"),
    Advertiser("549759331195", "fabletics_se"),
    Advertiser("549760823740", "justfab_de"),
]


def generate_pinterest_oauth_code_url(consumer_id, redirect_uri="https://localhost/"):
    """
    use this to generate URL to start oauth flow to get code
    """

    url = (
        f"https://www.pinterest.com/oauth/"
        f"?consumer_id={consumer_id}"
        f"&redirect_uri={redirect_uri}"
        "&response_type=code"
    )
    print(url)
    return url


def generate_pinterest_token(client_id: str, redirect_uri: str, code: str, secret: str):
    url = "https://api.pinterest.com/v3/oauth/access_token/"
    grant_type = "authorization_code"
    basic_auth = b64encode(f"{client_id}:{secret}".encode('utf-8')).decode('utf-8')
    headers = {"Authorization": f"Basic {basic_auth}"}
    payload = [("code", code), ("grant_type", grant_type), ("redirect_uri", redirect_uri)]

    req = requests.put(url, params=payload, headers=headers)
    try:
        token = req.json()["data"]["access_token"]
        return token
    except Exception:
        print(req.json())
        raise
