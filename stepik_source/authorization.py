""" Источник данных stepik.org """

import requests
import os
from dotenv import load_dotenv


class OAuthApi:
    """ Авторизация и получение токена для источника данных stepik.org """

    def __init__(self):
        self.BASE_URL = "https://stepik.org/oauth2/token/"

    def getToken(self) -> str:
        load_dotenv()
        auth = requests.auth.HTTPBasicAuth(os.environ.get("CLIENT_ID_STEPIK"), os.environ.get("CLIENT_SECRET_STEPIK"))
        response = requests.post(url=self.BASE_URL,
                                 data={'grant_type': 'client_credentials'},
                                 auth=auth
                                )
        return response.json()['access_token']


