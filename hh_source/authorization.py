""" Источник данных hh.ru """


class OAuthApi:
    """ Авторизация и получение токена для источника данных hh.ru """

    def __init__(self):
        self.BASE_URL = "https://hh.ru/oauth/token"

