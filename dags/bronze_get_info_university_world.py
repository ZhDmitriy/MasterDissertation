"""
    Получает информацию о университетах со всего мира
"""

import pycountry
import requests
import pandas as pd 
from datetime import datetime, timedelta
import json
import time
import random
from io import BytesIO
import boto3
from botocore.config import Config

from airflow.decorators import task, dag
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import Variable


COUNTRIES_LIST = list(pycountry.countries)


class UniversityInfo: 
    """ Информация о университетах """

    def __init__(self, country_name: str):
        self.country_name = country_name

    def make_requests(self) -> pd.DataFrame: 
        """ Отправляем запрос на получение информации о университетах по опреденной стране """
        time.sleep(random.choice([7]))

        # Поля отчета
        columns = ['name_university', 'alpha_two_code', 'country_name', 
                   'web_pages', 'state_province', 'domains'] 
        
        results_row = []
        BASE_URL = "http://universities.hipolabs.com/search?country="
        BASE_URL += str(self.country_name)
        try: 
            response = requests.get(url=BASE_URL)
            if response.status_code == 200: 
                if len(response.json()) == 0: 
                    print("Нет данных о университетах в переданной стране")
                else:
                    for item in response.json():
                        results_row.append([item['name'], item['alpha_two_code'], 
                                            item['country'], item['web_pages'][0], 
                                            item['state-province'], item['domains'][0]])
            else: 
                print(f"Ошибка запроса: {response.json}")
        except Exception as e: 
            print(f"{e}")
            response = []

        return pd.DataFrame(data=results_row, columns=columns)
    

S3_YANDEX_OBJECT_STORAGE_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_KEY')
S3_YANDEX_OBJECT_STORAGE_SECRET_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_SECRET_KEY')
S3_YANDEX_OBJECT_STORAGE_ENDPOINT = Variable.get('S3_YANDEX_OBJECT_STORAGE_ENDPOINT')


DEFAULT_ARGS = {
    'start_date': datetime(2025, 10, 10), 
    'end_date': datetime(2045, 1, 1),
    'owner': 'dmitriy_zhdanov',
    'retries': 4, 
    'retry_delay': timedelta(minutes=5), 
    'poke_interval': 600 
}


@dag('bronze_get_info_university_world', 
     schedule=CronTriggerTimetable('30 7 * * *', timezone='Europe/Moscow'), 
     default_args=DEFAULT_ARGS, max_active_tasks=3, tags=['bronze', 'university_world'], 
     catchup=False)
def taskflow():

    @task 
    def get_info_university_world():
        """ Получаем информация о университетах по всему миру """

        data_university_results = []
        for country in COUNTRIES_LIST: 
            print(f"Обработана страна: {country.name}")
            country_university = UniversityInfo(country_name=country.name)
            data_university = country_university.make_requests()
            data_university_results.append(data_university)

        data_total = pd.concat(data_university_results)

        session = boto3.session.Session()
        session = boto3.Session(
            aws_access_key_id=S3_YANDEX_OBJECT_STORAGE_KEY,
            aws_secret_access_key=S3_YANDEX_OBJECT_STORAGE_SECRET_KEY,
            region_name="ru-central1"
        )   

        s3_client = session.client(
            "s3", 
            endpoint_url=S3_YANDEX_OBJECT_STORAGE_ENDPOINT, 
            config=Config(region_name='ru-central1', signature_version='v4')
        )

        files_to_upload = [
            (f"university_world_info.parquet", data_total)
        ]

        for file_name, df in files_to_upload:
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer) 
            parquet_buffer.seek(0) 
            
            folder_name = ""
            if 'university_world_info' in file_name: 
                folder_name = 'bronze_layer/university_world_info/'
            
            s3_client.put_object(
                Bucket="education-dwh", 
                Key=folder_name + file_name, 
                Body=parquet_buffer.getvalue(),
                ContentType="application/octet-stream"
            )

    get_info_university_world = get_info_university_world()

    get_info_university_world

taskflow = taskflow()