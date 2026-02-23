"""
    Загрузка синтетических данных в Silver слой. Загрузка данных о университетах.
"""

import psycopg2
import pandas as pd 
import hashlib
import numpy as np
from typing import NoReturn
from datetime import datetime, timedelta
import json
import io
import pytz
import boto3
from botocore.config import Config

from airflow.decorators import task, dag
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import Variable

S3_YANDEX_OBJECT_STORAGE_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_KEY')
S3_YANDEX_OBJECT_STORAGE_SECRET_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_SECRET_KEY')
S3_YANDEX_OBJECT_STORAGE_ENDPOINT = Variable.get('S3_YANDEX_OBJECT_STORAGE_ENDPOINT')


class LoadToSilverLayerSyntetic:
    """ Загрузка синтетических данных в Silver слой """

    connection = psycopg2.connect(
        host=Variable.get('POSTGRES_HOST'),
        user=Variable.get('POSTGRES_USER'),
        password=Variable.get('POSTGRES_PASSWORD'),
        database=Variable.get('POSTGRES_DATABASE'),
    )

    def __init__(self, date_report: datetime): 
        self.syntetic_urls = { 
            "students_results_exam": f"bronze_layer/students_results_exam/students_results_{date_report.strftime("%Y-%m-%d")}.parquet", 
            "students_main_info": f"bronze_layer/students_main_info/students_main_info_{date_report.strftime("%Y-%m-%d")}.parquet", 
            "university_world_info": "bronze_layer/university_world_info/university_world_info.parquet"   
        }

    def get_data_from_object_storage(self, key: str) -> pd.DataFrame: 
        """ Получение данных из Object storage """

        session = boto3.session.Session()
        session = boto3.Session(
            aws_access_key_id=S3_YANDEX_OBJECT_STORAGE_KEY,
            aws_secret_access_key=S3_YANDEX_OBJECT_STORAGE_SECRET_KEY,
            region_name="ru-central1"
        )   
        object_storage = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net', 
            config=Config(region_name='ru-central1', signature_version='v4')
        )
        response = object_storage.get_object(Bucket='education-dwh', Key=key)
        parquet_buffer = io.BytesIO(response['Body'].read())
        data_from_object_storage = pd.read_parquet(parquet_buffer)
        return data_from_object_storage
    
    def load_to_silver_layer(self): 
        """ Загрузка данных Silver слой """

        cursor = self.connection.cursor()

        students_results_exam = self.get_data_from_object_storage(key=self.syntetic_urls['students_results_exam'])
        print(self.syntetic_urls['students_main_info'])
        students_main_info = self.get_data_from_object_storage(key=self.syntetic_urls['students_main_info'])
        university_world_info = self.get_data_from_object_storage(key=self.syntetic_urls['university_world_info'])

        for item in university_world_info.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.university_world_info (name_university, alpha_two_code, country_name, 
                                                           web_pages, state_province, domains, load_dttm)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name_university) DO NOTHING
                """, (
                    item.name_university,
                    item.alpha_two_code,
                    item.country_name,
                    item.web_pages,
                    item.state_province,
                    item.domains,
                    datetime.now(pytz.timezone('Europe/Moscow')).strftime("%Y-%m-%d")
                )
            )

        self.connection.commit()

        for item in students_main_info.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.students_main_info (student_guid, student_fio, 
                                                             student_school, student_class, student_birthday, load_dttm)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (student_guid) DO NOTHING
                """, (
                    item.student_guid,
                    item.student_fio,
                    item.student_school,
                    item.student_class,
                    item.student_birthday,
                    datetime.now(pytz.timezone('Europe/Moscow')).strftime("%Y-%m-%d")
                )
            )

        self.connection.commit()

        for item in students_results_exam.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.students_results (student_guid, name_exam, subject, 
                                                           primary_score, mark, min_primary_score, max_primary_score, 
                                                           number_protocol_gak, data_protocol_gak, date_event, load_dttm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (student_guid) DO NOTHING
                """, (
                    item.student_guid,
                    item.name_exam,
                    item.subject,
                    item.primary_score,
                    item.mark,
                    item.min_primary_score,
                    item.max_primary_score,
                    item.number_protocol_gak,
                    item.data_protocol_gak,
                    item.date_event,
                    datetime.now(pytz.timezone('Europe/Moscow')).strftime("%Y-%m-%d")
                )
            )

        self.connection.commit()


DEFAULT_ARGS = {
    'start_date': datetime(2025, 12, 14), 
    'end_date': datetime(2045, 1, 1),
    'owner': 'dmitriy_zhdanov',
    'retries': 4, 
    'retry_delay': timedelta(minutes=5), 
    'poke_interval': 600 
}
@dag('silver_load_to_syntetic_model', 
     schedule=CronTriggerTimetable('30 5 * * *', timezone='Europe/Moscow'), 
     default_args=DEFAULT_ARGS, max_active_tasks=3, tags=['silver', 'load_syntetic_model'], 
     catchup=False)
def taskflow():

    @task
    def get_data_load_to_syntetic_data_model():
        loader = LoadToSilverLayerSyntetic(date_report=datetime.now(pytz.timezone('Europe/Moscow'))-timedelta(days=1))
        loader.load_to_silver_layer()

    get_data_load_to_syntetic_data_model = get_data_load_to_syntetic_data_model()

taskflow = taskflow()
