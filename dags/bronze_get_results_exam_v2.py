"""
    Загружаем данные о результатах экзаменов учеников в Object Storage
"""

import pandas as pd 
from faker import Faker
from datetime import datetime, timedelta
import random
from io import BytesIO
import boto3
from botocore.config import Config

from airflow.decorators import task, dag
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import Variable


class ExamStudents: 
    """ Результаты студентов """

    def __init__(self, num_records: int): 
        self.fake = Faker('ru_RU')
        self.subjects = {
                         "Русский язык": [15, 37],
                         "Математика": [8, 31], 
                         "Литература": [12, 37],
                         "Физика": [10, 39], 
                         "Химия": [9, 38], 
                         "Биология": [13, 47], 
                         "История": [13, 44], 
                         "Обществознание": [14, 37], 
                         "География": [12, 31]
                         }
        self.num_records = num_records
        self.students_guids = [self.fake.uuid4() for _ in range(self.num_records)]

    def calculate_mark(self, score, max_score) -> int:
        """ Расчет оценки на основе баллов """
        percentage = score / max_score
        if percentage >= 0.85:
            return 5
        elif percentage >= 0.7:
            return 4
        elif percentage >= 0.5:
            return 3
        else:
            return 2
        
    def generate_exam_data(self) -> pd.DataFrame:
        """ Генерация данных о сдаче основного государственного экзамена """
        exams = []

        for item in range(self.num_records):
            subject = random.choice(list(self.subjects.keys()))
            min_max_score = self.subjects[subject]

            exam_date = self.fake.date_between(start_date='-60d', end_date='today')
            protocol_date = exam_date + timedelta(days=random.randint(5, 15))
            primary_score = random.randint(min_max_score[0], min_max_score[1])
            mark = self.calculate_mark(primary_score, min_max_score[1])

            exam_row = {
                'student_guid': self.students_guids[item],
                'name_exam': 'ОГЭ',
                'subject': subject,
                'date_event': exam_date.strftime('%Y-%m-%d'),
                'primary_score': primary_score,
                'mark': mark,
                'min_primary_score': min_max_score[0],
                'max_primary_score': min_max_score[1],
                'number_protocol_gak': f"{random.randint(50, 100)}рез",
                'data_protocol_gak': protocol_date.strftime('%Y-%m-%d')
            }

            exams.append(exam_row)

        return pd.DataFrame(data=exams, columns=['student_guid', 'name_exam', 'subject', 'date_event', 'primary_score', 
                                                 'mark', 'min_primary_score', 'max_primary_score', 
                                                 'number_protocol_gak', 'data_protocol_gak'])

    def generate_student_info(self):
        """ Генерация информации о студентах """
        students = []
        
        for student_guid in self.students_guids:
            student = {
                'student_guid': student_guid,
                'student_fio': self.fake.name(),
                'student_school': f"ГБОУ СОШ №{random.randint(1, 1500)}",
                'student_class': f"{random.randint(9, 11)}{random.choice(['А', 'Б', 'В', 'Г'])}",
                'student_birthday': self.fake.date_of_birth(minimum_age=14, maximum_age=17).strftime('%Y-%m-%d')
            }
            students.append(student)
            
        return pd.DataFrame(students)
    

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

@dag('bronze_get_results_students_exam', 
     schedule=CronTriggerTimetable('30 7 * * *', timezone='Europe/Moscow'), 
     default_args=DEFAULT_ARGS, max_active_tasks=3, tags=['bronze', 'exam_results'], 
     catchup=False)
def taskflow(): 

    @task
    def get_exam_results_and_load_to_object_storage():
        """ Получаем результаты экзамена по каждому предмету и загружаем в объектное хранилище """

        examStudents = ExamStudents(num_records=5000)
        data_students_results = examStudents.generate_exam_data()
        data_students_main_info = examStudents.generate_student_info()

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
            (f"students_results_{datetime.now().strftime('%Y-%m-%d')}.parquet", data_students_results),
            (f"students_main_info_{datetime.now().strftime('%Y-%m-%d')}.parquet", data_students_main_info)
        ]

        for file_name, df in files_to_upload:
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer) 
            parquet_buffer.seek(0) 
            
            folder_name = ""
            if 'students_main_info' in file_name: 
                folder_name = 'bronze_layer/students_main_info/'
            elif 'students_results' in file_name:
                folder_name = 'bronze_layer/students_results_exam/'
            
            s3_client.put_object(
                Bucket="education-dwh", 
                Key=folder_name + file_name, 
                Body=parquet_buffer.getvalue(),
                ContentType="application/octet-stream"
            )

    get_exam_results_and_load_to_object_storage = get_exam_results_and_load_to_object_storage()

    get_exam_results_and_load_to_object_storage

taskflow = taskflow()