"""
    Перегрузка данных из Bronze слоя в Silver слой для источника данных Stepik
"""

import psycopg2
import pandas as pd 
import numpy as np
from datetime import datetime, timedelta
import json
import io
import boto3
from botocore.config import Config

from airflow.decorators import task, dag
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import Variable

S3_YANDEX_OBJECT_STORAGE_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_KEY')
S3_YANDEX_OBJECT_STORAGE_SECRET_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_SECRET_KEY')
S3_YANDEX_OBJECT_STORAGE_ENDPOINT = Variable.get('S3_YANDEX_OBJECT_STORAGE_ENDPOINT')


class LoaderDVmodelStepik: 
    """" Загрузчик данных в модель данных Data Vault для источника данных Stepik """

    source_id = 1
    source_name = 'stepik'
    connection = psycopg2.connect(
        host=Variable.get('POSTGRES_HOST'),
        user=Variable.get('POSTGRES_USER'),
        password=Variable.get('POSTGRES_PASSWORD'),
        database=Variable.get('POSTGRES_DATABASE'),
    )
    stepik_urls = {
        "course_popular": "bronze_layer/courses_popular_stepik/courses_popular_stepik.parquet", 
        "course_popular_lessons": "bronze_layer/lessons_by_courses_stepik/lessons_by_courses_stepik.parquet"
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
        courses_popular = pd.read_parquet(parquet_buffer)
        return courses_popular
    
    def load_data_to_dv_model(self):
        """ Загрузка данных в Data Vault модель базы данных """
        course_popular = self.get_data_from_object_storage(key=self.stepik_urls['course_popular'])
        course_popular['id'] = course_popular['id'].astype(int)

        course_popular_lessons = self.get_data_from_object_storage(key=self.stepik_urls['course_popular_lessons'])
        course_popular_lessons['id'] = course_popular_lessons['id'].astype(int)

        cursor = self.connection.cursor()

        # загрузка хабов
        stepik_popular_course = pd.read_sql(sql="""
            SELECT t.course_id, t.load_dttm, t.source_id, t.source_name
            FROM silver_layer.hub_stepik_course t 
        """, con=self.connection)
        stepik_popular_course['course_id'] = stepik_popular_course['course_id'].astype(int)

        mask = ~course_popular['id'].isin(stepik_popular_course['course_id'])
        course_popular_for_load_hub = course_popular.loc[mask, ['id', 'load_dttm']]        

        for item in course_popular_for_load_hub.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.hub_stepik_course (course_id, load_dttm, source_id, source_name)
                VALUES (%s, %s, %s, %s)
                """, (
                    item.id,
                    item.load_dttm,
                    self.source_id,
                    self.source_name
                    )
                )
            
        self.connection.commit()

        stepik_popular_course_lesson = pd.read_sql(sql="""
            SELECT t.lesson_id, t.load_dttm, t.source_id, t.source_name
            FROM dwh.silver_layer.hub_stepik_lesson t
        """, con=self.connection)
        stepik_popular_course_lesson['lesson_id'] = stepik_popular_course_lesson['lesson_id'].astype(int)

        mask = ~course_popular_lessons['id'].isin(stepik_popular_course_lesson['lesson_id'])
        course_popular_lessons_for_load_hub = course_popular_lessons.loc[mask, ['id', 'load_dttm']]    

        for item in course_popular_lessons_for_load_hub.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.hub_stepik_lesson (lesson_id, load_dttm, source_id, source_name)
                VALUES (%s, %s, %s, %s)
                """, (
                    item.id,
                    item.load_dttm,
                    self.source_id,
                    self.source_name
                    )
                )
            
        self.connection.commit()

        # загрузка линков
        stepik_link_course_lesson = pd.read_sql(sql="""
            SELECT t.link_stepik_course_lesson_hash_key, t.course_id, t.lesson_id, t.source_id, t.load_dttm
            FROM dwh.silver_layer.link_stepik_course_lesson t
        """, con=self.connection)

        mask = ~course_popular_lessons['linkHashCourseLesson'].isin(stepik_link_course_lesson['link_stepik_course_lesson_hash_key'])
        course_popular_lessons_for_load_link = course_popular_lessons.loc[mask, ['id', 'course_id', 'linkHashCourseLesson', 'load_dttm']]

        for item in course_popular_lessons_for_load_link.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.link_stepik_course_lesson (link_stepik_course_lesson_hash_key, course_id, lesson_id, source_id, load_dttm)
                VALUES (%s, %s, %s, %s, %s)
                """, (
                    item.linkHashCourseLesson,
                    item.course_id,
                    item.id,
                    self.source_id,
                    item.load_dttm,
                    )
                )
            
        self.connection.commit()

        # загрузка саттелитов
        stepik_sat_course_popular = pd.read_sql(sql="""
            SELECT t.course_id, t.load_dttm, t.hashkeydiff
            FROM dwh.silver_layer.sat_stepik_courses t
        """, con=self.connection)
        stepik_sat_course_popular['course_id'] = stepik_sat_course_popular['course_id'].astype(int)

        mask = ~course_popular['id'].isin(stepik_sat_course_popular['course_id'])
        course_popular_for_load_sat = course_popular.loc[mask]

        def convert_value(val):
            """
            Конвертирует значение для вставки в PostgreSQL.
            """
            
            if isinstance(val, dict):
                return json.dumps(val, ensure_ascii=False, default=str)
            
            if isinstance(val, (list, np.ndarray)):
                if isinstance(val, np.ndarray):
                    val = val.tolist()
                if any(isinstance(item, (dict, list, np.ndarray)) for item in val):
                    return json.dumps(val, ensure_ascii=False, default=str)
                return val
            
            if isinstance(val, np.integer):
                return int(val)
            if isinstance(val, np.floating):
                return float(val) if not pd.isna(val) else None
            
            if isinstance(val, (datetime, pd.Timestamp)):
                return val.isoformat()
            
            if isinstance(val, bool):
                return val
        
            return val

        for item in course_popular_for_load_sat.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.sat_stepik_courses (
                    course_id, load_dttm, summary, workload, cover, intro, course_format, 
                    certificate_footer, certificate_cover_org, is_certificate_issued, 
                    instructors, tags, has_tutors, is_enabled, review_summary, schedule_type, 
                    certificates_count, learners_count, lessons_count, quizzes_count, 
                    challenges_count, peer_reviews_count, instructor_reviews_count, 
                    time_to_complete, is_popular, is_processed_with_paddle, is_unsuitable, 
                    is_paid, price, difficulty, last_update_price_date, language, 
                    is_featured, is_public, canonical_url, title, begin_date, end_date, 
                    grading_policy_source, is_active, create_date, update_date, hashKeyDiff
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """, (
                    convert_value(item.id),
                    convert_value(item.load_dttm),
                    convert_value(item.summary),
                    convert_value(item.workload),
                    convert_value(item.cover),
                    convert_value(item.intro),
                    convert_value(item.course_format),
                    convert_value(item.certificate_footer),
                    convert_value(item.certificate_cover_org),
                    convert_value(item.is_certificate_issued),
                    convert_value(item.instructors),
                    convert_value(item.tags),
                    convert_value(item.has_tutors),
                    convert_value(item.is_enabled),
                    convert_value(item.review_summary),
                    convert_value(item.schedule_type),
                    convert_value(item.certificates_count),
                    convert_value(item.learners_count),
                    convert_value(item.lessons_count),
                    convert_value(item.quizzes_count),
                    convert_value(item.challenges_count),
                    convert_value(item.peer_reviews_count),
                    convert_value(item.instructor_reviews_count),
                    convert_value(item.time_to_complete),
                    convert_value(item.is_popular),
                    convert_value(item.is_processed_with_paddle),
                    convert_value(item.is_unsuitable),
                    convert_value(item.is_paid),
                    convert_value(item.price),
                    convert_value(item.difficulty),
                    convert_value(item.last_update_price_date),
                    convert_value(item.language),
                    convert_value(item.is_featured),
                    convert_value(item.is_public),
                    convert_value(item.canonical_url),
                    convert_value(item.title),
                    convert_value(item.begin_date),
                    convert_value(item.end_date),
                    convert_value(item.grading_policy_source),
                    convert_value(item.is_active),
                    convert_value(item.create_date),
                    convert_value(item.update_date),
                    convert_value(item.hashKeyDiff)
                )
            )

        self.connection.commit()

        stepik_sat_course_popular_lessons = pd.read_sql(sql="""
            SELECT t.lesson_id, t.load_dttm
            FROM dwh.silver_layer.sat_stepik_lesson t
        """, con=self.connection)
        stepik_sat_course_popular_lessons['lesson_id'] = stepik_sat_course_popular_lessons['lesson_id'].astype(int)
        
        mask = ~course_popular_lessons['id'].isin(stepik_sat_course_popular_lessons['lesson_id'])
        course_popular_lessons_for_load_sat = course_popular_lessons.loc[mask]

        for item in course_popular_lessons_for_load_sat.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.sat_stepik_lesson (
                    lesson_id, load_dttm, steps, actions, progress, subscriptions, 
                    viewed_by, passed_by, time_to_complete, cover_url, 
                    is_comments_enabled, is_exam_without_progress, is_blank, is_draft, 
                    is_orphaned, courses, units, owner, language, 
                    is_featured, is_public, canonical_url, title, slug, create_date, 
                    update_date, learners_group, testers_group, moderators_group, 
                    assistants_group, teachers_group, admins_group, discussions_count, 
                    discussion_proxy, discussion_threads, epic_count, abuse_count, 
                    vote_delta, vote, lti_consumer_key, lti_secret_key, lti_private_profile
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s
                )
                """, (
                    convert_value(item.id),
                    convert_value(item.load_dttm),
                    convert_value(item.steps),
                    convert_value(item.actions),
                    convert_value(item.progress),
                    convert_value(item.subscriptions),
                    convert_value(item.viewed_by),
                    convert_value(item.passed_by),
                    convert_value(item.time_to_complete),
                    convert_value(item.cover_url),
                    convert_value(item.is_comments_enabled),
                    convert_value(item.is_exam_without_progress),
                    convert_value(item.is_blank),
                    convert_value(item.is_draft),
                    convert_value(item.is_orphaned),
                    convert_value(item.courses),
                    convert_value(item.units),
                    convert_value(item.owner),
                    convert_value(item.language),
                    convert_value(item.is_featured),
                    convert_value(item.is_public),
                    convert_value(item.canonical_url),
                    convert_value(item.title),
                    convert_value(item.slug),
                    convert_value(item.create_date),
                    convert_value(item.update_date),
                    convert_value(item.learners_group),
                    convert_value(item.testers_group),
                    convert_value(item.moderators_group),
                    convert_value(item.assistants_group),
                    convert_value(item.teachers_group),
                    convert_value(item.admins_group),
                    convert_value(item.discussions_count),
                    convert_value(item.discussion_proxy),
                    convert_value(item.discussion_threads),
                    convert_value(item.epic_count),
                    convert_value(item.abuse_count),
                    convert_value(item.vote_delta),
                    convert_value(item.vote),
                    convert_value(item.lti_consumer_key),
                    convert_value(item.lti_secret_key),
                    convert_value(item.lti_private_profile)
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
@dag('silver_load_to_stepik_model', 
     schedule=CronTriggerTimetable('30 7 * * *', timezone='Europe/Moscow'), 
     default_args=DEFAULT_ARGS, max_active_tasks=3, tags=['silver', 'load_stepik_to_dv'], 
     catchup=False)
def taskflow():

    @task
    def get_data_from_stepik_object_storage():
        loader_dv_model = LoaderDVmodelStepik()
        loader_dv_model.load_data_to_dv_model()

    get_data_from_stepik_object_storage = get_data_from_stepik_object_storage()

taskflow = taskflow()