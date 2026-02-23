"""
    Получение информации с образовательной платформы Stepik
"""


import requests 
import hashlib
import pandas as pd 
from datetime import datetime, timedelta
import json
from io import BytesIO
import boto3
from botocore.config import Config

from airflow.decorators import task, dag
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import Variable


S3_YANDEX_OBJECT_STORAGE_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_KEY')
S3_YANDEX_OBJECT_STORAGE_SECRET_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_SECRET_KEY')
S3_YANDEX_OBJECT_STORAGE_ENDPOINT = Variable.get('S3_YANDEX_OBJECT_STORAGE_ENDPOINT')


class ClientStepik: 
    """ Получение данных с сайта Stepik """

    API_HOST = "https://stepik.org"
    CLIENT_ID = Variable.get('CLIENT_ID_STEPIK')
    CLIENT_SECRET = Variable.get('CLIENT_SECRET_STEPIK')

    def get_oauth_stepik(self, client_id: str, client_secret: str): 
        """ Авторизация через приложение на Stepik """
        auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
        response = requests.post('https://stepik.org/oauth2/token/',
                                data={'grant_type': 'client_credentials'},
                                auth=auth)
        token = response.json().get('access_token', None)
        if not token:
            print('Unable to authorize with provided credentials')
            exit(1)
        return token

    def fetch_objects_popular_courses(self, obj_class, obj_ids, token: str) -> json:
        objs = []
        step_size = 30
        for i in range(0, len(obj_ids), step_size):
            obj_ids_slice = obj_ids[i:i + step_size]
            api_url = '{}/api/{}s?{}'.format(self.API_HOST, obj_class,
                                            '&'.join('ids[]={}'.format(obj_id)
                                                    for obj_id in obj_ids_slice))
            response = requests.get(api_url, 
                                    headers={'Authorization': 'Bearer ' + token}
                                    ).json()
            objs += response['{}s'.format(obj_class)]
        return objs

    def get_popular_courses(self, count_courses: int) -> pd.DataFrame: 
        """ Получение популярных курсов """
        token = self.get_oauth_stepik(client_id=self.CLIENT_ID, client_secret=self.CLIENT_SECRET)
        courses = self.fetch_objects_popular_courses(obj_class='course', obj_ids=list(range(count_courses)), token=token)

        resultData = []
        for course in courses: 
            hashKeyDiff = hashlib.md5(
                f"{course.get('id')};{course.get('summary')};{course.get('workload')};{course.get('cover')};"
                f"{course.get('intro')};{course.get('course_format')};{course.get('certificate_footer')};"
                f"{course.get('certificate_cover_org')};{course.get('is_certificate_issued')};{course.get('is_certificate_auto_issued')};"
                f"{course.get('instructors')};{course.get('tags')};{course.get('has_tutors')};{course.get('is_enabled')};{course.get('review_summary')};"
                f"{course.get('schedule_type')};{course.get('certificates_count')};{course.get('learners_count')};{course.get('lessons_count')};"
                f"{course.get('quizzes_count')};{course.get('challenges_count')};{course.get('peer_reviews_count')};{course.get('instructor_reviews_count')};"
                f"{course.get('time_to_complete')};{course.get('is_popular')};{course.get('is_processed_with_paddle')};{course.get('is_unsuitable')};"
                f"{course.get('is_paid')};{course.get('price')};{course.get('difficulty')};{course.get('last_update_price_date')};"
                f"{course.get('language')};{course.get('is_featured')};{course.get('is_public')};{course.get('canonical_url')};{course.get('title')};"
                f"{course.get('begin_date')};{course.get('end_date')};{course.get('grading_policy_source')};{course.get('is_active')};"
                f"{course.get('create_date')};{course.get('update_date')}"
                .encode('utf-8')
            ).hexdigest()
            resultData.append([course.get('id'), course.get('summary'), course.get('workload'), course.get('cover'),
                course.get('intro'), course.get('course_format'), course.get('certificate_footer'),
                course.get('certificate_cover_org'), course.get('is_certificate_issued'), course.get('is_certificate_auto_issued'),
                course.get('instructors'), course.get('tags'), course.get('has_tutors'), course.get('is_enabled'), course.get('review_summary'),
                course.get('schedule_type'), course.get('certificates_count'), course.get('learners_count'), course.get('lessons_count'),
                course.get('quizzes_count'), course.get('challenges_count'), course.get('peer_reviews_count'), course.get('instructor_reviews_count'),
                course.get('time_to_complete'), course.get('is_popular'), course.get('is_processed_with_paddle'), course.get('is_unsuitable'),
                course.get('is_paid'), course.get('price'), course.get('difficulty'), course.get('last_update_price_date'),
                course.get('language'), course.get('is_featured'), course.get('is_public'), course.get('canonical_url'), course.get('title'),
                course.get('begin_date'), course.get('end_date'), course.get('grading_policy_source'), course.get('is_active'), course.get('create_date'),
                course.get('update_date'), hashKeyDiff, datetime.now().strftime("%Y-%m-%d")])
         
        return pd.DataFrame(data=resultData, columns=['id', 'summary', 'workload', 'cover', 'intro', 'course_format',
                                                    'certificate_footer', 'certificate_cover_org', 'is_certificate_issued',
                                                    'is_certificate_auto_issued', 'instructors', 'tags', 'has_tutors',
                                                    'is_enabled', 'review_summary', 'schedule_type', 'certificates_count',
                                                    'learners_count', 'lessons_count', 'quizzes_count', 'challenges_count',
                                                    'peer_reviews_count', 'instructor_reviews_count', 'time_to_complete',
                                                    'is_popular', 'is_processed_with_paddle', 'is_unsuitable', 'is_paid',
                                                    'price', 'difficulty', 'last_update_price_date', 'language', 'is_featured',
                                                    'is_public', 'canonical_url', 'title', 'begin_date', 'end_date',
                                                    'grading_policy_source', 'is_active', 'create_date', 'update_date', 'hashKeyDiff', 'load_dttm'])
    
    def fetch_lesson_by_course(self, token: str, course_id: int) -> json:
        response = requests.get(url=f"https://stepik.org:443/api/lessons?course={course_id}", 
                                headers={'Authorization': 'Bearer ' + token}
                                ).json()
        return response
      
    def get_popular_courses_lessons(self, courses_id: list) -> pd.DataFrame: 
        """ Получение детализированной информации: доступные уроки и информация про них """
        token = self.get_oauth_stepik(client_id=self.CLIENT_ID, 
                                      client_secret=self.CLIENT_SECRET)
        resultData = []
        for course_id in courses_id:
            courses_lessons = self.fetch_lesson_by_course(token=token, course_id=course_id)
            for item in courses_lessons['lessons']:
                hashKeyDiff = hashlib.md5(
                    f"{item.get('id')};{course_id};{item.get('steps')};{item.get('actions')};"
                    f"{item.get('progress')};{item.get('subscriptions')};{item.get('viewed_by')};{item.get('passed_by')};"
                    f"{item.get('time_to_complete')};{item.get('cover_url')};{item.get('is_comments_enabled')};"
                    f"{item.get('is_exam_without_progress')};{item.get('is_blank')};{item.get('is_draft')};{item.get('is_orphaned')};"
                    f"{item.get('courses')};{item.get('units')};{item.get('owner')};{item.get('language')};"
                    f"{item.get('is_featured')};{item.get('is_public')};{item.get('canonical_url')};"
                    f"{item.get('title')};{item.get('slug')};{item.get('create_date')};"
                    f"{item.get('update_date')};{item.get('learners_group')};{item.get('testers_group')};"
                    f"{item.get('moderators_group')};{item.get('assistants_group')};{item.get('teachers_group')};{item.get('admins_group')};"
                    f"{item.get('discussions_count')};{item.get('discussion_proxy')};{item.get('discussion_threads')};{item.get('epic_count')};"
                    f"{item.get('abuse_count')};{item.get('vote_delta')};{item.get('vote')};{item.get('lti_consumer_key')};"
                    f"{item.get('lti_secret_key')};{item.get('lti_private_profile')}"
                    .encode('utf-8')
                ).hexdigest()
                resultData.append([item.get('id'), course_id, hashlib.md5(f"{item.get('id')};{course_id}".encode('utf-8')).hexdigest(), item.get('steps'), 
                                item.get('actions'), item.get('progress'), item.get('subscriptions'), 
                                item.get('viewed_by'), item.get('passed_by'), item.get('time_to_complete'), 
                                item.get('cover_url'), item.get('is_comments_enabled'), item.get('is_exam_without_progress'), 
                                item.get('is_blank'), item.get('is_draft'), item.get('is_orphaned'), item.get('courses'), 
                                item.get('units'), item.get('owner'), item.get('language'), 
                                item.get('is_featured'), item.get('is_public'), item.get('canonical_url'), 
                                item.get('title'), item.get('slug'), item.get('create_date'), 
                                item.get('update_date'), item.get('learners_group'), item.get('testers_group'), 
                                item.get('moderators_group'), item.get('assistants_group'), item.get('teachers_group'), item.get('admins_group'), 
                                item.get('discussions_count'), item.get('discussion_proxy'), item.get('discussion_threads'), item.get('epic_count'), 
                                item.get('abuse_count'), item.get('vote_delta'), item.get('vote'), item.get('lti_consumer_key'), 
                                item.get('lti_secret_key'), item.get('lti_private_profile'), hashKeyDiff, datetime.now().strftime("%Y-%m-%d")])
        return pd.DataFrame(data=resultData, columns=['id', 'course_id', 'linkHashCourseLesson', 'steps', 'actions', 'progress', 'subscriptions', 'viewed_by', 
                                                    'passed_by', 'time_to_complete', 'cover_url', 'is_comments_enabled', 'is_exam_without_progress', 'is_blank', 
                                                    'is_draft', 'is_orphaned', 'courses', 'units', 'owner', 'language', 'is_featured', 'is_public', 'canonical_url', 
                                                    'title', 'slug', 'create_date', 'update_date', 'learners_group', 'testers_group', 'moderators_group', 
                                                    'assistants_group', 'teachers_group', 'admins_group', 'discussions_count', 'discussion_proxy', 
                                                    'discussion_threads', 'epic_count', 'abuse_count', 'vote_delta', 'vote', 'lti_consumer_key', 
                                                    'lti_secret_key', 'lti_private_profile', 'hashKeyDiff', 'load_dttm'])
    

def load_data_to_object_storage(df_report: pd.DataFrame, 
                            file_name: str, 
                            folder_name: str, 
                            save_history: bool):
    """ Загрузка данных в объектное хранилище """

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

    if save_history:
        files_to_upload = [(f"{file_name}_{datetime.now().strftime('%Y-%m')}.parquet", df_report)]
    else: 
        files_to_upload = [(f"{file_name}.parquet", df_report)]      

    for file_name, df in files_to_upload:
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer) 
        parquet_buffer.seek(0) 

        if folder_name in file_name: 
            folder_name = f'bronze_layer/{folder_name}/'

            s3_client.put_object(
                Bucket="education-dwh", 
                Key=folder_name + file_name, 
                Body=parquet_buffer.getvalue(),
                ContentType="application/octet-stream"
            )


DEFAULT_ARGS = {
    'start_date': datetime(2025, 12, 14), 
    'end_date': datetime(2045, 1, 1),
    'owner': 'dmitriy_zhdanov',
    'retries': 4, 
    'retry_delay': timedelta(minutes=5), 
    'poke_interval': 600 
}
@dag('bronze_get_info_from_stepik', 
     schedule=CronTriggerTimetable('30 3 * * *', timezone='Europe/Moscow'), 
     default_args=DEFAULT_ARGS, max_active_tasks=3, tags=['bronze', 'info_from_stepik'], 
     catchup=False)
def taskflow():
    
    @task
    def get_courses_and_lessons_by_course():
        """ Получение популярных курсов и уроков, которые доступны """

        client_stepik = ClientStepik()
        courses_df = client_stepik.get_popular_courses(count_courses=20000)

        load_data_to_object_storage(df_report=courses_df, 
                                    file_name="courses_popular_stepik", 
                                    folder_name="courses_popular_stepik", 
                                    save_history=False)
        print("Популярные курсы получены и загружены в Object Storage")
        
        course_id = courses_df['id'].values.tolist()
        lessons_by_courses_df = client_stepik.get_popular_courses_lessons(courses_id=course_id)

        load_data_to_object_storage(df_report=lessons_by_courses_df, 
                            file_name="lessons_by_courses_stepik", 
                            folder_name="lessons_by_courses_stepik", 
                            save_history=False)
        print("Уроки по популярным курсам получены и загружены в Object Storage")


    get_courses_and_lessons_by_course = get_courses_and_lessons_by_course()

taskflow = taskflow()