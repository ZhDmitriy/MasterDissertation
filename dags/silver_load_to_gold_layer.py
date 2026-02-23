""" 
    Загрузка данных в Gold Layer хранилища данных
"""

import clickhouse_connect
import pandas as pd 
import numpy as np 

from typing import NoReturn
from datetime import datetime, timedelta
import json
import io

from airflow.decorators import task, dag
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook


client = clickhouse_connect.get_client(
    host='host.docker.internal',  
    port=8123,
    username='default',
    password='', 
    database='golden_layer'
)

hook = PostgresHook(
    postgres_conn_id='postgres_default'
)


class GoldenLayerLoader: 
    """ Загрузчик в Golder layer СУБД ClickHouse """

    def __init__(self): 
        pass 

    def loader_to_dm_students_synthetic_results(self) -> NoReturn: 
        sql_query = """ 
            SELECT t.student_guid, t.name_exam, t.subject, t.date_event,
                   t.primary_score, t.mark, t.min_primary_score, t.max_primary_score,
                   t.number_protocol_gak, t.data_protocol_gak
            FROM dwh.silver_layer.students_results t
        """
        columns = ['student_guid', 'name_exam', 'subject', 'date_event', 'primary_score', 'mark', 'min_primary_score', 
                   'max_primary_score', 'number_protocol_gak', 'data_protocol_gak']
        response = hook.get_records(sql=sql_query) 
        result_rows = []
        for item in response: 
            result_rows.append([str(item[0]), str(item[1]), str(item[2]), 
                                str(item[3]), str(item[4]), str(item[5]), 
                                str(item[6]), str(item[7]), str(item[8]), 
                                str(item[9])])
        data_df = pd.DataFrame(data=result_rows, columns=columns)
        data_df['load_dttm'] = datetime.now().strftime("%Y-%m-%d")

        # Удаляем данные за сегодня 
        client.query("""
                        ALTER TABLE dm_students_synthetic_results 
                        DELETE WHERE load_dttm::date = now()::date 
                     """)
        
        # Вставляем новые данные за сегодня
        client.insert_df(table="dm_students_synthetic_results", df=data_df)
        
    def loader_to_dm_students_main_synthetic_info(self) -> NoReturn:
        sql_query = """ 
            SELECT t.student_guid, t.student_fio,
                   t.student_school, t.student_class, 
                   t.student_birthday, t.load_dttm
            FROM dwh.silver_layer.students_main_info t
        """
        columns = ['student_guid', 'student_fio', 'student_school', 'student_class', 'student_birthday']
        response = hook.get_records(sql=sql_query) 
        result_rows = []
        for item in response: 
            result_rows.append([item[0], item[1], item[2], item[3], item[4]])
        data_df = pd.DataFrame(data=result_rows, columns=columns)
        data_df['load_dttm'] = datetime.now().strftime("%Y-%m-%d")

        # Удаляем данные за сегодня 
        client.query("""
                        ALTER TABLE dm_students_main_synthetic_info 
                        DELETE WHERE load_dttm::date = now()::date 
                     """)
        
        # Вставляем новые данные за сегодня
        client.insert_df(table="dm_students_main_synthetic_info", df=data_df)
 
    def loader_to_dm_university_world_info(self) -> NoReturn: 
        sql_query = """ 
            SELECT t.alpha_two_code, t.country_name,
                   t.web_pages, t.state_province, 
                   t.domains, t.load_dttm
            FROM dwh.silver_layer.university_world_info t
        """
        columns = ['alpha_two_code', 'country_name', 'web_pages', 'state_province', 'domains']
        response = hook.get_records(sql=sql_query) 
        result_rows = [] 
        for item in response:
            result_rows.append([item[0], item[1], item[2], item[3], item[4]])
        data_df = pd.DataFrame(data=result_rows, columns=columns)
        data_df['load_dttm'] = datetime.now().strftime("%Y-%m-%d")

        # Удаляем данные за сегодня 
        client.query("""
                        ALTER TABLE dm_university_world_info 
                        DELETE WHERE load_dttm::date = now()::date 
                     """)
        
        # Вставляем новые данные за сегодня
        client.insert_df(table="dm_university_world_info", df=data_df)
     
    def loader_to_dm_subject_shedule(self) -> NoReturn: 
        sql_query = """ 
            SELECT
                t.subject_id,
                m.subject_name,
                t.start_at::TIMESTAMP,
                t.finish_at::TIMESTAMP,
                t.cancelled,
                CASE
                    WHEN t.lesson_type LIKE '0' THEN 'DOP'
                    ELSE t.lesson_type
                    END
                        AS lesson_type,
                CASE
                    WHEN t.replaced LIKE 'True' THEN 'Замена учителя'
                    ELSE 'Без замены учителя'
                    END AS replaced,
                t.room_name,
                t.room_number,
                t.link_to_join,
                t.health_status,
                t.absence_reason_id,
                t.nonattendance_reason_id,
                m.source_name,
                m.source_id,
                t.person_id,
                t.person_name,
                d.student_class,
                d.student_subject_love,
                d.student_advanced_level_name,
                t.load_dttm
            FROM dwh.silver_layer.sat_subject_mash_shedule t
            LEFT JOIN (SELECT t.subject_id, t.source_id, t.source_name, t.subject_name
                    FROM dwh.silver_layer.hub_subject_mash_base t) m on t.subject_id = m.subject_id
            LEFT JOIN (SELECT t.person_id,
                            t.load_dttm,
                            t.student_class,
                            t.student_subject_love,
                            t.student_advanced_level_name
                    FROM dwh.silver_layer.sat_student_mash_base t
                    WHERE t.load_dttm::date = (SELECT max(load_dttm::date) FROM dwh.silver_layer.sat_student_mash_base)) d
                            on t.person_id = d.person_id
            ORDER BY t.start_at desc
        """
        columns = ['subject_id', 'subject_name', 'start_at', 'finish_at', 'cancelled', 'lesson_type', 'replaced', 'room_name', 
                   'room_number', 'link_to_join', 'health_status', 'absence_reason_id', 'nonattendance_reason_id', 'source_name', 
                   'source_id', 'person_id', 'person_name', 'student_class', 'student_subject_love', 'student_advanced_level_name']
        response = hook.get_records(sql=sql_query) 
        result_rows = [] 
        for item in response:
            result_rows.append([str(item[0]), item[1], str(item[2]), str(item[3]), item[4], 
                                item[5], item[6], str(item[7]).replace('nan', ''), str(item[8]).replace('nan', ''), str(item[9]).replace('nan', ''), 
                                str(item[10]).replace('None', ''), str(item[11]).replace('None', ''), str(item[12]).replace('None', ''), item[13], str(item[14]), 
                                item[15], item[16], str(item[17]), item[18], item[19]])
        data_df = pd.DataFrame(data=result_rows, columns=columns)
        data_df['load_dttm'] = datetime.now().strftime("%Y-%m-%d")

        # Удаляем данные за сегодня 
        client.query("""
                        ALTER TABLE dm_subject_shedule 
                        DELETE WHERE load_dttm::date = now()::date 
                     """)
        
        # Вставляем новые данные за сегодня
        client.insert_df(table="dm_subject_shedule", df=data_df)

    def loader_to_dm_subject_mash_score_current(self) -> NoReturn:
        sql_query = """ 
            SELECT t.subject_id, m.subject_name,
                CASE
                    WHEN t.value LIKE 'NaN' THEN null
                    ELSE t.value::Float
                END AS value, t.comment, t.weight, t.point_date,
                t.control_form_name, t.comment_exists, t.created_at::TIMESTAMP, t.updated_at::TIMESTAMP, t.criteria, t.date,
                t.has_files, t.is_point, t.is_exam, t.original_grade_sustem_type, m.source_id,
                m.source_name, d.person_id, t.person_name, d.student_class, d.student_subject_love, d.student_advanced_level_name, t.load_dttm
            FROM dwh.silver_layer.sat_subject_mash_score_current t
            LEFT JOIN (
                SELECT t.subject_id, t.load_dttm, t.source_id, t.source_name, t.subject_name
                FROM dwh.silver_layer.hub_subject_mash_base t
            ) m on m.subject_id = t.subject_id
            LEFT JOIN (SELECT
                    t.person_id,
                    t.load_dttm,
                    t.student_class,
                    t.student_subject_love,
                    t.student_advanced_level_name
            FROM dwh.silver_layer.sat_student_mash_base t
            WHERE t.load_dttm::date = (SELECT max(load_dttm::date) FROM dwh.silver_layer.sat_student_mash_base)) d on t.person_id = d.person_id
        """
        columns = ['subject_id', 'subject_name', 'value', 'comment', 'weight', 'point_date', 'control_form_name', 
                   'comment_exists', 'created_at', 'updated_at', 'criteria', 'date', 'has_files', 'is_point', 
                   'is_exam', 'original_grade_sustem_type', 'source_id', 'source_name', 'person_id', 'person_name', 
                   'student_class', 'student_subject_love', 'student_advanced_level_name']
        response = hook.get_records(sql=sql_query) 
        result_rows = [] 
        for item in response:
            result_rows.append([item[0], item[1], str(item[2]), item[3], str(item[4]), item[5], item[6], item[7], str(item[8]), str(item[9]), 
                                item[10], item[11], item[12], item[13], item[14], str(item[15]), str(item[16]), item[17], 
                                item[18], item[19], str(item[20]), item[21], item[22]])
        data_df = pd.DataFrame(data=result_rows, columns=columns)
        data_df['load_dttm'] = datetime.now().strftime("%Y-%m-%d")

        # Удаляем данные за сегодня 
        client.query("""
                        ALTER TABLE dm_subject_mash_score_current 
                        DELETE WHERE load_dttm::date = now()::date 
                     """)
        
        # Вставляем новые данные за сегодня
        client.insert_df(table="dm_subject_mash_score_current", df=data_df)

    def loader_to_dm_subject_mash_score_history(self) -> NoReturn: 
        sql_query = """ 
            SELECT t.subject_id, m.subject_name, m.source_id, m.source_name, t.periods, t.year_mark,
                   t.intermediate_attestation_mark, t.training_camp_mark,
                   t.period_name, t.begin_date, t.end_date, t.person_id, t.person_name, d.student_class,
                   d.student_subject_love, d.student_advanced_level_name, t.load_dttm
            FROM dwh.silver_layer.sat_subject_mash_score_history t
            LEFT JOIN (
                SELECT t.subject_id, t.load_dttm, t.source_id, t.source_name, t.subject_name
                FROM dwh.silver_layer.hub_subject_mash_base t
            ) m on m.subject_id = t.subject_id
            LEFT JOIN (SELECT
                    t.person_id,
                    t.load_dttm,
                    t.student_class,
                    t.student_subject_love,
                    t.student_advanced_level_name
            FROM dwh.silver_layer.sat_student_mash_base t
            WHERE t.load_dttm::date = (SELECT max(load_dttm::date) FROM dwh.silver_layer.sat_student_mash_base)) d on t.person_id = d.person_id
        """
        columns = ['subject_id', 'subject_name', 'source_id', 'source_name', 'periods', 'year_mark', 
                   'intermediate_attestation_mark', 'training_camp_mark', 'period_name', 'begin_date', 
                   'end_date', 'person_id', 'person_name', 'student_class', 'student_subject_love', 
                   'student_advanced_level_name']
        response = hook.get_records(sql=sql_query) 
        result_rows = [] 
        for item in response: 
            result_rows.append([item[0], item[1], str(item[2]), item[3], str(item[4]), str(item[5]), 
                                str(item[6]).replace('None', ''), item[7].replace('None', ''), item[8], str(item[9]), str(item[10]), item[11], 
                                item[12], item[13], item[14], item[15]])
        data_df = pd.DataFrame(data=result_rows, columns=columns)
        data_df['load_dttm'] = datetime.now().strftime("%Y-%m-%d")

        # Удаляем данные за сегодня 
        client.query("""
                        ALTER TABLE dm_subject_mash_score_history 
                        DELETE WHERE load_dttm::date = now()::date 
                     """)
        
        # Вставляем новые данные за сегодня
        client.insert_df(table="dm_subject_mash_score_history", df=data_df)
        
    def loader_to_dm_subject_mash_omissions_history(self) -> NoReturn: 
        sql_query = """
            SELECT t.subject_id, t.date, t.lesson_id::Float,
                t.begin_time, t.end_time, t.absence_reason,
                t.lesson_name, t.time_range, t.is_virtual, t.person_id,
                t.person_name, m.source_id, m.source_name, m.subject_name,
                d.student_class, d.student_subject_love, d.student_advanced_level_name, 
                t.load_dttm
            FROM dwh.silver_layer.sat_subject_mash_omissions_history t
            LEFT JOIN (
                SELECT t.subject_id, t.load_dttm, t.source_id, t.source_name, t.subject_name
                FROM dwh.silver_layer.hub_subject_mash_base t
            ) m on m.subject_id = t.subject_id
            LEFT JOIN (SELECT
                    t.person_id,
                    t.load_dttm,
                    t.student_class,
                    t.student_subject_love,
                    t.student_advanced_level_name
            FROM dwh.silver_layer.sat_student_mash_base t
            WHERE t.load_dttm::date = (SELECT max(load_dttm::date) FROM dwh.silver_layer.sat_student_mash_base)) d on t.person_id = d.person_id
        """
        columns = ['subject_id', 'date', 'lesson_id', 'begin_time', 'end_time', 'absence_reason', 'lesson_name', 'time_range', 
                   'is_virtual', 'person_id', 'person_name', 'source_id', 'source_name', 'subject_name', 'student_class', 'student_subject_love', 
                   'student_advanced_level_name']
        response = hook.get_records(sql=sql_query) 
        result_rows = [] 
        for item in response:
            result_rows.append([item[0], str(item[1]), str(item[2]).replace('nan', ''), str(item[3]), str(item[4]), str(item[5]).replace('NaN', ''), 
                                str(item[6]).replace('None', ''), str(item[7]), str(item[8]), str(item[9]), str(item[10]), str(item[11]), 
                                str(item[12]), str(item[13]), str(item[14]), str(item[15]), str(item[16])]) 
        data_df = pd.DataFrame(data=result_rows, columns=columns)
        data_df['load_dttm'] = datetime.now().strftime("%Y-%m-%d")

        # Удаляем данные за сегодня 
        client.query("""
                        ALTER TABLE dm_subject_mash_omissions_history 
                        DELETE WHERE load_dttm::date = now()::date 
                     """)
        
        # Вставляем новые данные за сегодня
        client.insert_df(table="dm_subject_mash_omissions_history", df=data_df)

    def loader_to_dm_stepik_lesson_by_course(self) -> NoReturn: 
        sql_query = """ 
            SELECT t.lesson_id, t.steps, t.actions, t.progress, t.subscriptions, t.viewed_by, t.passed_by,
                t.time_to_complete, t.cover_url, t.is_comments_enabled, t.is_exam_without_progress, t.is_blank,
                t.is_draft, t.is_orphaned, t.courses, t.units, t.owner, t.language, t.is_featured, t.is_public, t.canonical_url,
                t.title, t.slug, t.create_date, t.update_date, t.learners_group, t.testers_group, t.moderators_group,
                t.assistants_group, t.admins_group, t.discussions_count, t.discussion_proxy, t.discussion_threads,
                t.epic_count, t.epic_count, t.abuse_count, t.vote_delta, t.vote, t.lti_consumer_key, t.lti_secret_key, t.lti_private_profile, 
                m.course_id, m.source_id, m.source_name, d.title as course_title
            FROM dwh.silver_layer.sat_stepik_lesson t
            LEFT JOIN (
                SELECT m.lesson_id, t.course_id, t.load_dttm, t.source_id, t.source_name
                FROM dwh.silver_layer.hub_stepik_course t
                LEFT JOIN dwh.silver_layer.link_stepik_course_lesson m on m.course_id = t.course_id
            ) m on t.lesson_id = m.lesson_id
            LEFT JOIN dwh.silver_layer.sat_stepik_courses d ON d.course_id = m.course_id
        """
        columns = ['lesson_id', 'steps', 'actions', 'progress', 'subscriptions', 'viewed_by', 'passed_by', 'time_to_complete', 
                   'cover_url', 'is_comments_enabled', 'is_exam_without_progress', 'is_blank', 'is_draft', 'is_orphaned', 
                   'courses', 'units', 'owner', 'language', 'is_featured', 'is_public', 'canonical_url', 'title', 'slug', 'create_date', 
                   'update_date', 'learners_group', 'testers_group', 'moderators_group', 'assistants_group', 'admins_group', 'discussions_count', 
                   'discussion_proxy', 'discussion_threads', 'epic_count', 'abuse_count', 'vote_delta', 'vote', 'lti_consumer_key', 'lti_secret_key', 
                   'lti_private_profile', 'course_id', 'source_id', 'source_name', 'course_title']
        response = hook.get_records(sql=sql_query) 
        result_rows = [] 
        for item in response:
            result_rows.append([str(item[i]).replace('None', '') for i in range(0, 44, 1)])
        data_df = pd.DataFrame(data=result_rows, columns=columns)
        data_df['load_dttm'] = datetime.now().strftime("%Y-%m-%d")

        # Удаляем данные за сегодня 
        client.query("""
                        ALTER TABLE dm_stepik_lesson_by_course 
                        DELETE WHERE load_dttm::date = now()::date 
                     """)
        
        # Вставляем новые данные за сегодня
        client.insert_df(table="dm_stepik_lesson_by_course", df=data_df)
       
    def loader_to_dm_stepik_courses(self) -> NoReturn: 
        sql_query = """ 
            SELECT t.course_id, t.summary, t.workload, t.cover, t.intro, t.course_format, t.certificate_footer, t.certificate_cover_org,
                t.is_certificate_issued, t.instructors, t.tags, t.has_tutors, t.is_enabled, t.review_summary, t.schedule_type,
                t.certificates_count, t.learners_count, t.lessons_count, t.quizzes_count, t.challenges_count, t.peer_reviews_count,
                t.instructor_reviews_count, t.time_to_complete, t.is_popular, t.is_processed_with_paddle, t.is_unsuitable, t.is_paid,
                t.price, t.difficulty, t.last_update_price_date, t.language, t.is_featured, t.is_public, t.canonical_url, t.title,
                t.begin_date, t.end_date, t.grading_policy_source, t.is_active, t.create_date, t.update_date, m.source_id, m.source_name,
                t.load_dttm
            FROM dwh.silver_layer.sat_stepik_courses t
            LEFT JOIN (
                SELECT t.course_id, t.load_dttm, t.source_id, t.source_name
                FROM dwh.silver_layer.hub_stepik_course t
            ) m on t.course_id = m.course_id
        """
        columns = ['course_id', 'summary', 'workload', 'cover', 'intro', 'course_format', 'certificate_footer', 'certificate_cover_org', 
                   'is_certificate_issued', 'instructors', 'tags', 'has_tutors', 'is_enabled', 'review_summary', 'schedule_type', 
                   'certificates_count', 'learners_count', 'lessons_count', 'quizzes_count', 'challenges_count', 'peer_reviews_count', 
                   'instructor_reviews_count', 'time_to_complete', 'is_popular', 'is_processed_with_paddle', 'is_unsuitable', 'is_paid', 
                   'price', 'difficulty', 'last_update_price_date', 'language', 'is_featured', 'is_public', 'canonical_url', 'title', 
                   'begin_date', 'end_date', 'grading_policy_source', 'is_active', 'create_date', 'update_date', 'source_id', 'source_name']
        response = hook.get_records(sql=sql_query) 
        result_rows = [] 
        for item in response:
            result_rows.append([str(item[i]).replace('None', '') for i in range(0, 43, 1)])
        data_df = pd.DataFrame(data=result_rows, columns=columns)
        data_df['load_dttm'] = datetime.now().strftime("%Y-%m-%d")

        # Удаляем данные за сегодня 
        client.query("""
                        ALTER TABLE dm_stepik_courses 
                        DELETE WHERE load_dttm::date = now()::date 
                     """)
        
        # Вставляем новые данные за сегодня
        client.insert_df(table="dm_stepik_courses", df=data_df)


golder_layer_loader = GoldenLayerLoader()


DEFAULT_ARGS = {
    'start_date': datetime(2026, 2, 14), 
    'end_date': datetime(2045, 1, 1),
    'owner': 'dmitriy_zhdanov',
    'retries': 4, 
    'retry_delay': timedelta(minutes=5), 
    'poke_interval': 600 
}
@dag('silver_load_to_gold_layer', 
     schedule=CronTriggerTimetable('30 8 * * *', timezone='Europe/Moscow'), 
     default_args=DEFAULT_ARGS, max_active_tasks=3, tags=['gold', 'load_to_gold_layer'], 
     catchup=False)
def taskflow():

    @task
    def load_to_dm_students_synthetic_results():
        """ Загрузка данных в витрину dm_students_synthetic_results """
        golder_layer_loader.loader_to_dm_students_synthetic_results()

    @task 
    def load_to_dm_students_main_synthetic_info():
        """ Загрузка данных в витрину dm_students_main_synthetic_info """
        golder_layer_loader.loader_to_dm_students_main_synthetic_info()

    @task 
    def load_to_dm_university_world_info():
        """ Загрузка данных в витрину dm_university_world_info """
        golder_layer_loader.loader_to_dm_university_world_info()

    @task 
    def load_to_dm_subject_shedule(): 
        """ Загрузка данных в витрину dm_subject_shedule """
        golder_layer_loader.loader_to_dm_subject_shedule()

    @task 
    def load_to_dm_subject_mash_score_current(): 
        """ Загрузка данных в витрину dm_subject_mash_score_current """
        golder_layer_loader.loader_to_dm_subject_mash_score_current()

    @task 
    def load_to_dm_subject_mash_score_history():
        """ Загрузка данных в витрину dm_subject_mash_score_history """
        golder_layer_loader.loader_to_dm_subject_mash_score_history()

    @task 
    def load_to_dm_subject_mash_omissions_history(): 
        """ Загрузка данных в витрину dm_subject_mash_omissions_history """
        golder_layer_loader.loader_to_dm_subject_mash_omissions_history()

    @task 
    def load_to_dm_stepik_lesson_by_course(): 
        """ Загрузка данных в витрину dm_stepik_lesson_by_course """
        golder_layer_loader.loader_to_dm_stepik_lesson_by_course()

    @task 
    def load_to_dm_stepik_courses(): 
        """ Загрузка данных в витрину dm_stepik_courses """
        golder_layer_loader.loader_to_dm_stepik_courses()

    load_to_dm_students_synthetic_results = load_to_dm_students_synthetic_results()
    load_to_dm_students_main_synthetic_info = load_to_dm_students_main_synthetic_info()
    load_to_dm_university_world_info = load_to_dm_university_world_info()
    load_to_dm_subject_shedule = load_to_dm_subject_shedule()
    load_to_dm_subject_mash_score_current = load_to_dm_subject_mash_score_current()
    load_to_dm_subject_mash_score_history = load_to_dm_subject_mash_score_history()
    load_to_dm_subject_mash_omissions_history = load_to_dm_subject_mash_omissions_history()
    load_to_dm_stepik_lesson_by_course = load_to_dm_stepik_lesson_by_course()
    load_to_dm_stepik_courses = load_to_dm_stepik_courses()

    load_to_dm_students_synthetic_results >> load_to_dm_students_main_synthetic_info >> load_to_dm_university_world_info >> \
    load_to_dm_subject_shedule >> load_to_dm_subject_mash_score_current >> load_to_dm_subject_mash_score_history >> load_to_dm_subject_mash_omissions_history >> \
    load_to_dm_stepik_lesson_by_course >> load_to_dm_stepik_courses

taskflow = taskflow()