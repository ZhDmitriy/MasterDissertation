"""
    Перегрузка данных из Bronze слоя в Silver слой для источника данных МЭШ
"""

import psycopg2
import pandas as pd 
import hashlib
import numpy as np
from typing import NoReturn
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


class LoaderDVmodelMash: 
    """ Загрузчик данных в модель данных Data Vault для источника данных МЭШ """

    source_id = 2
    source_name = 'mash'
    connection = psycopg2.connect(
        host=Variable.get('POSTGRES_HOST'),
        user=Variable.get('POSTGRES_USER'),
        password=Variable.get('POSTGRES_PASSWORD'),
        database=Variable.get('POSTGRES_DATABASE'),
    )

    def __init__(self, date_report: datetime): 
        """ date_report - текущее время """
        self.mash_urls = {
            "students_current_score": f"bronze_layer/students_current_score/students_current_score_{date_report.strftime("%Y-%m")}.parquet", 
            "students_history_of_passes": f"bronze_layer/students_history_of_passes/students_history_of_passes_{date_report.strftime("%Y-%m")}.parquet", 
            "students_homework_and_schedule": f"bronze_layer/students_homework_and_schedule/students_homework_and_schedule_{date_report.strftime("%Y-%m")}.parquet", 
            "students_independent_diagnostics": "bronze_layer/students_independent_diagnostics/students_independent_diagnostics.parquet", 
            "students_main_base": f"bronze_layer/students_main_base/students_main_base_{date_report.strftime("%Y-%m")}.parquet", 
            "students_participation_olympic": "bronze_layer/students_participation_olympic/students_participation_olympic.parquet", 
            "students_result_exam_score": "bronze_layer/students_result_exam_score/students_result_exam_score.parquet", 
            "students_result_score": f"bronze_layer/students_result_score/students_result_score.parquet"
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
    
    def make_link_hash_md5(self, row): 

        hashKeyDiff = hashlib.md5(
            f"{row['subject_id']};{row['person_id']}"
            .encode('utf-8')
        ).hexdigest()

        return hashKeyDiff

    def load_hub_and_link_mash(self) -> NoReturn:
        """ Прогрузка хабов и линка """ 

        cursor = self.connection.cursor()

        # Текущие предметы
        students_current_score = self.get_data_from_object_storage(key=self.mash_urls['students_current_score'])
        students_current_score['subject_id'] = students_current_score['subject_id'].astype(str)
        students_current_score = students_current_score.rename(columns={'subject': 'subject_name'})

        # Исторические предметы
        students_result_score = self.get_data_from_object_storage(key=self.mash_urls['students_result_score'])
        students_result_score['subject_id'] = students_result_score['subject_id'].astype(str)

        # Расписание
        students_homework_and_schedule = self.get_data_from_object_storage(key=self.mash_urls['students_homework_and_schedule'])
        students_homework_and_schedule['subject_id'] = students_homework_and_schedule['subject_id'].astype(str)

        subject_id_and_name_all = pd.concat([students_current_score[['subject_id', 'subject_name', 'load_dttm']], students_result_score[['subject_id', 'subject_name', 'load_dttm']], students_homework_and_schedule[['subject_id', 'subject_name', 'load_dttm']]])
        subject_id_and_name_all = subject_id_and_name_all.drop_duplicates(subset=['subject_id', 'subject_name', 'load_dttm']).reset_index(drop=True)

        subject_id_and_name_all = subject_id_and_name_all[subject_id_and_name_all['subject_id'] != '0']

        # Прогрузка первого хаба
        hub_subject_mash_base = pd.read_sql(sql="""
             SELECT t.subject_id, t.load_dttm, t.source_id, t.source_name
             FROM silver_layer.hub_subject_mash_base t 
         """, con=self.connection)
        hub_subject_mash_base['subject_id'] = hub_subject_mash_base['subject_id'].astype(str)

        mask = ~subject_id_and_name_all['subject_id'].isin(hub_subject_mash_base['subject_id'])
        subject_id_and_name_all_load_hub = subject_id_and_name_all.loc[mask, ['subject_id', 'subject_name', 'load_dttm']]   

        for item in subject_id_and_name_all_load_hub.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.hub_subject_mash_base (subject_id, load_dttm, subject_name, source_id, source_name)
                VALUES (%s, %s, %s, %s, %s)
                """, (
                    str(item.subject_id),
                    item.load_dttm,
                    item.subject_name,
                    self.source_id,
                    self.source_name
                    )
                )
            
        self.connection.commit()

        # Получение информации о студенте
        students_main_info = self.get_data_from_object_storage(key=self.mash_urls['students_main_base'])
        students_main_info['person_id'] = students_main_info['person_id'].astype(str)

        student_mash = pd.read_sql(sql="""
            SELECT t.person_id, t.student_id, t.student_name, t.load_dttm, t.source_id, t.source_name
            FROM silver_layer.hub_student_mash t 
        """, con=self.connection)
        student_mash['person_id'] = student_mash['person_id'].astype(str)

        mask = ~students_main_info['person_id'].isin(student_mash['person_id'])
        students_main_info_load_hub = students_main_info.loc[mask, ['person_id', 'profile_id', 'person_name', 'load_dttm']]   

        # Прогрузка второго хаба
        for item in students_main_info_load_hub.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.hub_student_mash (person_id, student_id, student_name, load_dttm, source_id, source_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    str(item.person_id),
                    str(item.profile_id),
                    item.person_name,
                    item.load_dttm,
                    self.source_id,
                    self.source_name
                    )
                )
            
        self.connection.commit()

        # Прогрузка линков 
        subject_id_and_name_all_link = subject_id_and_name_all[['subject_id', 'subject_name', 'load_dttm']]
        students_main_info_link = students_main_info[['person_id', 'person_name']]
        subject_person_df = pd.merge(subject_id_and_name_all_link, students_main_info_link, how='cross')[['subject_id', 'person_id', 'load_dttm']]
        subject_person_df['link_student_hash_key'] = subject_person_df.apply(self.make_link_hash_md5, axis=1)

        student_subject_link = pd.read_sql(sql="""
            SELECT t.link_student_hash_key, t.person_id, t.subject_id, t.source_id, t.load_dttm
            FROM silver_layer.link_student_score_mash t
        """, con=self.connection)
        student_subject_link['link_student_hash_key'] = student_subject_link['link_student_hash_key'].astype(str)

        mask = ~subject_person_df['link_student_hash_key'].isin(student_subject_link['link_student_hash_key'])
        students_main_info_load_hub = subject_person_df.loc[mask, ['link_student_hash_key', 'subject_id', 'person_id', 'load_dttm']]   

        for item in students_main_info_load_hub.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.link_student_score_mash (link_student_hash_key, person_id, subject_id, source_id, load_dttm)
                VALUES (%s, %s, %s, %s, %s)
                """, (
                    str(item.link_student_hash_key),
                    str(item.person_id),
                    item.subject_id,
                    self.source_id,
                    item.load_dttm
                )
            )
            
        self.connection.commit()

    def load_satellit_mash_student_mash(self) -> NoReturn:
        """ Прогрузка сателлитов для первого хаба """

        cursor = self.connection.cursor()

        # Прогрузка сателлитов для hub_student_mash
        students_main_info = self.get_data_from_object_storage(key=self.mash_urls['students_main_base'])
        students_main_info['person_id'] = students_main_info['person_id'].astype(str)
        students_main_info['hashKeyDiff'] = students_main_info['hashKeyDiff'].astype(str)
        
        # Загрузка саттелита sat_student_mash_base
        mash_sat_student_mash_base = pd.read_sql(sql="""
            SELECT t.person_id, t.load_dttm, t.student_class, t.student_subject_love, t.student_advanced_level_name, t.hashkeydiff
            FROM dwh.silver_layer.sat_student_mash_base t
        """, con=self.connection)
        mash_sat_student_mash_base['person_id'] = mash_sat_student_mash_base['person_id'].astype(str)
        mash_sat_student_mash_base['hashkeydiff'] = mash_sat_student_mash_base['hashkeydiff'].astype(str)

        mask = (~students_main_info['person_id'].isin(mash_sat_student_mash_base['person_id'])) | (~students_main_info['hashKeyDiff'].isin(mash_sat_student_mash_base['hashkeydiff']))
        students_main_info_for_load_sat = students_main_info.loc[mask]

        for item in students_main_info_for_load_sat.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.sat_student_mash_base (person_id, student_class, student_subject_love, student_advanced_level_name, hashKeyDiff, load_dttm)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    str(item.person_id),
                    str(item.class_student),
                    str(item.subject_love_student),
                    str(item.student_advanced_level_name),
                    str(item.hashKeyDiff), 
                    item.load_dttm
                )
            )
            
        self.connection.commit()

        # Загрузка саттелита sat_subject_independent_diagnostics
        students_independent_diagnostics = self.get_data_from_object_storage(key=self.mash_urls['students_independent_diagnostics'])
        students_independent_diagnostics['person_id'] = students_independent_diagnostics['person_id'].astype(str)
        students_independent_diagnostics['hashKeyDiff'] = students_independent_diagnostics['hashKeyDiff'].astype(str)

        mash_sat_subject_independent_diagnostics = pd.read_sql(sql="""
            SELECT t.person_id, t.load_dttm, t.learningyear, t.recordid,
                   t.workid, t.eventdate, t.subject, t.schoolid, t.diagnostic_name,
                   t.maxresult, t.resultvalue, t.percentresult, t.leveltype,
                   t.diagnostic_note, t.isvisible, t.markvalue5, t.hashkeydiff
            FROM dwh.silver_layer.sat_subject_independent_diagnostics t
        """, con=self.connection)
        mash_sat_subject_independent_diagnostics['person_id'] = mash_sat_subject_independent_diagnostics['person_id'].astype(str)
        mash_sat_subject_independent_diagnostics['hashkeydiff'] = mash_sat_subject_independent_diagnostics['hashkeydiff'].astype(str)

        mask = (~students_independent_diagnostics['person_id'].isin(mash_sat_subject_independent_diagnostics['person_id'])) | (~students_independent_diagnostics['hashKeyDiff'].isin(mash_sat_subject_independent_diagnostics['hashkeydiff']))
        students_independent_diagnostics_for_load_sat = students_independent_diagnostics.loc[mask]

        for item in students_independent_diagnostics_for_load_sat.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.sat_subject_independent_diagnostics (person_id, load_dttm, learningyear, recordid, workid, eventdate, 
                                                                              subject, schoolid, diagnostic_name, maxresult, resultvalue, percentresult, 
                                                                              leveltype, diagnostic_note, isvisible, markvalue5, hashkeydiff)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(item.person_id),
                    item.load_dttm,
                    str(item.learningYear),
                    str(item.recordId),
                    str(item.workId), 
                    str(item.eventDate), 
                    str(item.subject), 
                    str(item.schoolId),
                    str(item.diagnostic_name),
                    str(item.maxResult),
                    str(item.resultValue),
                    str(item.percentResult),
                    str(item.levelType),
                    str(item.diagnostic_note),
                    str(item.isVisible),
                    str(item.markValue5),
                    str(item.hashKeyDiff)
                )
            )
            
        self.connection.commit()

    def load_satellit_mash_subject_mash_base(self): 
        """ Прогрузка сателлитов для второго хаба """

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
        
        cursor = self.connection.cursor()

        # Прогрузка сателлитов для hub_student_mash
        students_result_score = self.get_data_from_object_storage(key=self.mash_urls['students_result_score'])
        students_result_score['subject_id'] = students_result_score['subject_id'].astype(str)
        students_result_score['hashKeyDiff'] = students_result_score['hashKeyDiff'].astype(str)

        # Прогрузка сателлита для sat_subject_mash_score_history
        mash_sat_subject_mash_score_history = pd.read_sql(sql="""
            SELECT t.subject_id, t.load_dttm, t.periods, t.year_mark, 
                   t.intermediate_attestation_mark, t.training_camp_mark, t.hashkeydiff
            FROM dwh.silver_layer.sat_subject_mash_score_history t
        """, con=self.connection)
        mash_sat_subject_mash_score_history['subject_id'] = mash_sat_subject_mash_score_history['subject_id'].astype(str)
        mash_sat_subject_mash_score_history['hashkeydiff'] = mash_sat_subject_mash_score_history['hashkeydiff'].astype(str)

        mask = (~students_result_score['subject_id'].isin(mash_sat_subject_mash_score_history['subject_id'])) | (~students_result_score['hashKeyDiff'].isin(mash_sat_subject_mash_score_history['hashkeydiff']))
        students_result_score_for_load_sat = students_result_score.loc[mask]

        for item in students_result_score_for_load_sat.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.sat_subject_mash_score_history (subject_id, person_id, person_name, load_dttm, periods, year_mark, 
                                                                         intermediate_attestation_mark, training_camp_mark, 
                                                                         period_name, begin_date, end_date, calendar_id,
                                                                         hashkeydiff)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(item.subject_id),
                    str(item.person_id),
                    str(item.student_name),
                    item.load_dttm,
                    str(item.periods),
                    str(item.year_mark),
                    str(item.intermediate_attestation_mark), 
                    str(item.training_camp_mark), 
                    str(item.period_name), 
                    str(item.begin_date), 
                    str(item.end_date), 
                    str(item.calendar_id),
                    str(item.hashKeyDiff)
                )
            )

        self.connection.commit()

        # Прогрузка сателлита для sat_subject_homework
        students_homework_and_schedule = self.get_data_from_object_storage(key=self.mash_urls['students_homework_and_schedule'])
        students_homework_and_schedule['subject_id'] = students_homework_and_schedule['subject_id'].astype(str)
        students_homework_and_schedule['hashKeyDiffHomework'] = students_homework_and_schedule['hashKeyDiffHomework'].astype(str)
        students_homework_and_schedule['hashKeyDiffShedule'] = students_homework_and_schedule['hashKeyDiffShedule'].astype(str)

        students_homework_and_schedule = students_homework_and_schedule[students_homework_and_schedule['subject_id'] != '0']

        # Прогрузка сателлита для sat_subject_homework
        mash_sat_subject_homework = pd.read_sql(sql="""
            SELECT t.subject_id, t.load_dttm, t.start_at, t.finish_at, t.homework_count,
                t.homework_execute_count, t.homework_descriptions, t.homework_marks,
                t.homework_is_missed_lesson, t.hashkeydiff, t.homework_materials
            FROM dwh.silver_layer.sat_subject_homework t
        """, con=self.connection)
        mash_sat_subject_homework['subject_id'] = mash_sat_subject_homework['subject_id'].astype(str)
        mash_sat_subject_homework['hashkeydiff'] = mash_sat_subject_homework['hashkeydiff'].astype(str)

        mask = (~students_homework_and_schedule['subject_id'].isin(mash_sat_subject_homework['subject_id'])) | (~students_homework_and_schedule['hashKeyDiffHomework'].isin(mash_sat_subject_homework['hashkeydiff']))
        students_homework_for_load_sat = students_homework_and_schedule.loc[mask]

        for item in students_homework_for_load_sat.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.sat_subject_homework (subject_id, person_id, person_name, start_at, finish_at, homework_count, 
                                                               homework_execute_count, homework_descriptions, 
                                                               homework_materials, homework_marks, homework_is_missed_lesson,
                                                               hashkeydiff, load_dttm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    item.subject_id,
                    item.person_id, 
                    item.student_name,
                    item.start_at,
                    item.finish_at,
                    item.homework_count,
                    item.execute_count, 
                    item.descriptions, 
                    item.materials, 
                    item.marks, 
                    item.is_missed_lesson, 
                    item.hashKeyDiffHomework,
                    item.load_dttm
                )
            )

        self.connection.commit()

        # Прогрузка сателлита для sat_subject_mash_shedule
        mash_sat_subject_mash_shedule = pd.read_sql(sql="""
            SELECT t.subject_id, t.start_at, t.finish_at, t.cancelled, t.lesson_type,
                   t.replaced, t.room_name, t.room_number, t.link_to_join, t.health_status,
                   t.absence_reason_id, t.nonattendance_reason_id, t.hashkeydiff, t.load_dttm
            FROM dwh.silver_layer.sat_subject_mash_shedule t
        """, con=self.connection)
        mash_sat_subject_mash_shedule['subject_id'] = mash_sat_subject_mash_shedule['subject_id'].astype(str)
        mash_sat_subject_mash_shedule['hashkeydiff'] = mash_sat_subject_mash_shedule['hashkeydiff'].astype(str)

        mask = (~students_homework_and_schedule['subject_id'].isin(mash_sat_subject_mash_shedule['subject_id'])) | (~students_homework_and_schedule['hashKeyDiffShedule'].isin(mash_sat_subject_mash_shedule['hashkeydiff']))
        students_schedule_for_load_sat = students_homework_and_schedule.loc[mask]

        for item in students_schedule_for_load_sat.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.sat_subject_mash_shedule (subject_id, person_id, person_name, start_at, finish_at, cancelled, 
                                                                   lesson_type, replaced, room_name, room_number, 
                                                                   link_to_join, health_status, 
                                                                   absence_reason_id, nonattendance_reason_id, 
                                                                   hashkeydiff, load_dttm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    item.subject_id,
                    item.person_id,
                    item.student_name,
                    item.start_at,
                    item.finish_at,
                    item.cancelled,
                    item.lesson_type, 
                    item.replaced, 
                    item.room_name, 
                    item.room_number, 
                    item.link_to_join, 
                    item.health_status, 
                    item.absence_reason_id,
                    item.nonattendance_reason_id,
                    item.hashKeyDiffShedule,
                    item.load_dttm
                )
            )

        self.connection.commit()

        # Прогрузка сателлита для sat_subject_mash_score_current
        students_current_score = self.get_data_from_object_storage(key=self.mash_urls['students_current_score'])
        students_current_score['subject_id'] = students_current_score['subject_id'].astype(str)
        students_current_score['hashKeyDiff'] = students_current_score['hashKeyDiff'].astype(str)

        mash_sat_subject_mash_score_current = pd.read_sql(sql="""
            SELECT t.subject_id, t.load_dttm, t.value, t.comment,
                t.weight, t.point_date, t.control_form_name, t.comment_exists,
                t.created_at, t.updated_at, t.criteria, t.date, t.has_files,
                t.is_point, t.is_exam, t.hashkeydiff, t.original_grade_sustem_type
            FROM silver_layer.sat_subject_mash_score_current t
        """, con=self.connection)
        mash_sat_subject_mash_score_current['subject_id'] = mash_sat_subject_mash_score_current['subject_id'].astype(str)
        mash_sat_subject_mash_score_current['hashkeydiff'] = mash_sat_subject_mash_score_current['hashkeydiff'].astype(str)

        mask = (~students_current_score['subject_id'].isin(mash_sat_subject_mash_score_current['subject_id'])) | (~students_current_score['hashKeyDiff'].isin(mash_sat_subject_mash_score_current['hashkeydiff']))
        students_current_score_for_load_sat = students_current_score.loc[mask]

        for item in students_current_score_for_load_sat.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.sat_subject_mash_score_current (subject_id, person_id, person_name, value, comment, 
                                                                   weight, point_date, control_form_name, comment_exists, 
                                                                   created_at, updated_at, 
                                                                   criteria, date, has_files, is_point, is_exam, original_grade_sustem_type,
                                                                   hashkeydiff, load_dttm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    item.subject_id,
                    convert_value(item.person_id),
                    convert_value(item.student_name),
                    convert_value(item.value),
                    convert_value(item.comment),
                    convert_value(item.weight),
                    convert_value(item.point_date),
                    convert_value(item.control_form_name),
                    convert_value(item.comment_exists),
                    convert_value(item.created_at),
                    convert_value(item.updated_at),
                    convert_value(item.criteria),
                    convert_value(item.date),
                    convert_value(item.has_files),
                    convert_value(item.is_point),
                    convert_value(item.is_exam),
                    convert_value(item.original_grade_system_type),
                    item.hashKeyDiff,
                    item.load_dttm
                )
            )

        self.connection.commit()

        # Прогрузка сателлита для sat_subject_mash_omissions_history
        students_history_of_passes = self.get_data_from_object_storage(key=self.mash_urls['students_history_of_passes'])
        students_history_of_passes['subject_id'] = students_history_of_passes['subject_id'].astype(str)
        students_history_of_passes['hashKeyDiff'] = students_history_of_passes['hashKeyDiff'].astype(str)

        mash_sat_subject_mash_omissions_history = pd.read_sql(sql="""
            SELECT t.subject_id, t.date, t.lesson_id, t.begin_time, t.end_time, t.absence_reason, 
                t.lesson_name, t.time_range, t.is_virtual, t.hashkeydiff, t.load_dttm
            FROM silver_layer.sat_subject_mash_omissions_history t
        """, con=self.connection)
        mash_sat_subject_mash_omissions_history['subject_id'] = mash_sat_subject_mash_omissions_history['subject_id'].astype(str)
        mash_sat_subject_mash_omissions_history['hashkeydiff'] = mash_sat_subject_mash_omissions_history['hashkeydiff'].astype(str)

        mask = (~students_history_of_passes['subject_id'].isin(mash_sat_subject_mash_omissions_history['subject_id'])) | (~students_history_of_passes['hashKeyDiff'].isin(mash_sat_subject_mash_omissions_history['hashkeydiff']))
        students_history_of_passes_for_load_sat = students_history_of_passes.loc[mask]

        students_history_of_passes_for_load_sat = students_history_of_passes_for_load_sat[students_history_of_passes_for_load_sat['subject_id'] != 'None']

        for item in students_history_of_passes_for_load_sat.itertuples():
            cursor.execute("""
                INSERT INTO silver_layer.sat_subject_mash_omissions_history (subject_id, date, person_id, person_name, lesson_id, begin_time, 
                                                                             end_time, absence_reason, lesson_name, time_range, 
                                                                             is_virtual, hashkeydiff, load_dttm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    convert_value(item.subject_id),
                    convert_value(item.date),
                    convert_value(item.person_id),
                    convert_value(item.student_name),
                    convert_value(item.lesson_id),
                    convert_value(item.begin_time),
                    convert_value(item.end_time),
                    convert_value(item.absence_reason),
                    convert_value(item.lesson_name),
                    convert_value(item.time_range),
                    convert_value(item.is_virtual),
                    convert_value(item.hashKeyDiff),
                    convert_value(item.load_dttm)
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
@dag('silver_load_to_mash_model', 
     schedule=CronTriggerTimetable('30 8 * * *', timezone='Europe/Moscow'), 
     default_args=DEFAULT_ARGS, max_active_tasks=3, tags=['silver', 'load_mash_to_dv'], 
     catchup=False)
def taskflow():

    @task
    def get_data_from_mash_object_storage():
        loader_dv_model = LoaderDVmodelMash(date_report=datetime.now())
        loader_dv_model.load_hub_and_link_mash()
        loader_dv_model.load_satellit_mash_student_mash()
        loader_dv_model.load_satellit_mash_subject_mash_base()

    get_data_from_mash_object_storage = get_data_from_mash_object_storage()

taskflow = taskflow()