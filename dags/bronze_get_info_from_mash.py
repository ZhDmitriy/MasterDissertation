""" Получение данных из системы МЭШ """

import requests 
import re
import pandas as pd 
import hashlib
from datetime import datetime, timedelta
import json
from io import BytesIO
import boto3
from botocore.config import Config

from airflow.decorators import task, dag
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import Variable


STUDENT_BASE = [
    {"person_id": "6682664c-3fdb-475d-acdf-0e1585554c30", 
     "profile_id": "5653075", 
     "person_name": "Руслан Сижажев", 
     "class_student": 10, 
     "subject_love_student": "Физика; Информатика; Английский язык; Литература", 
     "student_advanced_level_name": "Физика; Математика; Информатика", 
     "auth_token": Variable.get('AUTH_TOKEN')
    }
    ]


S3_YANDEX_OBJECT_STORAGE_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_KEY')
S3_YANDEX_OBJECT_STORAGE_SECRET_KEY = Variable.get('S3_YANDEX_OBJECT_STORAGE_SECRET_KEY')
S3_YANDEX_OBJECT_STORAGE_ENDPOINT = Variable.get('S3_YANDEX_OBJECT_STORAGE_ENDPOINT')


ACADEMIC_YEAR = [
    {
        "id": 1,
        "name": "2014 - 2015",
        "begin_date": "2014-09-01",
        "end_date": "2015-08-31",
        "calendar_id": 27
    },
    {
        "id": 3,
        "name": "2015 - 2016",
        "begin_date": "2015-09-01",
        "end_date": "2016-08-31",
        "calendar_id": 29
    },
    {
        "id": 4,
        "name": "2016 - 2017",
        "begin_date": "2016-09-01",
        "end_date": "2017-08-31",
        "calendar_id": 30
    },
    {
        "id": 5,
        "name": "2017 - 2018",
        "begin_date": "2017-09-01",
        "end_date": "2018-08-31",
        "calendar_id": 31
    },
    {
        "id": 6,
        "name": "2018 - 2019",
        "begin_date": "2018-09-01",
        "end_date": "2019-08-31",
        "calendar_id": 32
    },
    {
        "id": 7,
        "name": "2019 - 2020",
        "begin_date": "2019-09-01",
        "end_date": "2020-08-31",
        "calendar_id": 33
    },
    {
        "id": 8,
        "name": "2020 - 2021",
        "begin_date": "2020-09-01",
        "end_date": "2021-08-31",
        "calendar_id": 37
    },
    {
        "id": 9,
        "name": "2021 - 2022",
        "begin_date": "2021-09-01",
        "end_date": "2022-08-31",
        "calendar_id": 38
    },
    {
        "id": 10,
        "name": "2022 - 2023",
        "begin_date": "2022-09-01",
        "end_date": "2023-08-31",
        "calendar_id": 40
    },
    {
        "id": 11,
        "name": "2023 - 2024",
        "begin_date": "2023-09-01",
        "end_date": "2024-08-31",
        "calendar_id": 43
    },
    {
        "id": 12,
        "name": "2024 - 2025",
        "begin_date": "2024-09-01",
        "end_date": "2025-08-31",
        "calendar_id": 52
    },
    {
        "id": 13,
        "name": "2025 - 2026",
        "begin_date": "2025-09-01",
        "end_date": "2026-08-31",
        "calendar_id": 53
    },
    {
        "id": 14,
        "name": "2026 - 2027",
        "begin_date": "2026-09-01",
        "end_date": "2027-08-31",
        "calendar_id": 54
    },
    {
        "id": 15,
        "name": "2027 - 2028",
        "begin_date": "2027-09-01",
        "end_date": "2028-08-31",
        "calendar_id": 55
    },
    {
        "id": 16,
        "name": "2028 - 2029",
        "begin_date": "2028-09-01",
        "end_date": "2029-08-31",
        "calendar_id": 56
    },
    {
        "id": 17,
        "name": "2029 - 2030",
        "begin_date": "2029-09-01",
        "end_date": "2030-08-31",
        "calendar_id": 57
    },
    {
        "id": 18,
        "name": "2030 - 2031",
        "begin_date": "2030-09-01",
        "end_date": "2031-08-31",
        "calendar_id": 58
    },
    {
        "id": 19,
        "name": "2031 - 2032",
        "begin_date": "2031-09-01",
        "end_date": "2032-08-31",
        "calendar_id": 59
    },
    {
        "id": 20,
        "name": "2032 - 2033",
        "begin_date": "2032-09-01",
        "end_date": "2033-08-31",
        "calendar_id": 60
    },
    {
        "id": 21,
        "name": "2033 - 2034",
        "begin_date": "2033-09-01",
        "end_date": "2034-08-31",
        "calendar_id": 61
    }
]


class MashSystem: 
  """ Система МЭШ """

  def __init__(self, person_id: str, token: str, profile_id: str, academic_year: dict, student_name: str): 
        self.person_id = person_id
        self.token = token
        self.profile_id = profile_id
        self.academic_year_mapping = academic_year
        self.student_name = student_name

  def get_result_score_student(self, academic_year_id: str) -> pd.DataFrame:
        """ Получение итоговых оценок студента """
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Authorization': 'Bearer ' + self.token,
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Profile-Id': self.profile_id,
            'Profile-Type': 'student',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'X-mes-subsystem': 'familyweb',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }
        params = {
            'person_id': self.person_id,
            'academic_year_id': academic_year_id
        }
        response = requests.get('https://school.mos.ru/api/family/web/v1/final_marks/v2', params=params, headers=headers).json()

        period_name = ""
        begin_date = ""
        end_date = ""
        calendar_id = ""
        for year_id in self.academic_year_mapping:
            if str(academic_year_id) == str(year_id['id']):
                period_name = year_id['name']
                begin_date = year_id['begin_date']
                end_date = year_id['end_date']
                calendar_id = year_id['calendar_id']

        result_rows = []
        for subject_mark_year in response['subjects']:
            
            periods_list = subject_mark_year['periods']
            periods_str = ""
            
            if periods_list:
                first_period = periods_list[0]
                start_date_period = first_period.get('start_date', '')
                end_date_period = first_period.get('end_date', '')
                periods_str = f"{start_date_period} - {end_date_period}"
                
                year_mark = first_period.get('final_mark')
                intermediate_attestation_mark = first_period.get('name')
            else:
                periods_str = ""
                year_mark = subject_mark_year.get('year_mark')
                intermediate_attestation_mark = subject_mark_year.get('intermediate_attestation_mark')
            
            hashKeyDiff = hashlib.md5(
                f"{subject_mark_year['id']};{periods_str};{year_mark};{self.student_name};{self.person_id}"
                f"{intermediate_attestation_mark};{subject_mark_year['training_camp_mark']};{period_name};{begin_date};{end_date};{calendar_id}"
                .encode('utf-8')
            ).hexdigest()
            
            result_rows.append([
                subject_mark_year['id'], 
                academic_year_id, 
                period_name, 
                begin_date, 
                end_date, 
                calendar_id, 
                self.student_name,
                self.person_id, 
                subject_mark_year['name'], 
                periods_str, 
                year_mark, 
                intermediate_attestation_mark, 
                subject_mark_year['training_camp_mark'], 
                hashKeyDiff, 
                datetime.now().strftime("%Y-%m-%d")
            ])

        data_result = pd.DataFrame(data=result_rows, columns=[
            'subject_id', 'academic_year_id', 'period_name', 'begin_date', 
            'end_date', 'calendar_id', 'student_name', 'person_id', 'subject_name', 
            'periods', 'year_mark', 'intermediate_attestation_mark', 
            'training_camp_mark', 'hashKeyDiff', 'load_dttm'
        ])
        
        return data_result 

  def parse_grade_item(self, item) -> json:
        """ Парсинг эндпоинтов """

        parsed = {
            'id': item.get('id'),
            'subject': item.get('subject_name'),
            'subject_id': item.get('subject_id'),
            'control_form': item.get('control_form_name'),
            'weight': item.get('weight', 1),
            'has_files': item.get('has_files', False),
            'is_point': item.get('is_point', False),
            'is_exam': item.get('is_exam', False),
        }
        
        if item.get('date'):
            parsed['date'] = item['date']
        else:
            parsed['date'] = None
        
        for time_field in ['created_at', 'updated_at']:
            if item.get(time_field):
                try:
                    parsed[time_field] = datetime.fromisoformat(item[time_field].replace('Z', '+00:00'))
                except ValueError:
                    parsed[time_field] = item[time_field]
            else:
                parsed[time_field] = None
        
        grade_value = item.get('value')
        parsed['original_value'] = grade_value
        
        grade_values = item.get('values', [{}])
        grade_data = grade_values[0].get('grade', {}) if grade_values else {}
        
        if grade_value == 'См':
            parsed['type'] = 'average_mark'
            parsed['is_numeric'] = False
            parsed['value'] = None
            
            comment = item.get('comment', '')
            if comment and 'балл' in comment.lower():
                match = re.search(r'(\d+\.?\d*)', comment.replace(',', '.'))
                if match:
                    parsed['comment_value'] = float(match.group(1))
                    parsed['comment'] = comment
                else:
                    parsed['comment_value'] = None
                    parsed['comment'] = comment
            else:
                parsed['comment_value'] = None
                parsed['comment'] = comment
                
        else:
            parsed['type'] = 'numeric_grade'
            parsed['is_numeric'] = True        
            parsed['five_point'] = grade_data.get('five')
            parsed['ten_point'] = grade_data.get('ten')
            parsed['hundred_point'] = grade_data.get('hundred')
            parsed['value'] = parsed['five_point']
            parsed['comment'] = item.get('comment', '')
            parsed['comment_value'] = None
        
        if grade_values and len(grade_values) > 0:
            grade_system = grade_values[0]
            parsed['grade_system'] = {
                'name': grade_system.get('name'),
                'id': grade_system.get('grade_system_id'),
                'type': grade_system.get('grade_system_type'),
                'max_score': grade_system.get('nmax')
            }
        else:
            parsed['grade_system'] = None
        
        parsed['point_date'] = item.get('point_date')
        parsed['criteria'] = item.get('criteria')
        parsed['comment_exists'] = item.get('comment_exists', False)
        parsed['original_grade_system_type'] = item.get('original_grade_system_type')

        return parsed

  def get_current_score_student(self, date_from: str, date_to: str) -> pd.DataFrame: 
        """ Получение текущих оценок о студенте """
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Authorization': 'Bearer ' + self.token,
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Profile-Id': self.profile_id,
            'Profile-Type': 'student',
            'Referer': 'https://school.mos.ru/diary/marks/current-marks',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'X-mes-subsystem': 'familyweb',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        params = {
            'student_id': self.profile_id,
            'from': date_from,
            'to': date_to,
        }

        response = requests.get('https://school.mos.ru/api/family/web/v1/marks', params=params, headers=headers).json()
        
        result_rows = []
        for score_item in response['payload']:
            parse_grade = self.parse_grade_item(item=score_item)
            hashKeyDiff = hashlib.md5(
                f"{parse_grade['subject_id']};{parse_grade['value']};{parse_grade['comment']};"
                f"{parse_grade['weight']};{parse_grade['point_date']};{parse_grade['control_form']};"
                f"{parse_grade['comment_exists']};{parse_grade['created_at']};{parse_grade['updated_at']};"
                f"{parse_grade['criteria']};{parse_grade['date']};{parse_grade['has_files']};{parse_grade['is_point']};{parse_grade['is_exam']};"
                f"{parse_grade['original_grade_system_type']};{parse_grade['grade_system']};{parse_grade.get('five_point', 0)};{parse_grade['original_value']};{parse_grade['type']};{parse_grade['id']};{parse_grade.get('hundred_point', 0)};{parse_grade['comment_value']};{self.person_id};{self.student_name}"
                .encode('utf-8')
            ).hexdigest()
            result_rows.append([parse_grade['id'], self.person_id, self.profile_id, self.student_name, parse_grade['subject'], parse_grade['subject_id'], 
                                parse_grade['control_form'], parse_grade['weight'], parse_grade['has_files'], parse_grade['is_point'], parse_grade['is_exam'], 
                                parse_grade['date'], parse_grade['created_at'], parse_grade['updated_at'], parse_grade['original_value'], parse_grade['type'], 
                                parse_grade['is_numeric'], parse_grade.get('five_point', 0), parse_grade.get('ten_point', 0), parse_grade.get('hundred_point', 0), parse_grade['value'], 
                                parse_grade['comment'], parse_grade['comment_value'], parse_grade['grade_system'], parse_grade['point_date'], parse_grade['criteria'], 
                                parse_grade['comment_exists'], parse_grade['original_grade_system_type'], hashKeyDiff, datetime.now().strftime("%Y-%m-%d")])
            
            data = pd.DataFrame(data=result_rows, columns=['parse_grade_id', 'person_id', 'profile_id', 'student_name', 'subject',  
                                                           'subject_id', 'control_form_name', 'weight', 'has_files', 'is_point', 'is_exam', 'date', 'created_at', 
                                                           'updated_at', 'original_value', 'type', 
                                                           'is_numeric', 'five_point', 'ten_point', 'hundred_point', 'value', 'comment', 'comment_value',  
                                                           'grade_system', 'point_date', 'criteria', 'comment_exists', 'original_grade_system_type', 'hashKeyDiff', 'load_dttm'])
        return data

  def parse_history_of_passes(self, data: dict) -> json:
        """ Парсинг истории пропусков """

        result = []
        for day_data in data.get('payload', []):
            day_result = {
                'date': day_data['date'],
                'lessons': [],
                'total_lessons': len(day_data['lessons']),
                'has_lessons': len(day_data['lessons']) > 0
            }
            for lesson in day_data['lessons']:
                lesson_info = {
                    'begin': lesson['begin_time'],
                    'end': lesson['end_time'],
                    'subject': lesson['subject_name'],
                    'lesson_name': lesson['lesson_name'],
                    'is_absent': lesson['absence_reason_id'] is not None,
                    'absence_reason': lesson['absence_reason_id'],
                    'is_virtual': lesson['is_virtual'],
                    'subject_id': lesson['subject_id'],
                    'lesson_id': lesson['lesson_id']
                }   
                if lesson['begin_time'] and lesson['end_time']:
                    try:
                        start = datetime.strptime(lesson['begin_time'], '%H:%M')
                        end = datetime.strptime(lesson['end_time'], '%H:%M')

                        if end < start:
                            end = datetime.strptime(lesson['end_time'], '%H:%M') + timedelta(days=1)
                        
                        duration = (end - start).seconds // 60
                        lesson_info['duration_minutes'] = duration
                        lesson_info['time_range'] = f"{lesson['begin_time']}-{lesson['end_time']}"
                    except:
                        lesson_info['duration_minutes'] = None
                        lesson_info['time_range'] = None
                else:
                    lesson_info['duration_minutes'] = None
                    lesson_info['time_range'] = None
                
                day_result['lessons'].append(lesson_info)
            
            result.append(day_result)
        
        return result

  def get_history_of_passes(self, dates: str):
        """ Получение истории пропусков и посещений занятий """

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Authorization': 'Bearer ' + self.token,
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Profile-Id': self.profile_id,
            'Profile-Type': 'student',
            'Referer': 'https://school.mos.ru/diary/school/attendance',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'X-mes-subsystem': 'familyweb',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        params = {
            'student_id': self.profile_id,
            'dates': dates,
        }

        response = requests.get('https://school.mos.ru/api/family/web/v1/schedule/short', params=params, headers=headers)
        response = self.parse_history_of_passes(response.json())

        mapping_new_lesson = {
            'Группа Разговоры о важном 10 класс': '33923999', 
            'Группа Россия — мои горизонты 10 класс': '33924000', 
            'Мир путешествий(Мир путешествий 2025-2026)': '33924001', 
            'Олимпиадная математика-10 кл(Олимпиадная математика -10 кл 2025-2026)': '33924002'
        }

        result_rows_lesson = [] 
        for item in response:
            for item_lesson in item['lessons']:
                subject_id = item_lesson['subject_id']
                subject_name = item_lesson['subject']
                
                if (str(subject_id) == '0' or str(subject_id) == '') and subject_name in mapping_new_lesson:
                    subject_id = mapping_new_lesson[subject_name]
                
                hashKeyDiff = hashlib.md5(
                    f"{subject_id};{item['date']};{item_lesson['lesson_id']};{item_lesson['begin']};"
                    f"{item_lesson['end']};{item_lesson['duration_minutes']};{item_lesson['absence_reason']};"
                    f"{item_lesson['lesson_name']};{item_lesson['time_range']};{item_lesson['is_virtual']};{self.person_id};{self.student_name}"
                    .encode('utf-8')
                ).hexdigest()
                
                result_rows_lesson.append([
                    item['date'], 
                    item_lesson['begin'], 
                    self.person_id, 
                    self.profile_id, 
                    self.student_name, 
                    item_lesson['end'], 
                    item_lesson['subject'], 
                    item_lesson['lesson_name'], 
                    item_lesson['is_absent'], 
                    item_lesson['absence_reason'], 
                    item_lesson['is_virtual'], 
                    str(subject_id), 
                    item_lesson['lesson_id'], 
                    item_lesson['duration_minutes'], 
                    item_lesson['time_range'], 
                    hashKeyDiff, 
                    datetime.now().strftime("%Y-%m-%d")
                ])
        
        data_rows_lesson = pd.DataFrame(data=result_rows_lesson, columns=['date', 'begin_time', 'person_id', 'profile_id', 'student_name', 'end_time', 'subject', 
                                                                          'lesson_name', 'is_absent', 'absence_reason', 'is_virtual', 'subject_id', 'lesson_id', 
                                                                          'duration_minutes', 'time_range', 'hashKeyDiff', 'load_dttm'])

        return data_rows_lesson

  def get_homework_and_shedule(self, begin_date: str, end_date: str):
        """ Получение домашнего задания и расписания студента """

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Authorization': self.token,
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Profile-Id': self.profile_id,
            'Profile-Type': 'student',
            'Referer': 'https://school.mos.ru/diary/schedules/week/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'X-Mes-Role': 'student',
            'X-mes-subsystem': 'familyweb',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        params = {
            'person_ids': self.person_id,
            'begin_date': begin_date,
            'end_date': end_date,
            'expand': 'homework',
            'source_types': 'PLAN,AE,EC,EVENTS,AFISHA,ORGANIZER,OLYMPIAD,PROF',
        }

        response = requests.get('https://school.mos.ru/api/eventcalendar/v1/api/events', params=params, headers=headers).json()

        result_rows = []
        for item in response['response']: 
            homework = item.get('homework', {})

            # hash для sat с shedule 
            hashKeyDiffShedule = hashlib.md5(
                f"{item['start_at']};{item['finish_at']};{item.get('cancelled', 0)};"
                f"{item.get('lesson_type', 0)};{item.get('replaced', 0)};{item.get('room_name', 0)};"
                f"{item.get('room_number', 0)};{item.get('link_to_join', 0)};{item.get('health_status', 0)};"
                f"{item.get('absence_reason_id', 0)};{item.get('nonattendance_reason_id', 0)};{self.person_id};{self.student_name}"
                .encode('utf-8')
            ).hexdigest()

            # hash для sat с homework
            hashKeyDiffHomework = hashlib.md5(
                f"{item['start_at']};{item['finish_at']};{homework.get('total_count', 0)};"
                f"{homework.get('execute_count', 0)};{homework.get('descriptions', [''])};{homework.get('materials', [])};"
                f"{item.get('marks', 0)};{item.get('is_missed_lesson', 0)};{self.person_id};{self.student_name}"
                .encode('utf-8')
            ).hexdigest()
            
            mapping_new_lesson = {
                'Группа Разговоры о важном 10 класс': '33923999', 
                'Группа Россия — мои горизонты 10 класс': '33924000', 
                'Мир путешествий(Мир путешествий 2025-2026)': '33924001', 
                'Олимпиадная математика-10 кл(Олимпиадная математика -10 кл 2025-2026)': '33924002'
            }
        
            materials = homework.get('materials', [])

            if (materials and 
                isinstance(materials, list) and 
                len(materials) > 0 and 
                isinstance(materials[0], dict) and
                'count_execute' in materials[0] and 
                'count_learn' in materials[0]):
                
                count_execute = materials[0].get('count_execute', '')
                count_learn = materials[0].get('count_learn', '')
                materials_parsed = f"{count_execute}/{count_learn}"
            else:
                materials_parsed = materials

            result_rows.append([
                item['id'], 
                self.person_id, 
                self.profile_id, 
                self.student_name, 
                item['source_id'], 
                item['source'], 
                item['start_at'], 
                item['finish_at'], 
                item.get('cancelled', 0), 
                item.get('lesson_type', 0), 
                item.get('course_lesson_type', 0), 
                item.get('lesson_form', 0), 
                item.get('replaced', 0), 
                item.get('room_name', 0), 
                item.get('room_number', 0), 
                mapping_new_lesson.get(item.get('subject_name', 0), item.get('subject_id', 0)) 
                if str(item.get('subject_id', 0)) == '0' 
                else item.get('subject_id', 0), 
                item.get('subject_name', 0), 
                item.get('link_to_join', 0), 
                item.get('health_status', 0), 
                item.get('absence_reason_id', 0), 
                item.get('nonattendance_reason_id', 0), 
                homework.get('presence_status_id', 0), 
                homework.get('total_count', 0), 
                homework.get('execute_count', 0), 
                homework.get('descriptions', ['']), 
                materials_parsed,  
                item.get('marks', 0), 
                item.get('is_missed_lesson', 0), 
                hashKeyDiffShedule, 
                hashKeyDiffHomework, 
                datetime.now().strftime("%Y-%m-%d")
            ])
                        
            data_homework_and_shedule = pd.DataFrame(data=result_rows, columns=['id', 'person_id', 'profile_id', 'student_name', 'source_id', 'source', 
                                                                                'start_at', 'finish_at', 'cancelled', 'lesson_type', 'course_lesson_type', 
                                                                                'lesson_form', 'replaced', 'room_name', 'room_number', 'subject_id', 
                                                                                'subject_name', 'link_to_join', 'health_status', 'absence_reason_id', 
                                                                                'nonattendance_reason_id', 'presence_status_id', 'homework_count', 'execute_count', 
                                                                                'descriptions', 'materials', 'marks', 'is_missed_lesson', 'hashKeyDiffShedule', 'hashKeyDiffHomework', 'load_dttm'])
            
            data_homework_and_shedule = data_homework_and_shedule.astype(str)

        return data_homework_and_shedule
  
  def get_data_independent_diagnostics(self) -> pd.DataFrame: 
        """ Получение данных по прохождению независимой диагностики """

        headers = {
            'Accept': '*/*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Authorization': 'Bearer ' + self.token,
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Referer': 'https://school.mos.ru/portfolio/student/study',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'X-Mes-Subsystem': 'studentportfolioweb',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        params = {
            'count': '2',
        }

        response = requests.get(
            f'https://school.mos.ru/portfolio/app/persons/{self.person_id}/independent-diagnostic-grouped',
            params=params,
            headers=headers
        )

        if response.status_code == 200:         
            resultData = []
            for item_learning_year in response.json()['data']:
                for item_diagnostic in item_learning_year['diagnostics']['diagnostic']:
                    hashKeyDiff = hashlib.md5(
                        f"{item_learning_year['learningYear']};{self.person_id};{self.student_name};"
                        f"{item_diagnostic['recordId']};{item_diagnostic['workId']};{item_diagnostic['eventDate']};"
                        f"{item_diagnostic['subject']};{item_diagnostic['schoolId']};{item_diagnostic['name']};"
                        f"{item_diagnostic['maxResult']};{item_diagnostic['resultValue']};{item_diagnostic['percentResult']};{item_diagnostic['levelType']};{item_diagnostic['note']};"
                        f"{item_diagnostic['isVisible']};{item_diagnostic['markValue5']}"
                        .encode('utf-8')
                    ).hexdigest()
                    resultData.append([item_learning_year['learningYear'], self.person_id, self.profile_id, self.student_name, 
                                        item_diagnostic['recordId'], item_diagnostic['workId'], 
                                        item_diagnostic['eventDate'], item_diagnostic['subject'], item_diagnostic['schoolId'], 
                                        item_diagnostic['name'], item_diagnostic['maxResult'], item_diagnostic['resultValue'], 
                                        item_diagnostic['percentResult'], item_diagnostic['levelType'], item_diagnostic['note'], 
                                        item_diagnostic['isVisible'], item_diagnostic['markValue5'], hashKeyDiff, datetime.now().strftime("%Y-%m-%d")])
        else: 
            print(response.text)
            
        return pd.DataFrame(data=resultData, 
                            columns=['learningYear', 'person_id', 'profile_id', 
                                    'student_name', 'recordId', 'workId', 
                                    'eventDate', 'subject', 'schoolId', 'diagnostic_name', 
                                    'maxResult', 'resultValue', 'percentResult', 'levelType', 
                                    'diagnostic_note', 'isVisible', 'markValue5', 'hashKeyDiff', 
                                    'load_dttm'])

  def get_result_exam_student(self) -> pd.DataFrame: 
        """ Получение данных по результатам сдачи экзаменов """

        headers = {
            'Accept': '*/*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Authorization': 'Bearer ' + self.token,
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Referer': 'https://school.mos.ru/portfolio/student/study',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'X-Mes-Subsystem': 'studentportfolioweb',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        response = requests.get(
            f'https://school.mos.ru/portfolio/app/persons/{self.person_id}/govexams/list/',
            headers=headers
        )

        if response.status_code == 200: 
            resultData = []
            for item_exam in response.json()['data']:
                resultData.append([item_exam['id'], self.person_id, self.profile_id, self.student_name, item_exam['name'], 
                                item_exam['date'], item_exam['formaGia'], item_exam['normalizedMarkValue'], item_exam['normalizedMarkBasis'], 
                                item_exam['primaryMarkValue'], item_exam['primaryMarkBasis'], item_exam['positiveResultThreshold'], 
                                item_exam['percentMarkValue'], item_exam['isCredit'], item_exam['examsId'], item_exam['approbation'], 
                                item_exam['examResult']['variant'], item_exam['examResult']['scoreA'], item_exam['examResult']['scoreB'], 
                                item_exam['examResult']['scoreC'], item_exam['examResult']['scoreD'], item_exam['subjectID'], 
                                datetime.now().strftime("%Y-%m-%d")])
        else: 
            print(response.text)
        
        return pd.DataFrame(data=resultData, 
                            columns=['exam_id', 'person_id', 'profile_id', 'student_name', 'exam_name', 
                                     'exam_date', 'formaGia', 'normalizedMarkValue', 'normalizedMarkBasis', 
                                     'primaryMarkValue', 'primaryMarkBasis', 'positiveResultThreshold', 
                                     'percentMarkValue', 'isCredit', 'examsId', 'approbation', 'exam_result_variant', 
                                     'exam_result_score_a', 'exam_result_score_b', 'exam_result_score_c', 'exam_result_score_d', 
                                     'subject_id', 'load_dttm'])

  def get_participation_olympic(self) -> pd.DataFrame:
        """ Участие в олимпиадах """

        headers = {
            'Accept': '*/*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Authorization': 'Bearer ' + self.token,
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Referer': 'https://school.mos.ru/portfolio/student/study',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'X-Mes-Subsystem': 'studentportfolioweb',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        response = requests.get(
            f'https://school.mos.ru/portfolio/app/persons/{self.person_id}/events/list',
            headers=headers
        )

        if response.status_code == 200:
            resultData = []
            for item_olympiad in response.json()['data']:
                for item_file in item_olympiad['fileReferences']:
                    resultData.append([item_olympiad['id'], self.person_id, self.profile_id, self.student_name, 
                                        item_olympiad['personId'], item_file['creationDate'], item_file['editDate'], 
                                        item_file['dataKind'], item_file['hashCode'], item_file['creatorId'], 
                                        item_file['isImport'], item_file['name'], item_file['isDelete'], item_file['source']['code'], 
                                        item_file['source']['value'], item_file['source']['id'], item_file['linkedObjects'], item_file['category']['code'], 
                                        item_file['category']['value'], item_file['category']['parentId'], item_file['category']['isArchive'], 
                                        item_file['category']['shortValue'], item_file['category']['id'], item_file['categoryCode'], 
                                        item_file['levelEvent'], item_file['organizators'], item_file['format']['code'],
                                        item_file['format']['value'], item_file['format']['isArchive'], item_file['format']['id'], 
                                        item_file['linkedObjectIds'], item_file['result'], item_file['description'], item_file['profile'], 
                                        item_file['location'], item_file['participantNumber'], item_file['stageEvent'], item_file['startDate'], 
                                        item_file['endDate'], item_file['actionStage'], item_file['maxScore'], item_file['schoolId'], item_file['parallelId'], 
                                        item_file['resultId'], item_file['resultUrl'], item_file['participationStatus']['code'], item_file['participationStatus']['value'], 
                                        item_file['participationStatus']['isArchive'], item_file['subjects']['code'], item_file['subjects']['value'], 
                                        item_file['subjects']['isArchive'], item_file['subjects']['adaptability'], item_file['subjects']['id'], datetime.now().strftime("%Y-%m-%d")])
        else: 
            print(response.text)

        return pd.DataFrame(data=resultData, 
                            columns=['olympiad_id', 'person_id', 'profile_id', 'student_name', 'personId', 'creationDate', 'editDate', 
                                     'dataKind', 'hashCode', 'creatorId', 'isImport', 'name', 'isDelete', 'source_code','source_value', 
                                      'source_id', 'linkedObjects', 'category_code', 'category_value', 'category_parentId', 
                                      'category_isArchive', 'category_shortValue', 'category_id', 'categoryCode', 'levelEvent', 
                                      'organizators', 'format_code', 'format_value', 'format_isArchive', 'format_id', 'linkedObjectIds', 
                                      'result', 'description', 'profile', 'location', 'participantNumber', 'stageEvent', 'startDate', 
                                      'endDate', 'actionStage', 'maxScore', 'schoolId', 'parallelId', 'resultId', 'resultUrl', 'participationStatus_code', 
                                      'participationStatus_value', 'participationStatus_isArchive', 'subjects_code', 'subjects_value', 
                                      'subjects_isArchive', 'subjects_adaptability', 'subjects_id', 'load_dttm'])


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
@dag('bronze_get_info_from_mash', 
     schedule=CronTriggerTimetable('30 6 * * *', timezone='Europe/Moscow'), 
     default_args=DEFAULT_ARGS, max_active_tasks=3, tags=['bronze', 'info_from_mash'], 
     catchup=False)
def taskflow():
    
    @task
    def get_result_score_student_tsk():
        """ Получение итоговых оценок """

        report_result_score_all = []

        for student_base in STUDENT_BASE:
            print(f"Обработка студента, person_id: {student_base['person_id']}")
            print(f"Обработка студента, person_name: {student_base['person_name']}")

            mash_system = MashSystem(
                    person_id=student_base['person_id'], 
                    student_name=student_base['person_name'],
                    profile_id=student_base['profile_id'], 
                    token=student_base['auth_token'], 
                    academic_year=ACADEMIC_YEAR)
            
            
            for academic_year in ACADEMIC_YEAR:
                data_mash = mash_system.get_result_score_student(academic_year_id=str(academic_year['id']))
                report_result_score_all.append(data_mash)

        report_result_score = pd.concat(report_result_score_all)

        load_data_to_object_storage(df_report=report_result_score, 
                                    file_name="students_result_score", 
                                    folder_name="students_result_score", 
                                    save_history=False)

    @task
    def get_get_current_score_student_tsk():
        """ Получение текущих оценок """
        
        report_result_current_score_all = []

        for student_base in STUDENT_BASE:
            print(f"Обработка студента, person_id: {student_base['person_id']}")
            print(f"Обработка студента, person_name: {student_base['person_name']}")

            mash_system = MashSystem(
                person_id=student_base['person_id'], 
                student_name=student_base['person_name'],
                profile_id=student_base['profile_id'], 
                token=student_base['auth_token'], 
                academic_year=ACADEMIC_YEAR)
            
            date_from = (datetime.now() - timedelta(days=1095)).strftime("%Y-%m-%d")
            date_to = datetime.now().strftime("%Y-%m-%d")

            data_mash = mash_system.get_current_score_student(date_from=date_from, date_to=date_to)
            report_result_current_score_all.append(data_mash)

        report_result_current_score = pd.concat(report_result_current_score_all)

        load_data_to_object_storage(df_report=report_result_current_score, 
                            file_name="students_current_score", 
                            folder_name="students_current_score", 
                            save_history=True)

    @task
    def get_history_of_passes_tsk():
        """ Получение истории пропусков и посещений """

        report_result_history_of_passes_all = []

        for student_base in STUDENT_BASE:
            print(f"Обработка студента, person_id: {student_base['person_id']}")
            print(f"Обработка студента, person_name: {student_base['person_name']}")
            
            mash_system = MashSystem(
                person_id=student_base['person_id'], 
                student_name=student_base['person_name'],
                profile_id=student_base['profile_id'], 
                token=student_base['auth_token'], 
                academic_year=ACADEMIC_YEAR)
            
            dates = []

            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            current_date = start_date
            while current_date <= end_date:
                dates.append(current_date.date().strftime("%Y-%m-%d"))
                current_date += timedelta(days=1)

            dates = ",".join(dates)

            data_mash = mash_system.get_history_of_passes(dates=dates)
            report_result_history_of_passes_all.append(data_mash)

        report_result_history_of_passes = pd.concat(report_result_history_of_passes_all)

        load_data_to_object_storage(df_report=report_result_history_of_passes, 
                    file_name="students_history_of_passes", 
                    folder_name="students_history_of_passes",
                    save_history=True)

    @task
    def get_homework_and_shedule_tsk():
        """ Получение домашнего задания и расписания """
        
        report_result_homework_and_schedule_all = []

        for student_base in STUDENT_BASE:
            print(f"Обработка студента, person_id: {student_base['person_id']}")
            print(f"Обработка студента, person_name: {student_base['person_name']}")

            mash_system = MashSystem(
                person_id=student_base['person_id'], 
                student_name=student_base['person_name'],
                profile_id=student_base['profile_id'], 
                token=student_base['auth_token'], 
                academic_year=ACADEMIC_YEAR)
            
            begin_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            end_date = datetime.now().strftime("%Y-%m-%d")

            data_mash = mash_system.get_homework_and_shedule(begin_date=begin_date, end_date=end_date)
            report_result_homework_and_schedule_all.append(data_mash)

        report_result_homework_and_schedule = pd.concat(report_result_homework_and_schedule_all)

        load_data_to_object_storage(df_report=report_result_homework_and_schedule, 
            file_name="students_homework_and_schedule", 
            folder_name="students_homework_and_schedule",
            save_history=True)
        
    @task
    def get_data_independent_diagnostics_tsk():

        reports_independent_diagnostics = []
        for student_base in STUDENT_BASE:
            print(f"Обработка студента, person_id: {student_base['person_id']}")
            print(f"Обработка студента, person_name: {student_base['person_name']}")

            mash_system = MashSystem(
                person_id=student_base['person_id'], 
                student_name=student_base['person_name'],
                profile_id=student_base['profile_id'], 
                token=student_base['auth_token'], 
                academic_year=ACADEMIC_YEAR)
            
            data_mash = mash_system.get_data_independent_diagnostics()
            reports_independent_diagnostics.append(data_mash)

            report_independent_diagnostics = pd.concat(reports_independent_diagnostics)

            load_data_to_object_storage(df_report=report_independent_diagnostics, 
                file_name="students_independent_diagnostics", 
                folder_name="students_independent_diagnostics",
                save_history=False)

    @task
    def get_result_exam_students_tsk():
        """ Получение итоговых баллов и оценок по сдаче государственных экзаменов """
        
        reports_exam_students = []
        for student_base in STUDENT_BASE:
            print(f"Обработка студента, person_id: {student_base['person_id']}")
            print(f"Обработка студента, person_name: {student_base['person_name']}")

            mash_system = MashSystem(
                person_id=student_base['person_id'], 
                student_name=student_base['person_name'],
                profile_id=student_base['profile_id'], 
                token=student_base['auth_token'],  
                academic_year=ACADEMIC_YEAR)
            
            data_mash = mash_system.get_result_exam_student()
            reports_exam_students.append(data_mash)

            report_exam_students = pd.concat(reports_exam_students)

            load_data_to_object_storage(df_report=report_exam_students, 
                file_name="students_result_exam_score", 
                folder_name="students_result_exam_score", 
                save_history=False)

    @task
    def get_participation_olympic_tsk():
        """ Получение данных по участию в олимпиадах """
        
        reports_participation_olympic = []
        for student_base in STUDENT_BASE:
            print(f"Обработка студента, person_id: {student_base['person_id']}")
            print(f"Обработка студента, person_name: {student_base['person_name']}")
            
            mash_system = MashSystem(
                person_id=student_base['person_id'], 
                student_name=student_base['person_name'],
                profile_id=student_base['profile_id'], 
                token=student_base['auth_token'], 
                academic_year=ACADEMIC_YEAR)
            
            data_mash = mash_system.get_participation_olympic()
            reports_participation_olympic.append(data_mash)

            report_participation_olympic = pd.concat(reports_participation_olympic)

            load_data_to_object_storage(df_report=report_participation_olympic, 
                file_name="students_participation_olympic", 
                folder_name="students_participation_olympic", 
                save_history=False)

    @task
    def get_student_mash_base_tsk():
        """ Получение базовой информации для каждого ученика """
        
        resultData = []
        for item in STUDENT_BASE: 
            hashKeyDiff = hashlib.md5(
                f"{item['person_id']};{item['profile_id']};{item['person_name']};"
                f"{item['class_student']};{item['subject_love_student']};{item['student_advanced_level_name']}"
                .encode('utf-8')
            ).hexdigest()
            resultData.append([item['person_id'], item['profile_id'], 
                               item['person_name'], item['class_student'], 
                               item['subject_love_student'], item['student_advanced_level_name'], hashKeyDiff, datetime.now().strftime("%Y-%m-%d")])
            
        load_data_to_object_storage(df_report=pd.DataFrame(data=resultData, 
                                                           columns=['person_id', 'profile_id', 'person_name', 
                                                                    'class_student', 'subject_love_student', 
                                                                    'student_advanced_level_name', 'hashKeyDiff', 'load_dttm']), 
                file_name="students_main_base", 
                folder_name="students_main_base", 
                save_history=True)

    get_result_score_student_tsk = get_result_score_student_tsk()
    get_get_current_score_student_tsk = get_get_current_score_student_tsk()
    get_history_of_passes_tsk = get_history_of_passes_tsk()
    get_homework_and_shedule_tsk = get_homework_and_shedule_tsk()
    get_data_independent_diagnostics_tsk = get_data_independent_diagnostics_tsk()
    get_result_exam_students_tsk = get_result_exam_students_tsk()
    get_participation_olympic_tsk = get_participation_olympic_tsk()
    get_student_mash_base_tsk = get_student_mash_base_tsk()

    get_result_score_student_tsk >> get_get_current_score_student_tsk >> get_history_of_passes_tsk >> get_homework_and_shedule_tsk >> get_data_independent_diagnostics_tsk >> get_result_exam_students_tsk >> get_participation_olympic_tsk >> get_student_mash_base_tsk


taskflow = taskflow()