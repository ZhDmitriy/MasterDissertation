""" 
    Модели pydantic для обеспечения качества данных
"""

from pydantic import BaseModel, field_validator, Field
from datetime import date, datetime


class DmStudentsSyntheticResults(BaseModel):
    student_guid: str 
    name_exam: str 
    subject: str 
    date_event: date 
    primary_score: int 
    mark: int 
    min_primary_score: int 
    max_primary_score: int 
    number_protocol_gak: str 
    data_protocol_gak: date 

    @field_validator('mark')
    @classmethod
    def check_mark_student(cls, mark): 
        mark = int(mark)
        if (mark < 1) or (mark > 5): 
            raise ValueError('Оценка не может быть больше 5 или меньше 1')


class DmStudentsMainSyntheticInfo(BaseModel): 
    student_guid: str 
    student_info: str 
    student_fio: str 
    student_school: str 
    student_class: str 
    student_birthday: date

    @field_validator('student_class')
    @classmethod
    def check_student_class(cls, student_class): 
        student_class = str(student_class)[:-1]
        if student_class.isdigit(): 
            class_num = int(student_class)
            if (class_num > 11) or (class_num < 1): 
                raise ValueError('Номер класса не может быть меньше 1 или больше 11')
            return student_class
        else: 
            raise ValueError('Класс должен быть указан цифрой')

    @field_validator('student_birthday')
    @classmethod 
    def check_student_birthday(cls, student_birthday): 
        student_birthday = datetime.strptime(student_birthday, "%Y-%m-%d")
        date_today = datetime.now().date().isoformat()
        if (date_today - student_birthday) < 6: 
            raise ValueError('Ученик не может быть младше 6 лет')


class DmUniversityWorldInfo(BaseModel): 
    pass 


class DmSubjectShedule(BaseModel): 
    pass 


class DmSubjectMashScoreCurrent(BaseModel):
    pass 


class DmSubjectMashScoreHistory(BaseModel):
    pass 


class DmSubjectMashOmissionsHistory(BaseModel):
    pass 


class DmStepikLessonByCourse(BaseModel):
    pass 


class DmStepikCourses(BaseModel): 
    pass 