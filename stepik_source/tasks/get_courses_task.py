"""
    Information about courses on platform Stepik.org
"""

from luigi import Task, run, build, LocalTarget
from stepik_source.authorization import OAuthApi
import json
from tqdm import tqdm
import requests
from typing import NoReturn
import pandas as pd
from stepik_source.schemas.course import Course

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
except ImportError as e:
    print("Failed to import pyspark modules. Please install them and try again.")

try:
    import yaml
except ImportError as e:
    print("Failed to import pyyaml modules. Please install them and try again.")


class EndpointsCourseStepik:

    def getEnpointsStepikCourseList(self) -> str:
        with open(r"E:\DataPlatfromEducation\stepik_source\config.yaml", "r") as file:
            endpointsStepikCourse = yaml.safe_load(file)
            return endpointsStepikCourse['api_endpoints_stepik'][0]['get_course'][0]

endpoitsStepikCourse = EndpointsCourseStepik()

class GetStepikCourses(Task):

    def run(self):
        nPage = 1
        coursesData = {
            'id': [], 'position': [], 'title': [],
            'title_en': [], 'description': [], 'language': [],
            'platform': [], 'social_image_url': [],
            'similar_authors': [], 'similar_course_lists': [],
            'similar_specializations': [], 'courses': []
        }
        acsessToken = OAuthApi()

        def parseStepikCourseJson(response: json) -> NoReturn:
            for itemCourse in  response['course-lists']:
                for numCourse in itemCourse['courses']:
                    coursesData['id'].append(itemCourse['id'])
                    coursesData['position'].append(itemCourse['position'])
                    coursesData['title'].append(itemCourse['title'])
                    coursesData['title_en'].append(itemCourse['title_en'])
                    coursesData['description'].append(itemCourse['description'])
                    coursesData['language'].append(itemCourse['language'])
                    coursesData['platform'].append(itemCourse['platform'])
                    coursesData['social_image_url'].append(itemCourse['social_image_url'])
                    coursesData['similar_authors'].append(itemCourse['similar_authors'])
                    coursesData['similar_course_lists'].append(itemCourse['similar_course_lists'])
                    coursesData['similar_specializations'].append(itemCourse['similar_specializations'])
                    coursesData['courses'].append(numCourse)

        def makeRequests(nPage = 1) -> json:
            endpointCourseStepik = f"{endpoitsStepikCourse.getEnpointsStepikCourseList()}={nPage}"
            response = requests.get(url=endpointCourseStepik, headers={'Authorization': 'Bearer ' + acsessToken.getToken()}).json()
            return response

        response = makeRequests(nPage=1)
        parseStepikCourseJson(response)
        while response['meta']['has_next']:
            nPage += 1
            response = makeRequests(nPage=nPage)
            parseStepikCourseJson(response)
        pd.DataFrame(coursesData).to_csv(r"E:\DataPlatfromEducation\stepik_source\tasks_file\coursesStepik.csv")

    def output(self):
        return LocalTarget(r"E:\DataPlatfromEducation\stepik_source\tasks_file\coursesStepik.csv")


class ValidationStepikCourses(Task):


    def requires(self):
        return GetStepikCourses()

    def run(self):
        stepikData = pd.read_csv(r"E:\DataPlatfromEducation\stepik_source\tasks_file\coursesStepik.csv")
        validationCoursesData = {
            'id': [], 'position': [], 'title': [],
            'title_en': [], 'description': [], 'language': [],
            'platform': [], 'social_image_url': [],
            'similar_authors': [], 'similar_course_lists': [],
            'similar_specializations': [], 'courses': []
        }
        for itemCourseStepik in tqdm(range(len(stepikData))):
            try:
                validateCourse = Course(
                    id=stepikData.iloc[itemCourseStepik]['id'],
                    position=stepikData.iloc[itemCourseStepik]['position'],
                    title=stepikData.iloc[itemCourseStepik]['title'],
                    title_en=stepikData.iloc[itemCourseStepik]['title_en'],
                    description=stepikData.iloc[itemCourseStepik]['description'],
                    language=stepikData.iloc[itemCourseStepik]['language'],
                    platform=stepikData.iloc[itemCourseStepik]['platform'],
                    social_image_url=stepikData.iloc[itemCourseStepik]['social_image_url'],
                    similar_authors=stepikData.iloc[itemCourseStepik]['similar_authors'],
                    similar_course_lists=stepikData.iloc[itemCourseStepik]['similar_course_lists'],
                    similar_specializations=stepikData.iloc[itemCourseStepik]['similar_course_lists'],
                    courses=stepikData.iloc[itemCourseStepik]['courses']
                )
                validationCoursesData['id'].append(validateCourse.id)
                validationCoursesData['position'].append(validateCourse.position)
                validationCoursesData['title'].append(validateCourse.title)
                validationCoursesData['title_en'].append(validateCourse.title_en)
                validationCoursesData['description'].append(validateCourse.description)
                validationCoursesData['language'].append(validateCourse.language)
                validationCoursesData['platform'].append(validateCourse.platform)
                validationCoursesData['social_image_url'].append(validateCourse.social_image_url)
                validationCoursesData['similar_authors'].append(validateCourse.similar_authors)
                validationCoursesData['similar_course_lists'].append(validateCourse.similar_course_lists)
                validationCoursesData['similar_specializations'].append(validateCourse.similar_specializations)
                validationCoursesData['courses'].append(validateCourse.courses)
            except ValueError as e:
                print("Validation error: ", e)
        return pd.DataFrame(validationCoursesData).to_csv(r"E:\DataPlatfromEducation\stepik_source\tasks_file\validationStepik.csv")

    def output(self):
        return LocalTarget(r"E:\DataPlatfromEducation\stepik_source\tasks_file\validationStepik.csv")



if __name__ == '__main__':
    build([
        ValidationStepikCourses()
    ], workers=1, local_scheduler=True)