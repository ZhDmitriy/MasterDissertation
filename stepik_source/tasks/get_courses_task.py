"""
    Information about courses on platform Stepik.org
"""

from stepik_source.authorization import OAuthApi
import luigi
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

    def getEndpointStepikReviewsCourses(self) -> str:
        with open(r"E:\DataPlatfromEducation\stepik_source\config.yaml", "r") as file:
            endpointsStepikCourseReview = yaml.safe_load(file)
            return endpointsStepikCourseReview['api_endpoints_stepik'][1]['get_reviews_courses'][0]


endpoitsStepikCourse = EndpointsCourseStepik()
acsessToken = OAuthApi()


class GetStepikCourses(luigi.Task):

    def run(self):
        nPage = 1
        coursesData = {
            'id': [], 'position': [], 'title': [],
            'title_en': [], 'description': [], 'language': [],
            'platform': [], 'social_image_url': [],
            'similar_authors': [], 'similar_course_lists': [],
            'similar_specializations': [], 'courses': []
        }

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
        return luigi.LocalTarget(r"E:\DataPlatfromEducation\stepik_source\tasks_file\coursesStepik.csv")


class ValidationStepikCourses(luigi.Task):


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
        return luigi.LocalTarget(r"E:\DataPlatfromEducation\stepik_source\tasks_file\validationStepik.csv")


class GetCoursesReviewsStepik(luigi.Task):

    def requires(self):
        return ValidationStepikCourses()

    def run(self):
        courseReviewsData = {'id': [], 'course': [], 'user': [], 'score': [], 'text': [],
                             'reply_text': [], 'reply_created_at ': [], 'reply_updated_at': [],
                             'reply_created_by': [], 'reply_updated_by': [], 'create_date': [],
                             'update_date': [], 'translations': [], 'epic_count': [],
                             'abuse_count': [], 'vote_delta': [], 'vote': []}
        validationDataCourse = pd.read_csv(r"E:\DataPlatfromEducation\stepik_source\tasks_file\validationStepik.csv")
        courses = validationDataCourse['courses']

        def makeRequestsCourseReviews(course: int) -> json:
            endpointCourseReviewsStepik = f"{endpoitsStepikCourse.getEndpointStepikReviewsCourses()}={course}"
            response = requests.get(url=endpointCourseReviewsStepik, headers={'Authorization': 'Bearer ' + acsessToken.getToken()}).json()
            return response

        for numCourse in courses:
            print(numCourse)
            responseReviewsCourse = makeRequestsCourseReviews(course=numCourse)
            for itemReviewsCourse in responseReviewsCourse['course-reviews']:
                courseReviewsData['id'].append(itemReviewsCourse['id'])
                courseReviewsData['course'].append(itemReviewsCourse['course'])
                courseReviewsData['user'].append(itemReviewsCourse['score'])
                courseReviewsData['text'].append(itemReviewsCourse['text'])
                courseReviewsData['reply_text'].append(itemReviewsCourse['reply_text'])
                courseReviewsData['reply_updated_at'].append(itemReviewsCourse['reply_updated_at'])
                courseReviewsData['reply_created_by'].append(itemReviewsCourse['reply_created_by'])
                courseReviewsData['reply_updated_by'].append(itemReviewsCourse['reply_updated_by'])
                courseReviewsData['create_date'].append(itemReviewsCourse['create_date'])
                courseReviewsData['update_date'].append(itemReviewsCourse['update_date'])
                courseReviewsData['translations'].append(itemReviewsCourse['translations'])
                courseReviewsData['epic_count'].append(itemReviewsCourse['epic_count'])
                courseReviewsData['abuse_count'].append(itemReviewsCourse['abuse_count'])
                courseReviewsData['vote_delta'].append(itemReviewsCourse['vote_delta'])
                courseReviewsData['vote'].append(itemReviewsCourse['vote'])
        return pd.DataFrame(courseReviewsData).to_csv(r"E:\DataPlatfromEducation\stepik_source\tasks_file\courseReviewsDataStepik.csv")

    def output(self):
        return luigi.LocalTarget(r"E:\DataPlatfromEducation\stepik_source\tasks_file\courseReviewsDataStepik.csv")


if __name__ == '__main__':
    luigi.build([
        GetCoursesReviewsStepik()
    ])