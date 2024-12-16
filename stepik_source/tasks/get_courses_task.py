"""
    Information about courses on platform Stepik.org
"""


from luigi import Task, run, build, LocalTarget
from stepik_source.authorization import OAuthApi
import json
import requests
from typing import NoReturn
import pandas as pd



class GetStepikCourses(Task):

    courseStepikData = "coursesStepik.csv"

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

        # TODO: try with pyspark library
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
            endpointCourseStepik = "https://stepik.org:443/api/course-lists?page={}".format(nPage)
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
        return LocalTarget(self.courseStepikData)



if __name__ == '__main__':
    build([GetStepikCourses()], local_scheduler=True)