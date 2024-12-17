from datetime import date
from pydantic import BaseModel, Field, field_validator

class Course(BaseModel):
    id: int
    position: int
    title: str | None
    title_en: str | None | float
    description: str | None
    language: str

    @field_validator('language')
    def check_language(cls, language):
        if language not in ('ru'):
            raise ValueError('Invalid language')
        return language

    platform: int
    social_image_url: str | None
    similar_authors: list | str
    similar_course_lists: list | str
    similar_specializations: list | str
    courses: int | list