from datetime import date
from pydantic import BaseModel, Field

class Course(BaseModel):
    id: int
    name: str