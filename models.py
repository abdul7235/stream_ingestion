from pydantic import BaseModel

class RequestData(BaseModel):
    rtmp_link: str
    uid: str

class Task(BaseModel):
    task_id: str
    status: str
