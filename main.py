from fastapi import FastAPI
from models import RequestData, Task
from tasks import stream_reader
app = FastAPI()


@app.post("/startpublisher" , response_model=Task, status_code = 202)
async def start_publisher(requestData: RequestData):
    task_id = stream_reader.delay(requestData.rtmp_link, requestData.uid)
    return {'task_id': str(task_id), 'status': 'Processing'}


#celery -A worker worker --pool=solo -l info
#celery -A worker worker --loglevel=INFO
#uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2