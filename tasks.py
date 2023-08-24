from worker import app
from celery.utils.log import get_task_logger
from utils.producer import publisher

#celery_log = get_task_logger(__name__)

@app.task(name='stream_reader')
def stream_reader(rtmp_link, uid):
    publisher(rtmp_link, uid)
    #celery_log.info(f"Celery task completed!")
    return 'OK'