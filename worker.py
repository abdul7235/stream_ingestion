from celery import Celery

app = Celery("stream_reader",broker="pyamqp://guest@rabbitmq_container//",include=["tasks"])