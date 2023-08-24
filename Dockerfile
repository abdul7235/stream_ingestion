FROM python:3.8.10

RUN apt-get update && apt-get install -y ffmpeg

ADD . /stream_ingest

WORKDIR /stream_ingest

RUN pip install -r requirements.txt

#COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

#CMD uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2

#CMD celery -A worker worker --loglevel=INFO

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2 & celery -A worker worker --loglevel=INFO"]