FROM tiangolo/uwsgi-nginx-flask:python3.5
COPY ./app /app
RUN pip install -r /app/requirements.txt