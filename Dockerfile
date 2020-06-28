#cbe backend image

FROM python:3.6

ENV PYTHONPATH=/code/

WORKDIR /code

#copy code
COPY ./prvk8 /code/prvk8
COPY manage.py /code/

#COPY pip_cache /code/pip_cache/

COPY requirements.txt /code/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt --cache-dir /code/pip_cache

COPY start.sh /code/

EXPOSE 8000

CMD ["/code/start.sh"]

