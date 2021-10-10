FROM python:3.8.3-slim

COPY . .

RUN apt-get update \
    && apt-get -y install libpq-dev gcc libxml2-dev libxslt-dev \
    && pip install -r requirements.txt

CMD ["python", "main.py"]
