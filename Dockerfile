FROM python:3.12.3-slim

COPY . .

RUN apt-get update \
    && apt-get -y install libpq-dev gcc libxml2-dev libxslt-dev zlib1g-dev \
    && pip install -r requirements.txt

CMD ["python", "main.py"]
