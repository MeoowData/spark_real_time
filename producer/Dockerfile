
FROM python:3.9

WORKDIR /usr/src/app

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY ./producer.py .

CMD [ "python", "producer.py" ]
