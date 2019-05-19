FROM python:3-slim

WORKDIR /stream

ADD requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "main.py" ]
