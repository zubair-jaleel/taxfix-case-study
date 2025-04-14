FROM python:3.12.2

WORKDIR /app

COPY . .

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python", "etl.py"]
