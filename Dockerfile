FROM python:3.10

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

WORKDIR ./src

COPY src .

CMD ["python", "app.py"]
