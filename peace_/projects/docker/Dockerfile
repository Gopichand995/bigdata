FROM python:3.8-slim
ADD . /code
WORKDIR /code
RUN pip install -r requirements.txt
CMD ["python3", "app.py"]