from time import sleep
from flask import Flask
from celery import Celery
import redis

app = Flask(__name__)
cache = redis.Redis(host='redis', port='6379')


def hit_count():
    retries = 5
    while True:
        try:
            return cache.incr('hits')
        except redis.exceptions.ConnectionError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            sleep(0.5)


@app.route('/')
def hello():
    count = hit_count()
    return f"Hello Lana...I have been noticed {count} times"


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
