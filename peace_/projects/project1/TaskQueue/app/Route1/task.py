from celery import Celery
from time import sleep

# app = Celery('delay', broker="redis://localhost:6379/0", backend="db+sqlite:///db.sqlite3")


# @app.task
# def reverse(text):
#     sleep(5)
#     return text[::-1]


# @app.task
# # def email(fname, lname):
# #     sleep(5)
# #     return f"{(fname + lname).lower()}@gmail.com"

celery = Celery(
    'delay',
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)


@celery.task()
def background(n):
    delay_ = 5
    print("Task Running")
    print(f"Simulating a {delay_} second delay")
    sleep(delay_)
    print("Task Completed")
    _ = f"Hello {n}"
    return _
