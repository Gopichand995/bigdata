from flask import Flask, jsonify
import os
import json
import sys
import re
import sqlite3
import pandas as pd
import numpy as np
from random import randint
from uuid import uuid4

current_path = os.getcwd()

# file handling
with open("data.json", "r") as read_data:
    file_data = json.load(read_data)

# flask app creation
app = Flask(__name__)


# routing the path for api

@app.route('/home/')
@app.route('/')
def home():
    return f"<h1> Lana Del Rey </h1>"


@app.route('/lana')
def details():
    # sqlite db connection
    conn = sqlite3.connect("employees.db")
    cursor = conn.cursor()

    # create db table
    create_str = """CREATE TABLE IF NOT EXISTS employees (
                        first text,
                        last text,
                        age int,
                        others text
                        )"""
    cursor.execute(create_str)

    # insert multiple records
    for _ in range(5):
        employee = [f"first{randint(1, 100)}", f"last{randint(1, 100)}", randint(1, 100), str(uuid4())[:8]]
        cursor.execute("INSERT INTO employees VALUES (?, ?, ?, ?)",
                       (employee[0], employee[1], employee[2], employee[3]))

    # saves the changes
    conn.commit()

    # get the results
    cursor.execute("SELECT * FROM employees")
    df = pd.DataFrame(cursor.fetchall())
    df.rename(columns={0: "First Name", 1: "Last Name", 2: "Age", 3: "Others"}, inplace=True)
    print(f"df:\n {df}")
    df_json = df.to_json(orient="index")
    print(f"df_json:\n {df_json}")

    # delete the records
    # cursor.execute("DELETE FROM employees where 1=1")
    # conn.commit()
    # print("records deleted successfully")

    # closes the open db connections
    conn.close()

    # exception handling
    try:
        # system argument
        index_ = sys.argv[1]

        # df json data
        output = {}
        data = json.loads(df_json)
        output["data"] = data

        # file meta data
        file_data["index"] = index_
        output["file_meta"] = file_data
        output["isSuccess"] = True
        return jsonify(output), 201
    except Exception as err_:
        error_json = {
            "isSuccess": False,
            "error_message": str(err_)
        }
        return error_json, 604


if __name__ == "__main__":
    app.run(port=8080, debug=True)






















#
# @app.route('/lana/<name>')
# def full_name(name):
#     return f"<h1> Lana's full name is {name.upper()} </h1>"
#
#
# @app.route('/lana_songs/<name>')
# def song_start_letter(name):
#     return f"<h1> Lana's nth song starts with {name[10]} </h1>"
#
#
# @app.route('/lana')
# def index_():
#     # test = ["apple", "boy", "cat", "dog", "elephant"]
#     return render_template("lana.html")
#
#
# @app.route('/')
# def index():
#     return render_template("index.html")
#
#
# @app.route('/sign_up')
# def signup_form():
#     return render_template("sign_up.html")
#
#
# @app.route('/thank_you')
# def thank_you():
#     first = request.args.get("first")
#     last = request.args.get("last")
#     return render_template("thankyou.html", first=first, last=last)
#
#
# @app.errorhandler(404)
# def page_not_found(e):
#     return render_template("404.html"), 404


# class SqliteDataBase:
#     def __int__(self):
#         self.conn = sqlite3.connect("employees.db")
#         self.c = self.conn.cursor()
#
#     def create_db(self):
#         self.c.execute("""CREATE TABLE IF NOT EXISTS employees (
#                     first text,
#                     last text,
#                     age int,
#                     pay float
#                     )""")
#         self.conn.commit()
#         print("db created successfully")
#
#     def insert_into_db(self):
#         for employee in employees:
#             self.c.execute("INSERT INTO employees VALUES (?, ?, ?)",
#                            (employee[0], employee[1], employee[2]))
#         self.conn.commit()
#         print("db values inserted successfully")
#
#     def fetch_from_db(self):
#         self.c.execute("SELECT * FROM employees")
#         print(self.c.fetchall())
#
#     def close_db(self):
#         self.conn.close()
#
#
# db_ = SqliteDataBase()
# db_.create_db()
# db_.insert_into_db()
# db_.fetch_from_db()
