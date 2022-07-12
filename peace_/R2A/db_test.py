import psycopg2
import pandas as pd
import json


def connect_db():
    # conn = psycopg2.connect(**dbCred)
    database = "postgresql"
    conn = psycopg2.connect(
        host="bed8e2d5-eu-de.lb.appdomain.cloud",
        port="5432",
        database="dkl488ee8c964364",
        user="postgres",
        password="Passw@rd12345678"
    )
    conn.autocommit = False
    con = conn
    cursor = con.cursor()
    return database, conn, con, cursor


_, conn, con, cursor = connect_db()
comp_code = "r2rdev_dkl488ee8_pgid_1d8e45ed"
user_id = "WID3e618b69-fe0d-4d50-81a5-6d2ee251adf5"
db_schema = "r2rdev_dkl488ee8_pgid_1d8e45ed"

try:
    select_ver_str = "SELECT JID, RUNDATE, RUNVERSION, REQUESTDATA, ITEMNAME, COAFIELDID, COAFILEID, TEMPLATEID FROM " + \
                     db_schema + ".JOURNAL_DOWNLOAD_TRACE " + \
                     "WHERE ID = %s"
    df_download = pd.read_sql(select_ver_str, conn, params=[270,])
    req_body = df_download["requestdata"][0]
    req_body = json.loads(req_body)
    print(req_body["slaCategory"])
except Exception as e:
    print('error\n', str(e))
finally:
    if conn:
        if cursor:
            cursor.close()
            print("db_conn_closed")
        conn.close()
    else:
        pass