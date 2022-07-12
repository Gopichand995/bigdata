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
    select_ver_str = "SELECT ID, STATUS, FILETYPE, ITEMNAME, REQUESTDATA FROM " + db_schema + ".JOURNAL_DOWNLOAD_TRACE " + \
                     "WHERE CREATEDBY = %s and ISDELETE = %s"
    df_download = pd.read_sql(select_ver_str, conn, params=[user_id, False])
    if not df_download.empty:
        df_download.rename(columns={"filetype": "fileType",
                                    "itemname": "itemName",
                                    "requestdata": "reqData"}, inplace=True)
        data_json = df_download.to_json(orient='records')
        data_json = json.loads(data_json)
        for i in range(0, len(data_json)):
            data_json[i]["reqData"] = json.loads(data_json[i]["reqData"])
            del data_json[i]["reqData"]["reqInfo"]
    else:
        data_json = []
    # output = json.dumps(data_json)
    # output = Response(output, status=200, mimetype='application/json')
    print(data_json, '\n', len(data_json))
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
