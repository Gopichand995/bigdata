import pandas as pd
from pathlib import Path
import shutil
import inspect
import os
import sys
import json
import datetime
import psycopg2
from flask import Response
import jc3commonutils.log_operations as logops
import jc3commonutils.connection as connection
import jc3commonutils.encrypt_decrypt as encrypt_decrypt

file_type = "csv"
created_date_time = datetime.datetime.today()
old_template_dir = "C:/Gopichand/work/r2a/development/template_columns/old_templates/"  # connection.get_template_path(comp_code)
old_template_backup_dir = "C:/Gopichand/work/r2a/development/template_columns/temp_backup/"  # can dynamically pass this param
datastore_path = "C:/Gopichand/work/r2a/development/template_columns/datastore/"  # connection.get_datastore(comp_code)
comp_code = "r2rdev_ikdv5fcd8_pgid_46f7c7b5"
db_schema = connection.get_schema(comp_code)
company_id = "993f260b-e1b0-49c3-b142-b0456a2707888"
created_by = ""
###############################################################################
LOG_DIR = logops.connection.get_log_path()
LOGPATH = connection.get_log_path()
LOGPATH = LOGPATH + "/jccc_be_log/"
KEY = connection.get_file_key()
FILEDELIMITER = connection.get_file_delimiter()


###############################################################################
def mod_col_name(column_name):
    """
    mod_col_name
    """
    logger = logops.get_logger(LOG_DIR + "/jccc_be_log/", __name__)
    fn_name_from_inspect = inspect.stack()[0][3]
    logger.info("".join(["[", fn_name_from_inspect, "]", " : Initializing"]))
    sp_chars_temp = [
        {"spChar": "(", "value": "a"},
        {"spChar": ")", "value": "b"},
        {"spChar": "[", "value": "c"},
        {"spChar": "]", "value": "d"},
        {"spChar": "{", "value": "e"},
        {"spChar": "}", "value": "f"},
        {"spChar": "/", "value": "g"},
        {"spChar": "~", "value": "h"},
        {"spChar": "!", "value": "i"},
        {"spChar": "@", "value": "j"},
        {"spChar": "#", "value": "k"},
        {"spChar": "$", "value": "l"},
        {"spChar": "%", "value": "m"},
        {"spChar": "^", "value": "n"},
        {"spChar": "&", "value": "o"},
        {"spChar": "*", "value": "p"},
        {"spChar": "=", "value": "q"},
        {"spChar": ",", "value": "r"},
        {"spChar": ":", "value": "s"},
        {"spChar": ";", "value": "t"},
        {"spChar": ">", "value": "u"},
        {"spChar": "<", "value": "v"},
        {"spChar": "<", "value": "w"},
        {"spChar": "|", "value": "x"},
        {"spChar": ".", "value": "y"},
        {"spChar": "?", "value": "z"},
        {"spChar": "+", "value": "A"},
        {"spChar": "-", "value": "B"},
        {"spChar": " ", "value": "C"},
        {"spChar": "_", "value": "D"}
    ]
    for i in range(0, len(sp_chars_temp)):
        if sp_chars_temp[i]['spChar'] in column_name:
            before_value = sp_chars_temp[i]['spChar']
            after_value = sp_chars_temp[i]['value']
            column_name = column_name.replace(before_value, after_value)
        else:
            column_name = column_name
    logger.info("".join(["[", fn_name_from_inspect, "]", " : Process Completed"]))
    return column_name


def modify_template_cols(new_template_dir, jid_template_map_path):
    """
    Get conn, con & cursor
    If pconn is needed, assign con
    modify_template_cols
    """
    try:
        _, conn, con, cursor = connection.connect_db()
        pconn = con
    except psycopg2.OperationalError:
        error_trace = sys.exc_info()
        error = {
            "errorType": str(error_trace[0])[8:][:-2],
            "errorOn": str(error_trace[1])
        }
        error = json.dumps(error)
        error = Response(error, status=604, mimetype='application/json')
        return error
    except:
        err = sys.exc_info()
        error = {
            "errorType": str(err[0])[8:][:-2],
            "errorOn": str(err[1])
        }
        error = json.dumps(error)
        error = Response(error, status=604, mimetype='application/json')
        return error
    try:
        # read the jid, template from mapping
        jid_template_map_df = pd.read_csv(jid_template_map_path)
        for template_name in list(jid_template_map_df["Template"].unique()):
            print(f"template_name: {template_name}")
            try:
                temp_df = jid_template_map_df[jid_template_map_df["Template"] == template_name]
                jid_list = list(temp_df["Journal Id"].unique())
                print(f"jid_list: {jid_list}")

                # back up old template
                old_template_path = old_template_dir + template_name + '.' + file_type
                old_template_backup_path = old_template_backup_dir + template_name + '.' + file_type
                Path(old_template_backup_dir).mkdir(parents=True, exist_ok=True)
                shutil.copy(old_template_path, old_template_backup_path)

                # decrypt the old template file
                encrypt_decrypt.decrypt(old_template_path, KEY)

                # fetch the old, new template columns & comparision
                old_template_df = pd.read_csv(old_template_path)
                new_template_path = new_template_dir + template_name + '.' + file_type
                new_template_df = pd.read_csv(new_template_path)
                old_template_cols, new_template_cols = old_template_df.columns, new_template_df.columns
                added_col_list = list(set(new_template_cols) - set(old_template_cols))
                print(f"added_cols_list: {added_col_list}")
                del_col_list = list(set(old_template_cols) - set(new_template_cols))
                print(f"del_col_list: {del_col_list}")

                # column names cleaned(stripped, new line removal) & replaced old template with new one, removed new one
                m1_tmp = {}
                for i in range(0, len(added_col_list)):
                    new_colname = added_col_list[i].strip()
                    new_colname = str(new_colname).replace("\n", " ")
                    m_tmp = {added_col_list[i]: new_colname}
                    m1_tmp = {**m1_tmp, **m_tmp}
                print(f"m1_tmp: {m1_tmp}")
                new_template_df.rename(columns=m1_tmp, inplace=True)
                if file_type == "csv":
                    print(f"filetype: {file_type}")
                    new_template_df.to_csv(old_template_path, index=False, encoding="utf-8")
                else:
                    new_template_df.to_csv(old_template_dir + "/" + template_name + ".csv", index=False,
                                           encoding="utf-8")
                os.remove(new_template_path)

                # db operation: added columns insertion & renamed columns deletion(update?)
                select_str1 = "Select templateid from " \
                              + db_schema + ".journal_template_master where lower(name) = %s " \
                              + "AND companyid = %s AND isdeleted = false AND isactive = true"
                template_id_df = pd.read_sql(select_str1, pconn, params=[template_name.lower(), company_id])
                template_id = template_id_df.iloc[0, 0]
                add_col_list = list(m1_tmp.values())
                key_str1 = "name, ismandatoryfordr, ismandatoryforcr, templateid, createdby, createddatetime, modname"
                for add_col in add_col_list:
                    mod_name = mod_col_name(add_col)
                    cursor.execute("INSERT INTO "
                                   + db_schema + ".journal_template_columns ("
                                   + key_str1 + ") VALUES(%s, %s, %s, %s, %s, %s, %s)",
                                   (add_col, False, False, int(template_id), created_by,
                                    created_date_time, mod_name))
                for del_col in del_col_list:
                    delete_str = "delete from " + db_schema + \
                                 ".journal_template_columns where templateid = %s AND name = %s"
                    pd.read_sql(delete_str, pconn, params=[int(template_id), del_col])
                pconn.commit()

                # replace master template files for each jid provided
                for jid in jid_list:
                    try:
                        jid_temp_path = datastore_path + str(jid) + '/Master/' + template_name + '.' + file_type
                        shutil.copy(old_template_path, jid_temp_path)
                    except Exception as err:
                        print(str(err))
                        continue
            except Exception as err:
                print(str(err))
                continue
    # exception handling
    except:
        err = sys.exc_info()
        error = {
            "errorType": str(err[0])[8:][:-2],
            "errorOn": str(err[1])
        }
        print(error)
    finally:
        connection.close_db(conn, con, cursor)


if __name__ == "__main__":
    modify_template_cols(new_template_dir="C:/Gopichand/work/r2a/development/template_columns/new_templates/",
                         jid_template_map_path="C:/Gopichand/work/r2a/development/template_columns/jid_template_mapping.csv")
