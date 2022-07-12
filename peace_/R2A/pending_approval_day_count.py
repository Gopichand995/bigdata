import psycopg2
import datetime
import pandas as pd


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
company_id = "b9790c86-ffa5-405d-8913-c51197b6476f3"
role_name = "Business Admin"
user_id = "WID3e618b69-fe0d-4d50-81a5-6d2ee251adf5"
separators = [4, 7, 10]
db_schema = "r2rdev_dkl488ee8_pgid_1d8e45ed"
curr_date = datetime.datetime.now().strftime("%Y-%m-%d")

try:
    str_select = '''
                    with
                    jrh as (
                        select b.*
                        from (
                            select a.*,
                                   row_number() over (partition by a.id order by a.rundate desc, a.runnumber desc) rn
                            from {dbschema}.journal_run_history a
                            where lower(a.runtype) != 'adhoc' ) b
                        where b.rn = 1
                    )
                    select result.*
                    from (
                    select  ji.id, ji.frequency,
                            ji.templateid, ji.ispaused, ji.sourcesla,
                            coalesce(ji.updateddatetime, ji.createddatetime) as lastupdatedatetime,
                            ji.statuschangedatetime,
                            jrh.status runstatus,
                            (case when jrh.status is null then ji.status
                                  when jrh.status is not null and ji.status != 'Approved' then ji.status
                                  else jrh.status
                            end ) journalstatus,                    
                            (case when ji.status = 'Pending Approval' then round((extract(epoch from(now() - (coalesce(ji.statuschangedatetime, ji.updateddatetime))))/86400)::numeric, 2)
                                  else null
                            end ) pendingapprovaldaycount
                    from {dbschema}.journal_info ji
                         left outer join jrh on (jrh.id = ji.id)
                    where lower(ji.frequency) != 'adhoc'
                    and ji.status = 'Pending Approval'
                    and ji.ownedby = (case  when %s = 'Preparer' then %s else ji.ownedby end)
                    and ji.approver = (case when %s = 'Approver' then %s else ji.approver end )
                    and (
                        ji.reviewer = (case when %s = 'Reviewer' then %s else ji.reviewer end)
                        or
                        coalesce(jrh.reviewer2nd, '') = (case when %s = 'Reviewer' then %s
                                                              else coalesce(jrh.reviewer2nd, '') end )
                        )
                    and ji.companyid = %s
                    ) result
                    '''.format(dbschema=db_schema)
    df_result = pd.read_sql(str_select, con,
                            params=[role_name, user_id, role_name, user_id,
                                    role_name, user_id, role_name, user_id,
                                    company_id])
    print(df_result)
    df_result["pendingapprovaldaycount"] = df_result["pendingapprovaldaycount"].fillna(0)
    recent_count = len(df_result[df_result["pendingapprovaldaycount"] <= int(separators[0])])
    moderate_count = len(df_result[(df_result["pendingapprovaldaycount"] > int(separators[0])) & (
            df_result["pendingapprovaldaycount"] <= int(separators[1]))])
    risk_count = len(df_result[(df_result["pendingapprovaldaycount"] > int(separators[1])) & (
            df_result["pendingapprovaldaycount"] <= int(separators[2]))])
    delay_count = len(df_result[df_result["pendingapprovaldaycount"] > int(separators[2])])
    output = {
        "recentCount": recent_count,
        "moderateCount": moderate_count,
        "riskCount": risk_count,
        "delayCount": delay_count
    }
    # print(output)
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
