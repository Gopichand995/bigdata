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
                    ),
                    grp as (
                        select grp_mas.id groupid, grp_mas.name groupname,
                               grp_mem.id elementid, grp_mem.name elementname
                        from  {dbschema}.journal_group_master grp_mas,
                              {dbschema}.journal_group_members grp_mem
                        where grp_mas.id = grp_mem.groupid
                        and grp_mas.isactive = true
                    ),
                    jrgrpele as (
                        select jr_grp.jid,
                            string_agg((grp.groupname || '-' || grp.elementname) ,'--' order by jr_grp.groupid, jr_grp.elementid ) grpelestr
                        from r2rdev_dkl488ee8_pgid_1d8e45ed.journal_group_element jr_grp
                             inner join grp on (jr_grp.groupid = grp.groupid and jr_grp.elementid = grp.elementid)
                        group by jr_grp.jid
                    ),
                    ind_detail as (
                        select indid, indname
                        from {dbschema}.indicator_master
                    )
                    select result.*
                    from (
                    select  ji.id, ji.name, ji.frequency, ji.templateid, jtm.name templatename, jcm.name calendarname, 
                            (case when jrh.status is null then ji.status
                                  when jrh.status is not null and ji.status != 'Approved' then ji.status
                                  else jrh.status
                            end ) journalstatus,
                            ji.ownedby preparer, ji.approver, ji.reviewer, ji.superuser, ji.createdby,
                            (case when ji.status = 'In Progress' then ji.ownedby
                                  when ji.status = 'Pending Approval' then ji.approver
                                  when ji.status = 'Approved' and jrh.status = 'Submit For Review' then ji.ownedby
                                  when ji.status = 'Approved' and jrh.status = 'Run Error' then ji.ownedby
                                  when ji.status = 'Approved' and jrh.status = 'Rejected' then ji.ownedby
                                  when ji.status = 'Approved' and jrh.status = 'Pending Sign Off' then ji.reviewer
                                  when ji.status = 'Approved' and jrh.status = 'Review 2' then jrh.reviewer2nd
                                  else null
                                  end ) actionowner,
                            ji.sourcecontact, ji.sourcesla, ji.sourceslatime, ji.postingcontact, ji.postingdate, ji.postingdatetime,
                            jrgrpele.grpelestr, ind_detail.indname as indicatorname, jrh.rundate, jrh.status runstatus, 
                            (case when ji.status = 'In Progress' then round((extract(epoch from(now() - (coalesce(ji.statuschangedatetime, ji.createddatetime))))/86400)::numeric, 2)
                                  when ji.status = 'Returned' then round((extract(epoch from(now() - (coalesce(ji.statuschangedatetime, ji.createddatetime))))/86400)::numeric, 2)
                                  else null
                            end ) inprogdaycount,
                            coalesce(ji.updateddatetime, ji.createddatetime) as lastupdatedatetime, ji.ispaused,
                            ji.statuschangedatetime
                    from {dbschema}.journal_info ji
                         left outer join jrh on (jrh.id = ji.id)
                         left outer join {dbschema}.journal_template_master jtm on (jtm.templateid = ji.templateid)
                         left outer join {dbschema}.journal_calendar_master jcm on (jcm.id = ji.calendarid)
                         left outer join jrgrpele on (jrgrpele.jid = ji.id)
                         left outer join ind_detail on (ind_detail.indid = ji.indid)
                    where lower(ji.frequency) != 'adhoc'
                    and ji.status != 'Approved'
                    and ji.status != 'Pending Approval'
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
    df_result["inprogdaycount"] = df_result["inprogdaycount"].fillna(0)
    time_dict = {
        "0": "0 am", "1": "1 am", "2": "2 am", "3": "3 am", "4": "4 am", "5": "5 am", "6": "6 am",
        "7": "7 am", "8": "8 am", "9": "9 am", "10": "10 am", "11": "11 am", "12": "12 pm",
        "13": "1 pm", "14": "2 pm", "15": "3 pm", "16": "4 pm", "17": "5 pm", "18": "6 pm",
        "19": "7 pm", "20": "8 pm", "21": "9 pm", "22": "10 pm", "23": "11 pm"
    }
    df_result["sourceslatime"].replace(time_dict, inplace=True)
    df_result["postingdatetime"].replace(time_dict, inplace=True)
    df_result.to_csv("C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\\data\\df_result.csv")
    recent_count = len(df_result[df_result["inprogdaycount"] <= int(separators[0])])
    moderate_count = len(df_result[(df_result["inprogdaycount"] > int(separators[0])) & (
            df_result["inprogdaycount"] <= int(separators[1]))])
    risk_count = len(df_result[(df_result["inprogdaycount"] > int(separators[1])) & (
            df_result["inprogdaycount"] <= int(separators[2]))])
    delay_count = len(df_result[df_result["inprogdaycount"] > int(separators[2])])
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
