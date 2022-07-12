import psycopg2
import datetime
import pandas as pd


def connect_db():
    # conn = psycopg2.connect(**dbCred)
    database = "postgresql"
    conn = psycopg2.connect(
        host="localhost",
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
#local
comp_code = "r2rdev_dkl488ee8_pgid_1d8e45ed"
company_id = "b9790c86-ffa5-405d-8913-c51197b6476f3"
role_name = "Business Admin"
user_id = "WID3e618b69-fe0d-4d50-81a5-6d2ee251adf5"
separators = ['50', '75']
db_schema = "r2rdev_dkl488ee8_pgid_1d8e45ed"
#dev
# comp_code = "r2rdev_ikdv5fcd8_pgid_46f7c7b5"
# company_id = "993f260b-e1b0-49c3-b142-b0456a2707888"
# role_name = "Business Admin"
# user_id = "WIDc01d6ccd-93fd-4267-919b-56142d6cb20c"
# separators = ['50', '75']
# db_schema = "r2rdev_ikdv5fcd8_pgid_46f7c7b5"

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
                    work_day_sla as (
                        select A3.*, A4.startdate, A4.enddate from (select A1.*, A2.name from (select calendarid,
                        year, month, workday, wddate from {dbschema}.journal_calendar_workday) A1
                        left join {dbschema}.journal_calendar_master A2 on
                        A1.calendarid = A2.id) A3 inner join (select year, month, startdate, enddate from
                        {dbschema}.journal_financial_month where companyid = %s and 
                        startdate <= %s order by startdate desc limit 1) A4 on
                        (A3.year = A4.year and A3.month = A4.month)
                    ),
                    work_day_posting as (
                        select A3.* from (select A1.*, A2.name from (select calendarid,
                        year, month, workday, wddate from {dbschema}.journal_calendar_workday) A1
                        left join {dbschema}.journal_calendar_master A2 on
                        A1.calendarid = A2.id) A3 inner join (select year, month from
                        {dbschema}.journal_financial_month where companyid = %s and 
                        startdate <= %s order by startdate desc limit 1) A4 on
                        (A3.year = A4.year and A3.month = A4.month)
                    )
                    select result.*
                    from (
                        select result1.*
                        from (
                            select oldresult.*, 
                                (case when (rundate between finstartdate and finenddate)
                                           and runstatus in ('Completed', 'Not to Post')
                                           then 0
                                      else 
                                          (case 
                                              when postingtoslahrspan <= pendingtopostinghr
                                              then 0
                                              else ((postingtoslahrspan - pendingtopostinghr)/postingtoslahrspan)*100
                                          end)
                                end ) slapercent
                            from (
                                select oldresult1.*,
                                    (case when (pendingtopostinghrold < postingtoslahrspan)
                                               and (rundate between sourceslacaldate and postingcaldate)
                                               and configstatus = 'Approved'
                                               and runstatus in ('Completed', 'Not to Post')
                                               then 999999
                                          when (pendingtopostinghrold < postingtoslahrspan)
                                               and (rundate between sourceslacaldate and postingcaldate)
                                               and configstatus = 'Approved'
                                               and runstatus not in ('Completed', 'Not to Post')
                                               then pendingtopostinghrold
                                          when (pendingtopostinghrold < postingtoslahrspan)
                                               and (rundate not between sourceslacaldate and postingcaldate)
                                               and configstatus = 'Approved'
                                               then pendingtopostinghrold
                                          when (pendingtopostinghrold < 0)
                                               and (rundate between sourceslacaldate and postingcaldate)
                                               and configstatus = 'Approved'
                                               and runstatus in ('Completed', 'Not to Post')
                                               then 999999
                                          when (pendingtopostinghrold < 0)
                                               and (rundate between sourceslacaldate and postingcaldate)
                                               and configstatus = 'Approved'
                                               and runstatus not in ('Completed', 'Not to Post')
                                               then pendingtopostinghrold
                                          when (pendingtopostinghrold < postingtoslahrspan)
                                               and configstatus = 'Approved'
                                               and runstatus is null
                                               then pendingtopostinghrold
                                          when (pendingtopostinghrold > postingtoslahrspan)
                                               and configstatus = 'Approved'
                                               then 999999
                                          else 999999
                                    end ) pendingtopostinghr
                                from (
                                    select  ji.id id, ji.name jidname, ji.frequency, jtm.name templatename, 
                                            ji.sourcesla, jcm.name calendarname, 
                                            ji.createdby, ji.ownedby preparer, ji.approver, ji.reviewer, ji.superuser,
                                            ji.postingdate, to_number(ji.sourceslatime, '9999') sourceslatime, 
                                            to_number(ji.postingdatetime, '9999') postingdatetime, ji.calendarid,
                                            coalesce(ji.updateddatetime, ji.createddatetime) as lastupdatedatetime,
                                            ji.statuschangedatetime, jrh.rundate rundate, 
                                            coalesce(ispaused, false) paused,
                                            to_char(jrh.rundate,'DD-MM-YYYY') rundatestr,
                                            jrh.id jrhid, ji.status configstatus, jrh.status runstatus,
                                            work_day_posting.wddate as postingcaldate,
                                            to_char(work_day_posting.wddate,'DD-MM-YYYY') postingcaldatestr,
                                            work_day_sla.wddate as sourceslacaldate,
                                            work_day_sla.startdate as finstartdate,
                                            work_day_sla.enddate as finenddate,
                                            to_char(work_day_sla.wddate,'DD-MM-YYYY') sourceslacaldatestr,
                                            (case when round((extract(epoch from(to_timestamp(concat(work_day_posting.wddate, ' ', to_number(ji.postingdatetime, '9999'), ':00:00'), 'YYYY-MM-DD HH24:MI:SS') - to_timestamp(concat(work_day_sla.wddate, ' ', to_number(ji.sourceslatime, '9999'), ':00:00'), 'YYYY-MM-DD HH24:MI:SS')))/3600)::numeric, 2) = 0 then 1
                                                  else round((extract(epoch from(to_timestamp(concat(work_day_posting.wddate, ' ', to_number(ji.postingdatetime, '9999'), ':00:00'), 'YYYY-MM-DD HH24:MI:SS') - to_timestamp(concat(work_day_sla.wddate, ' ', to_number(ji.sourceslatime, '9999'), ':00:00'), 'YYYY-MM-DD HH24:MI:SS')))/3600)::numeric, 2)
                                            end ) postingtoslahrspan,
                                            (case when ji.status = 'In Progress' then ji.ownedby
                                            when ji.status = 'Pending Approval' then ji.approver
                                            when ji.status = 'Approved' and jrh.status = 'Submit For Review' then ji.ownedby
                                            when ji.status = 'Approved' and jrh.status = 'Run Error' then ji.ownedby
                                            when ji.status = 'Approved' and jrh.status = 'Rejected' then ji.ownedby
                                            when ji.status = 'Approved' and jrh.status = 'Pending Sign Off' then ji.reviewer
                                            when ji.status = 'Approved' and jrh.status = 'Review 2' then jrh.reviewer2nd
                                            else null
                                            end ) actionowner,
                                            round((extract(epoch from(to_timestamp(concat(work_day_posting.wddate, ' ', to_number(ji.postingdatetime, '9999'), ':00:00'), 'YYYY-MM-DD HH24:MI:SS') - now()))/3600)::numeric, 2) pendingtopostinghrold,
                                            work_day_sla.year as finyear
                                    from {dbschema}.journal_info ji
                                         inner join {dbschema}.journal_template_master jtm on (jtm.templateid = ji.templateid)
                                         left outer join {dbschema}.journal_calendar_master jcm on (jcm.id = ji.calendarid)
                                         left outer join jrh on (jrh.id = ji.id)
                                         left outer join work_day_posting on (work_day_posting.workday = ji.postingdate and work_day_posting.calendarid = ji.calendarid)
                                         left outer join work_day_sla on (work_day_sla.workday = ji.sourcesla and work_day_sla.calendarid = ji.calendarid)
                                    where lower(ji.frequency) != 'adhoc'
                                    and ji.ownedby = (case  when %s = 'Preparer' then %s else ji.ownedby end)
                                    and ji.approver = (case when %s = 'Approver' then %s else ji.approver end )
                                    and (
                                        ji.reviewer = (case when %s = 'Reviewer' then %s else ji.reviewer end)
                                        or
                                        coalesce(jrh.reviewer2nd, '') = (case when %s = 'Reviewer' then %s
                                                                              else coalesce(jrh.reviewer2nd, '') end )
                                        )
                                    and ji.companyid = %s                        
                                ) oldresult1
                            ) oldresult
                        ) result1
                    ) result
                    '''.format(dbschema=db_schema)
    df_result = pd.read_sql(str_select, con,
                            params=[company_id, curr_date, company_id, curr_date, role_name, user_id, role_name,
                                    user_id,
                                    role_name, user_id, role_name, user_id,
                                    company_id])
    # df_result.to_csv('data\\get_sla_day_count.csv')
    df_result = df_result[df_result["configstatus"] == 'Approved']
    # print('df_result\n', df_result.loc[:, "slapercent"])
    # convert the "sourceslatime" & "postingdatetime" from 24hr format to 12hr am/pm format
    time_dict = {
        "0": "0 am", "1": "1 am", "2": "2 am", "3": "3 am", "4": "4 am", "5": "5 am", "6": "6 am",
        "7": "7 am", "8": "8 am", "9": "9 am", "10": "10 am", "11": "11 am", "12": "12 pm",
        "13": "1 pm", "14": "2 pm", "15": "3 pm", "16": "4 pm", "17": "5 pm", "18": "6 pm",
        "19": "7 pm", "20": "8 pm", "21": "9 pm", "22": "10 pm", "23": "11 pm"
    }
    df_result["sourceslatime"].replace(time_dict, inplace=True)
    df_result["postingdatetime"].replace(time_dict, inplace=True)
    # print('df_result1 \n', df_result)
    # df_result.to_excel('C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\Data\\sla_test1.xlsx', index=False)

    ################################################Handling for quarterly/yearly#####
    df_qhy = df_result[df_result["frequency"].isin(["Quarterly", "Half Yearly", "Monthly"])]
    df_res22 = pd.DataFrame()
    if not df_qhy.empty:
        fin_start_dt = df_result[df_result["finstartdate"].notnull()]["finstartdate"].unique()[0]
        fin_year = int(df_result[df_result["finyear"].notnull()]["finyear"].unique()[0])
        df_qhy = df_qhy.reset_index(drop=True)
        print(df_qhy['sourceslatime'])
        jid_list = list(df_qhy["id"])
        jid_list_tup = tuple(jid_list)
        select_new_query = '''
                    with
                    	cal_mon_wd as (
                        	select A1.calendarid as calendarid1, A1.year, A1.month, A1.workday, A1.wddate, A2.name calendarname1
                        	from {dbschema}.journal_calendar_workday A1
                        		inner join {dbschema}.journal_calendar_master A2
                        		on (A1.calendarid = A2.id)
                            )
                    		select jfd.jid, jfd.runfrequency, jfd.noofoccurance , jfd.monthnum , jfd.monthvalue , cmw.wddate, cmw.workday
                    		, cmw.calendarid1 , cmw.calendarname1 , cmw.year , cmw.month
                    		from 		   
                    		    {dbschema}.journal_frequency_detail jfd 
                    	 		inner join cal_mon_wd cmw on (cmw.month = jfd.monthvalue and cmw.year = %s)
                    	 	    inner join {dbschema}.journal_info ji on (ji.id = jfd.jid
                    	 	    		and (ji.sourcesla = cmw.workday or ji.postingdate = cmw.workday))
                                        and ji.calendarid = cmw.calendarid1
                                        and ji.id in %s
                    '''.format(dbschema=db_schema)
    #     df_res = pd.read_sql(select_new_query, con, params=[fin_year, jid_list_tup])
    #     print('df_res', df_res)
    #     df_res_y = df_res[df_res["runfrequency"] == "Yearly"]
    #     df_res_o = df_res[df_res["runfrequency"] != "Yearly"]
    #     df_res_o = df_res_o[df_res_o["wddate"] >= fin_start_dt]
    #     df_res_o = df_res_o.sort_values("wddate", ascending=True)
    #     df_res_o = df_res_o.drop_duplicates(["jid", "workday"], keep='first')
    #     drop_cols = ['jid', 'runfrequency', 'noofoccurance', 'monthnum', 'monthvalue',
    #                  'wddate', 'workday', 'calendarid1', 'calendarname1', 'year', 'month']
    #     df_res1 = pd.merge(df_qhy, df_res_o, left_on=["calendarid", "id", "sourcesla"],
    #                        right_on=["calendarid1", "jid", "workday"])
    #     df_res1["sourceslacaldate"] = df_res1["wddate"]
    #     df_res1 = df_res1.drop(drop_cols, axis=1)
    #
    #     df_res11 = pd.merge(df_qhy, df_res_y, left_on=["calendarid", "id", "sourcesla"],
    #                         right_on=["calendarid1", "jid", "workday"])
    #     df_res11["sourceslacaldate"] = df_res11["wddate"]
    #     df_res11 = df_res11.drop(drop_cols, axis=1)
    #     df_res11 = pd.concat([df_res1, df_res11])
    #     df_res11 = df_res11.reset_index(drop=True)
    #     ###################################################################
    #     df_res2 = pd.merge(df_qhy, df_res_o, left_on=["calendarid", "id", "postingdate"],
    #                        right_on=["calendarid1", "jid", "workday"])
    #     df_res2["postingcaldate"] = df_res2["wddate"]
    #     df_res2 = df_res2.drop(drop_cols, axis=1)
    #
    #     df_res22 = pd.merge(df_qhy, df_res_y, left_on=["calendarid", "id", "postingdate"],
    #                         right_on=["calendarid1", "jid", "workday"])
    #     df_res22["postingcaldate"] = df_res22["wddate"]
    #     df_res22 = df_res22.drop(drop_cols, axis=1)
    #     df_res22 = pd.concat([df_res2, df_res22])
    #     df_res22 = df_res22.reset_index(drop=True)
    #     ###################################################################
    #     df_res22["sourceslacaldate"] = df_res11["sourceslacaldate"]
    #     ###########################################################################################
    #     df_res22["sourceslacaldatestr"] = pd.to_datetime(df_res22["sourceslacaldate"]).dt.strftime("%d-%m-%Y")
    #     df_res22["postingcaldatestr"] = pd.to_datetime(df_res22["postingcaldate"]).dt.strftime("%d-%m-%Y")
    #     ###########################################################################################
    #     df_res22['slawithtime'] = pd.to_datetime(df_res22["sourceslacaldate"])
    #     df_res22['slawithtime'] += pd.to_timedelta(df_res22["sourceslatime"], unit='h')
    #
    #     df_res22['postwithtime'] = pd.to_datetime(df_res22["postingcaldate"])
    #     df_res22['postwithtime'] += pd.to_timedelta(df_res22["postingdatetime"], unit='h')
    #
    #     td = pd.to_timedelta((df_res22['postwithtime'] - df_res22['slawithtime'])).dt.components
    #     df_res22["postingtoslahrspan"] = td["days"] * 24 + td["hours"] + td["minutes"] / 60
    #
    #     td = pd.to_timedelta((df_res22['postwithtime'] - datetime.datetime.now())).dt.components
    #     df_res22["pendingtopostinghr"] = td["days"] * 24 + td["hours"] + td["minutes"] / 60
    #
    #     df_res22 = df_res22.drop(["slawithtime", "postwithtime"], axis=1)
    #     ###########################################################################################
    # if not df_res22.empty:
    #     df_result = df_result[~df_result["id"].isin(list(df_qhy["id"]))]
    #     df_result = pd.concat([df_result, df_res22])
    #     # df_result = df_result.sort_values(var, ascending=[orient])
    #     df_result = df_result.reset_index(drop=True)
    #
    # print('df_result2 \n', df_result)

    # df_result.to_excel('C:\\Users\\GopichandBarri\\Documents\\Github\\peace\\Data\\sla_test2.xlsx', index=False)
    # print(df_result.loc[:, "sourceslatime"])
    # date_check = (pd.to_datetime(df_result["rundate"]) >= pd.to_datetime(df_result["finstartdate"])) & (
    #         pd.to_datetime(df_result["rundate"]) <= pd.to_datetime(df_result["finenddate"]))
    # df_complete = df_result[((df_result["runstatus"].isin(["Completed", "Not to Post"])) & date_check)]
    # # print('df_complete\n', df_complete)
    # df_rem = df_result[~((df_result["runstatus"].isin(["Completed", "Not to Post"])) & date_check)]
    # # print('df_rem\n', df_rem)
    # active_count = len(df_rem[df_rem["slapercent"] <= int(separators[0])])
    # risk_count = len(df_rem[(df_rem["slapercent"] > int(separators[0])) & (df_rem["slapercent"] <= int(separators[1]))])
    # breached_count = len(df_rem[df_rem["slapercent"] > int(separators[1])])
    # achieved_count = len(df_complete)
    # output = {
    #     "activeCount": active_count,
    #     "riskCount": risk_count,
    #     "breachedCount": breached_count,
    #     "achievedCount": achieved_count
    # }
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
