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

str_select0 = """
    select distinct grp_mas.name groupname
    from  {dbschema}.journal_group_master grp_mas,
          {dbschema}.journal_group_members grp_mem
    where grp_mas.id = grp_mem.groupid
    and grp_mas.isactive = true
    and grp_mas.isdeleted = false
""".format(dbschema=db_schema)
groups = pd.read_sql(str_select0, con, params=[])
groups_dict = dict(zip(list(groups.loc[:, "groupname"]), ["Group with element(1)", "Group with element(2)"]))
print(groups_dict)

str_select = """
    with
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
                        string_agg(
                        (grp.groupname || '=`!' || grp.elementname)
                        ,'^|~' order by jr_grp.groupid, jr_grp.elementid ) grpelestr
                    from {dbschema}.journal_group_element jr_grp
                         inner join grp on (jr_grp.groupid = grp.groupid and jr_grp.elementid = grp.elementid)
                    group by jr_grp.jid
                )
    select ji.id, jrgrpele.grpelestr
    from {dbschema}.journal_info ji
    left outer join jrgrpele on (jrgrpele.jid = ji.id)
""".format(dbschema=db_schema)
df_result = pd.read_sql(str_select, con,
                        params=[role_name, user_id, role_name, user_id,
                                role_name, user_id, role_name, user_id,
                                company_id])

if conn:
    if cursor:
        cursor.close()
        print("db_conn_closed")
    conn.close()
else:
    pass

df_result["Group with element(1)"], df_result["Group with element(2)"] = None, None
if not df_result.empty:
    for rows in df_result.index.values:
        if df_result.at[rows, "grpelestr"] is not None:
            for each_group in df_result.at[rows, "grpelestr"].split('^|~'):
                each_comp = each_group.split('=`!')
                df_result.at[rows, groups_dict.get(each_comp[0])] = each_comp[0] + "-" + each_comp[1]

print(df_result)
