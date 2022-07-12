import pandas as pd
import re

data = pd.read_excel(
    "C:\\Gopichand\\work\\r2a\\journal_isues\\4173\\input\\exchange_rates\\Copy of EXCH_RATE_12.2020.xlsx",
    thousands=',', dtype=object)
# print(data)
data.columns = data.columns.astype(str)
print(data.columns)
print(data)

columnnames = ['DUMMY', 'LOCALCURRENCYCODE', 'IEAVERAGE']
# noinspection SpellCheckingInspection
cnames = list(data.columns)
# noinspection SpellCheckingInspection
colnames = []
for i in range(0, len(cnames)):
    m_up = re.sub(r'\W', '', str(cnames[i]))
    colnames.append(m_up)
# print(colnames)
# noinspection SpellCheckingInspection
rlist = []
for i in range(0, len(columnnames)):
    if columnnames[i] in colnames:
        rlist.append(i)
print(rlist)
# noinspection SpellCheckingInspection
matchnumberstart = len(rlist)
print('matchnumberstart', matchnumberstart)
# noinspection SpellCheckingInspection
length_count = len(data)
if length_count > 5000:
    length_count = 5000
rowlist = []
count = 1
for k in range(0, length_count):
    m_col = []
    for j in range(0, len(data.columns)):
        m_up = str(data.iloc[k, j])
        m_up = re.sub(r'\W', '', m_up)
        if m_up in columnnames:
            m_col.append(m_up)
    m_col_un = list(dict.fromkeys(m_col))
    if m_col_un and m_col:
        rowlist.append(count)
        count += 1
    else:
        rowlist.append(0)
print(rowlist)
matchnumbernotstart = max(enumerate(rowlist), key=(lambda x: x[1]))[1]

print('matchnumbernotstart: ', matchnumbernotstart)
#
if matchnumbernotstart > matchnumberstart:
    # noinspection SpellCheckingInspection
    # logger.info("".join(["[", fn_name_from_inspect, "]", " : There is junk value in header"]))
    # rowcolumn = max(enumerate(rowlist), key=(lambda x: x[1]))[0]
    rowcolumn = rowlist.index(max(rowlist))
    for i in range(0, rowcolumn):
        data.drop([i], inplace=True)
    df_up = data.reset_index().drop('index', axis=1)
    df_up = df_up.rename(columns=df_up.iloc[0])[1:len(df_up)].reset_index().drop('index', axis=1)
    m1_up = {}
    # noinspection SpellCheckingInspection
    colnames = list(df_up.columns.values)
    for i in range(0, len(colnames)):
        if colnames[i] == colnames[i]:
            oldname = re.sub(" ", "", str(colnames[i]))
            if len(oldname) == 1:
                if oldname == "%":
                    new_colname = "Percent"
                elif oldname == "$":
                    new_colname = "Dollar"
                else:
                    new_colname = oldname
            else:
                new_colname = re.sub(r'\W', '', str(colnames[i]))
            m_up = {colnames[i]: new_colname}
            m1_up = {**m1_up, **m_up}
        else:
            new_colname = "Unknown" + str(i)
            m_up = {colnames[i]: new_colname}
            m1_up = {**m1_up, **m_up}
    df_up.rename(columns=m1_up, inplace=True)
    # noinspection SpellCheckingInspection
    emptycols = list(df_up.loc[:,
                     list((100 * (df_up.isnull().sum() / len(df_up.index)) == 100))].columns)
    # noinspection SpellCheckingInspection
    allcols = list(df_up.columns)
    non_match_cols = list(set(allcols) - set(emptycols))
    df_up = df_up.drop(df_up.index[(df_up[non_match_cols].isnull().sum(axis=1) / len(
        allcols) * 100) == 100].tolist()).reset_index().drop('index', axis=1)
    df_up = df_up.loc[:, ~df_up.columns.duplicated()]
    # noinspection SpellCheckingInspection
    numericcols1 = []
    # noinspection SpellCheckingInspection
    clist = list(df_up.columns)
    # noinspection SpellCheckingInspection
    numericcols = list(set(clist) & set(numericcols1))
    for i in range(0, len(numericcols)):
        cname = numericcols[i]
        if df_up[cname].dtypes == "object":
            df_up[cname] = df_up[cname].astype(str).str.replace(" ", "").replace(",", "")
    df_up.dropna(how='all')
    print(df_up)
    df_up.to_csv('C:\\Users\\GopichandBarri\\exch_data.csv', sep=',', index=False)
    # logger.info("".join(["[", fn_name_from_inspect, "]", " : Save data to the file storage"]))
    # if ENCRYPTED:
    #     encrypt_decrypt.encrypt(pathwithfile, KEY)
    #     logger.info("".join(["[", fn_name_from_inspect, "]", " : File Encrypted successfully"]))
else:
    m1_up = {}
    # noinspection SpellCheckingInspection
    colnames = list(data.columns.values)
    for i in range(0, len(colnames)):
        if colnames[i] == colnames[i]:
            oldname = re.sub(" ", "", str(colnames[i]))
            if len(oldname) == 1:
                if oldname == "%":
                    new_colname = "Percent"
                elif oldname == "$":
                    new_colname = "Dollar"
                else:
                    new_colname = oldname
            else:
                new_colname = re.sub(r'\W', '', str(colnames[i]))
            m_up = {colnames[i]: new_colname}
            m1_up = {**m1_up, **m_up}
        else:
            new_colname = "Unknown" + str(i)
            m_up = {colnames[i]: new_colname}
            m1_up = {**m1_up, **m_up}
    data.rename(columns=m1_up, inplace=True)
    # noinspection SpellCheckingInspection
    emptycols = list(
        data.loc[:, list((100 * (data.isnull().sum() / len(data.index)) == 100))].columns)
    # noinspection SpellCheckingInspection
    allcols = list(data.columns)
    non_match_cols = list(set(allcols) - set(emptycols))
    df_up = data.drop(data.index[(data[non_match_cols].isnull().sum(axis=1) / len(
        allcols) * 100) == 100].tolist()).reset_index().drop('index', axis=1)
    df_up = df_up.loc[:, ~df_up.columns.duplicated()]
    # noinspection SpellCheckingInspection
    numericcols1 = []
    # noinspection SpellCheckingInspection
    clist = list(df_up.columns)
    # noinspection SpellCheckingInspection
    numericcols = list(set(clist) & set(numericcols1))
    for i in range(0, len(numericcols)):
        cname = numericcols[i]
        if df_up[cname].dtypes == "object":
            df_up[cname] = df_up[cname].astype(str).str.replace(" ", "").replace(",", "")
    df_up.dropna(how='all')
print(df_up)

