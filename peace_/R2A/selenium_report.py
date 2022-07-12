import pandas as pd

df = pd.read_csv("C:\\Users\\GopichandBarri\\Desktop\\auto.csv", header=None)
df[[1, 2, 3, 4]] = df[0].str.split('|', expand=True)
df1 = df.iloc[:, [3, 4]]
df2 = pd.DataFrame.transpose(df1)
df2.to_csv("C:\\Users\\GopichandBarri\\Desktop\\auto_new.csv", header=None, index=None)
df3 = pd.read_csv("C:\\Users\\GopichandBarri\\Desktop\\auto_new.csv")
df3 = df3.loc[:, ~df3.columns.str.contains('Unnamed')]
print(df3)
# n = 4
# df4 = pd.DataFrame()
# for i in range(int(len(df3.columns) / n)):
#     df5 = df3.iloc[:, 4 * i:4 * (i + 1)]
#     df5.columns = ["Step#", "Expected Results", "Step Description", "Actual Results"]
#     df4 = pd.concat([df4, df5], ignore_index=True)
# df4 = df4[["Step#", "Step Description", "Expected Results", "Actual Results"]]
# df4.to_csv("C:\\Users\\GopichandBarri\\Desktop\\auto_new_new.csv", index=None)

