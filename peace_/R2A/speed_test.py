import time
import pandas as pd

data = pd.read_csv("data\\df_result.csv")

print(data.head())

data[['Group with element(1)', 'Group with element(2)']] = data.grpelestr.str.split("--", expand=True)
string1 = "country-BAN"
string2 = "currency-INR"
print(data.loc[:, ['Group with element(1)', 'Group with element(2)']])
