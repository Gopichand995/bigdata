import json

with open("C:/Users/GopichandBarri/Documents/Github/JCCC-UI-ATOMATION/Utilities/report.json", 'r') as f:
    data = json.load(f)

for i in range(len(data["advisor"])):
    for j in range(len((data["advisor"][i]["report"]))):
        data["advisor"][i]["report"][j]["expected_result"] = data["advisor"][i]["report"][j][
            "actual_result_with_status"].get("pass").replace(" successfully", "")

for i in range(len(data["journal"])):
    for j in range(len((data["journal"][i]["report"]))):
        data["journal"][i]["report"][j]["expected_result"] = data["journal"][i]["report"][j][
            "actual_result_with_status"].get("pass").replace(" successfully", "")


print(json.dumps(data["advisor"]))
print(json.dumps(data["journal"]))
