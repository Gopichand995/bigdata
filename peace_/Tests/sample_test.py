import requests
import json

BASE_URL = "https://reqres.in"
data = {
    "name": "morpheus",
    "job": "leader"
}
response = requests.post(BASE_URL + "/api/users", data=data)
print(json.dumps(response.json(), indent=5))
