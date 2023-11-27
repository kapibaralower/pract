import requests
import json
import pandas as pd
import boto3


url_d = "https://bank.gov.ua/NBU_Exchange/exchange_site?start=20220101&end=20221231&valcode=usd&sort=exchangedate&order=desc&json"
url_e = "https://bank.gov.ua/NBU_Exchange/exchange_site?start=20220101&end=20221231&valcode=eur&sort=exchangedate&order=desc&json"

response_d = requests.get(url_d)
response_e = requests.get(url_e)

if response_d.status_code == 200:
    d_json = json.loads(response_d.text)
else:
    print(f"Error: {response_d.status_code}")

if response_e.status_code == 200:
    e_json = json.loads(response_e.text)
else:
    print(f"Error: {response_e.status_code}")

avg = []

month = 12

i = 0

while i < len(d_json)-1:
    month_d = []
    month_e = []

    while int(d_json[i]["exchangedate"][3] + d_json[i]["exchangedate"][4]) == month:
        month_d.append(d_json[i]["rate"])
        month_e.append(e_json[i]["rate"])
        if month == 1 and int(d_json[i]["exchangedate"][0] + d_json[i]["exchangedate"][1]) == 1:
            break
        i += 1
        
    avg.insert(0, {"month":month, "cc":"USD/EUR", "avg":f"{round(sum(month_d) / len(month_d), 4)}/{round(sum(month_e) / len(month_e), 4)}"})
    
    month -= 1

with open('avg_rate.json', 'w') as output_file:
    json.dump(avg, output_file)

with open('avg_rate.json', encoding='utf-8') as inputfile:
    df = pd.read_json(inputfile)

df.to_csv('avg_rate.csv', encoding='utf-8', index=False)

s3 = boto3.client('s3')

s3.upload_file('avg_rate.csv', 'store0512', 'avg_rate.csv')
