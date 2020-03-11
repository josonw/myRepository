# coding:utf-8
import requests

url = "http://localhost:26810/new/business/reload/mongo/?time=day"
res = requests.get(url)
print res.content