#https://www.amfiindia.com/spages/NAVAll.txt
#import code_parser as cp
import requests
import pymongo




scheme_info = {}
link = "https://www.amfiindia.com/spages/NAVAll.txt"
response = requests.session().get(link)
data = response.text.split("\n")

for scheme_data in data:
        if ";INF" in scheme_data:
            scheme = scheme_data.split(";")
            scheme_info[scheme[0]]=scheme[3]



