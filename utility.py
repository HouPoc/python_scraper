import requests
from requests.auth import HTTPProxyAuth
import csv
import re
import random
import time
import json
import random
import os
import pandas as pd
from datetime import datetime
from collections import defaultdict
from bs4 import BeautifulSoup

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
'''
The base url to get recent-sold-houses on redfin.com requires a city code, like https://www.redfin.com/city/30772/OR/Portland/recently-sold. 
30772 is the city code for Portland.  

get_city_code() is the function to scraper correct city id from redfin.com 
'''
def get_county_code(error_cap = 2):
    err_num = 0
    url = 'https://www.redfin.com/sitemap'
    
    
    counties = defaultdict(list)

    for state in states:
        print(state)
        err_num = 0
        processing = True
        while processing:
            try:
                respone = requests.get(url+'/'+state, headers = header, timeout = 3)
                respone.raise_for_status()

                bs = BeautifulSoup(respone.text, "html.parser")
                raw_data = bs.body.find_all('a', href=re.compile(r"^/sitemap"))
                for a in raw_data:
                    if a.text:
                        counties[state].append(a['href'])
                break
            except requests.exceptions.HTTPError as err_http:
                print ("Http Error:",err_http)
                err_log(":: Http Error " + ":: " + " - get_county_code - " + state)
                break
            except requests.exceptions.ConnectionError as err_connection:
                print ("Error Connecting:",err_connection)
                err_log(":: Connection Error " + ":: " +" - get_county_code - "  + state)
                return -1
            except requests.exceptions.Timeout as err_timeout:
                err_num += 1
                print ("Timeout Error:",err_timeout)
                err_log(":: Timeout Error " + ":: " + " - get_citget_county_codey_code - " + state)
                if err_num >= error_cap:
                    err_num = 0
                    processing = False 
                time.sleep(random.choice(range(5,10)))
            except requests.exceptions.RequestException as err:
                err_num += 1
                print("General Error:", err)
                err_log(":: General Error " + ":: " + " - get_county_code - "  + state)
                if err_num >=error_cap:
                    err_num = 0
                    print(err)
                    processing = False 
                time.sleep(random.choice(range(8,15)))
        time.sleep(random.choice(range(0,2)))
    with open('counties.json', 'w') as fp:
        json.dump(counties, fp)

def get_city_code(counties,error_cap = 2):
    
    url = 'https://www.redfin.com/'
    with open (counties, 'r') as county_link:
        county_data = json.load(county_link)
    for k in county_data:
        print(k)
        cities = defaultdict(list)
        for link in county_data[k]:
            err_num = 0
            processing = True
            while processing:
                try:
                    respone = requests.get(url+'/'+link, headers = header, timeout = 3)
                    respone.raise_for_status()

                    bs = BeautifulSoup(respone.text, "html.parser")
                    raw_data = bs.body.find_all('a', href=re.compile(r"^/city"))
                    for a in raw_data:
                        if a.text:
                            cities[k].append(a['href'])
                    break
                    
                except requests.exceptions.HTTPError as err_http:
                    print ("Http Error:",err_http)
                    err_log(":: Http Error " + ":: " + " - get_city_code - " + link)
                    break
                except requests.exceptions.ConnectionError as err_connection:
                    print ("Error Connecting:",err_connection)
                    err_log(":: Connection Error " + ":: " +" - get_city_code - "  + link)
                    return -1
                except requests.exceptions.Timeout as err_timeout:
                    err_num += 1
                    print ("Timeout Error:",err_timeout)
                    err_log(":: Timeout Error " + ":: " + " - get_city_code - " + link)
                    if err_num >= error_cap:
                        err_num = 0
                        processing = False 
                    time.sleep(random.choice(range(5,10)))
                except requests.exceptions.RequestException as err:
                    err_num += 1
                    print("General Error:", err)
                    err_log(":: General Error " + ":: " + " - get_city_code - "  + link)
                    if err_num >=error_cap:
                        err_num = 0
                        print(err)
                        processing = False
        filename = k + '.json'
        with open(filename, 'w') as fp:
            json.dump(cities, fp)