import requests
import csv
import re
import random
import time
import json
import pandas as pd
from datetime import datetime
from collections import defaultdict
from bs4 import BeautifulSoup
   

header = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6',
    'cache-control': 'no-cache',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
}

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

def err_log(err):
    fo = open('exception.log', 'a', encoding='utf-8')
    fo.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + err + '\n')
    fo.close() 
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

def get_home_data(url, error_cap = 2):
    home_data = dict()
    processing = True
    err_num = 0
    while processing:
        try:
            response = requests.get(url, header = header, timeouut = 3)
            response.raise_for_status()
            bs = BeautifulSoup(respone.text, "html.parser")
            # home price
            price_block = bs.select('div.info-block.price')
            price = price_block.find('div', {'class', 'statsValue'})
            home_data['price'] = price.get_text() 

            # price per sqrt
            price_per_sqrt_block = bs.select('div.info-block.sqft')
            price_per_sqrt = price_per_sqrt_block.find('div', {'class', 'statsLabel'})
            home_data['price_per_sqrt'] = price_per_sqrt.get_text()
            
            # home address
            home_data['stree-address'] = bs.find('span', {'class': 'street-class'})
            
            # home zip code
            home_data['postal-code'] = bs.find('span', {'class': 'posital-code'})
            fact_labels = bs.find_all('span', {'class': 'table-label'})
            fact_values = bs.find_all('div', {'class': 'table-value'})
            
            # home
            
            # home tax
            tax = bs.find('td', {'class': 'value'})
            home_data['tax'] = tax.get_text()
            
            # near school names
            school_names_divs = bs.find_all('div', {'class': 'school-name'})
            home_data['elementary'] = school_names_divs[0].get_text()
            home_data['middle'] = school_names_divs[1].get_text() 
            home_data['high'] = school_names_divs[2].get_text() 

            # near school ratings
            school_rating_tds = bs.find_all('td', {'class': 'gs-rating-col'})
            home_data['elementary-rating'] = school_rating_tds[0].find('div', {'class': 'rating'}).get_text()
            home_data['middle-rating'] = school_rating_tds[1].find('div', {'class': 'rating'}).get_text()
            home_data['high-rating'] = school_rating_tds[2].find('div', {'class': 'rating'}).get_text()
            
            # near shcool distances
            school_distance_tds = bs.find_all('td', {'class': 'distance-col'})
            home_data['elementary-distance'] = school_distance_tds[0].find('a').get_text()
            home_data['middle-distance'] = school_distance_tds[1].find('a').get_text()
            home_data['middle-distance'] = school_distance_tds[2].find('a').get_text()

            # walk score
            walk_score_div = bs.find('div', {'class': 'walkscore'})
            home_data['walkscore'] = walk_score_div.find('span', {'class': 'value'}).get_text()

            # transit score
            transit_score_div = bs.find('div', {'class': 'transitscore'})
            home_data['transitscore'] = transit_score_div.find('span', {'class': 'value'}).get_text()

            # bike score
            bike_score_div = bs.find('div', {'class': 'bikescore'})
            home_data['bikescore'] = bike_score_div.find('span', {'class': 'value'}).get_text()




        except requests.exceptions.RequestException as err:
            err_num += 1
            print("General Error:", err)
            err_log(":: General Error " + ":: " + " - get_home_data - "  + url)
            if err_num >=error_cap:
                err_num = 0
                print(err)
                processing = False
    return 

def redfin_scrapter(city_link_file, state,output_file = None, error_cap = 2):
    url = 'www.redfin.com'
    with open(city_link_file) as cities_link:
        links = json.load(cities_link)
    for link in cities_link[state]:
        err_num = 0
        processing = True
        while processing:
            try:
                response = requests.get(url + link + '/recently-sold', header = header, timout = 2)
                response.raise_for_status()
                bs = BeautifulSoup(respone.text, "html.parser")
                # there could be multiple pages of recent-sold houses for a given city
                # find the last page as the stop point
                pageNum = bs.find_all('a', href=re.compile(r"^/recently-sold"))[-1].get_text()
                for page_index in range(2, int(pageNum)+1):
                    response = requests.get(url + link + '/recently-sold' + '/page-' + str(page_index), header = header, timout = 2)
                    bs = BeautifulSoup(response.text, "html_parser")
                    home_links = bs.find_all('a', {'class': 'cover-all'})
                    for home_link in home_links:
                        home_route = home_link['href']
                        home_data = get_home_data(url+home_route)
                break 
            except requests.exceptions.HTTPError as err_http:
                    print ("Http Error:",err_http)
                    err_log(":: Http Error " + ":: " + " - redfin_scrapter - " + link)
                    break
                except requests.exceptions.ConnectionError as err_connection:
                    print ("Error Connecting:",err_connection)
                    err_log(":: Connection Error " + ":: " +" - redfin_scrapter - "  + link)
                    return -1
                except requests.exceptions.Timeout as err_timeout:
                    err_num += 1
                    print ("Timeout Error:",err_timeout)
                    err_log(":: Timeout Error " + ":: " + " - redfin_scrapter - " + link)
                    if err_num >= error_cap:
                        err_num = 0
                        processing = False 
                    time.sleep(random.choice(range(2,10)))
                except requests.exceptions.RequestException as err:
                    err_num += 1
                    print("General Error:", err)
                    err_log(":: General Error " + ":: " + " - redfin_scrapter - "  + link)
                    if err_num >=error_cap:
                        err_num = 0
                        print(err)
                        processing = False
                    time.sleep(random.choice(range(2,10)))