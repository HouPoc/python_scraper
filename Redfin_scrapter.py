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
   

USER_AGENTS = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0",
                "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
                "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
                "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
                "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
                "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
                "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
                "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
                "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5"]
 

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


ip_pool = [('108.187.168.148:8000','8e6n1Q','vbuL5E'),('108.187.189.93:8000', '99RRyg','J2uwe6'),('108.187.168.162:8000', 'pAks5P','tRDHT7'),
            ('170.83.232.182:8000','xRkEWV','U4tGNH'),('170.83.235.243:8000','EtXLcY','WUnbJb'),('170.83.235.18:8000', 'tUVaXU','ymty19')]



def err_log(err):
    fo = open('exception.log', 'a', encoding='utf-8')
    fo.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + err + '\n')
    fo.close() 



def get_home_data(url, error_cap = 2):
    using_local_ip = False
    home_data = dict()
    processing = True
    err_num = 0
    proxy = None
    proxy_ip = None
    auth = None
    while processing:
        try:
            start = time.time()
            if not using_local_ip:
                proxy_ip = random.choice(ip_pool)
                auth = HTTPProxyAuth(proxy_ip[1], proxy_ip[2])
                proxy = {'https': 'https://' + proxy_ip[0]}
            header['user-agent'] = random.choice(USER_AGENTS)
            response = requests.get(url,headers = header, proxies = proxy,timeout = 2, auth=auth)
            response.raise_for_status()
            print(time.time() - start)
            start = time.time()
            bs = BeautifulSoup(response.text, "html.parser")
            # home address
            home_data['stree-address'] = bs.find('span', {'class': 'street-address'}).get_text()
            
            # home zip code
            home_data['postal-code'] = bs.find('span', {'class': 'postal-code'}).get_text()
            
            
            # home county
            home_data['community'] = None
            if bs.find('span', text='Community'):
                home_data['community'] = bs.find('span', text='Community').find_next('span').get_text()


            # info block result
            info_block = bs.find_all('div', {'class': 'info-block'})
            
            #   home redfin price
            home_data['redfin_estimate_price'] = info_block[0].find('div', {'class' : 'statsValue'}).get_text()
            #   home sold price
            home_data['sold_price'] = info_block[1].find('div', {'class' : 'statsValue'}).get_text()
            #   home beds
            home_data['beds'] = info_block[2].find('div', {'class' : 'statsValue'}).get_text()
            #   home baths
            home_data['baths'] = info_block[3].find('div', {'class' : 'statsValue'}).get_text()
            #   home sqrt
            home_data['sqrt'] = info_block[4].find('span', {'class' : 'statsValue'}).get_text()

            # sold date
            home_data['sold_date'] = bs.find('td', {'class': 'date-col'}).get_text()
            
            # built year
            more_info_block = bs.find('div', {'class': 'more-info'})
            home_data['built_year'] = more_info_block.find('span', {'class': 'value'}).get_text()

            # home tax
            tax = bs.find('div', {'class': 'tax-record'}).find('td', {'class': 'value'})
            home_data['tax'] = tax.get_text()

            # home hoa
            home_data['hoa'] = '0'
            if bs.find('span', text = 'HOA Dues'):
                home_data['hoa'] = bs.find('span', text = 'HOA Dues').find_next('div').get_text()
            
            facts = bs.find('div', {'class': 'facts-table'})
           
            # home style 
            home_data['style'] = facts.find('span', text='Style').find_next('div').get_text()

            # home story
            home_data['stories'] = '1'
            if facts.find('span', text='Stories').find_next('div'):
                home_data['stories'] = facts.find('span', text='Stories').find_next('div').get_text()
            
            # home lot
            home_data['lot'] = 0
            if facts.find('span', text='Lot Size'):
                home_data['lot'] = facts.find('span', text='Lot Size').find_next('div').get_text()
            
            # home community
            home_data['county'] = None
            if facts.find('span', text='County'):
                home_data['county'] = facts.find('span', text='County').find_next('div').get_text()

            # near schools --- to be continue
    
            home_data['school_names'] = []
            home_data['school_ratings'] = []
            home_data['school_distances'] = []
            for name, gs_rating, distance in zip(bs.find_all('div', 'school-name'), bs.find_all('td', 'gs-rating-col'), bs.find_all('td', 'distance-col')):
                home_data['school_names'].append(name.get_text())
                home_data['school_ratings'].append(gs_rating.find('div').get_text())
                home_data['school_distances'].append(distance.find('a').get_text())

            # walk score
            walk_score_div = bs.find('div', {'class': 'walkscore'})
            home_data['walkscore'] = walk_score_div.find('span', {'class': 'value'}).get_text()

            # transit score
            transit_score_div = bs.find('div', {'class': 'transitscore'})
            home_data['transitscore'] = transit_score_div.find('span', {'class': 'value'}).get_text()

            # bike score
            bike_score_div = bs.find('div', {'class': 'bikescore'})
            home_data['bikescore'] = bike_score_div.find('span', {'class': 'value'}).get_text()
            print(time.time() - start)
            processing = False

        #  ------------handle http errors --------- #
        except requests.exceptions.HTTPError as err_http:
            if err_http.response.status_code == 403 or err_http.response.status_code == 503:
                # case: proxy IP is not avaliable 
        
        #  ------------handle other forms of errors --------- #
        except requests.exceptions.RequestException as err:
            # for other errors, try second time with a sleep time
            err_num += 1
            print("General Error:", err)
            err_log(":: General Error " + ":: " + str(err) +":: "+ " - get_home_data - "  + url)
            if err_num >=error_cap:
                err_num = 0
                processing = False
            time.sleep(random.choice(range(2,5)))
    return home_data

def redfin_scrapter(city_link_file, state,output_file = None, error_cap = 2):
    #proxy ={'https': 'https://' + random.choice(ip_pool)} 
    #print(proxy)
    url = 'https://www.redfin.com'
    with open(city_link_file) as cities_link:
        links = json.load(cities_link)
    for link in links[state]:
        err_num = 0
        processing = True
        while processing:
            try:
                header['user-agent'] = random.choice(USER_AGENTS)
                test = 'https://www.redfin.com/city/17151/CA/San-Francisco/recently-sold'
                #response = requests.get(url + link + '/recently-sold', headers = header, proxies = proxy,timeout = 2)
                response = requests.get(test, headers = header, proxies = proxy,timeout = 2)
                response.raise_for_status()
                bs = BeautifulSoup(response.text, "html.parser")
                # there could be multiple pages of recent-sold houses for a given city
                # find the last page as the stop point
                pageNum = bs.find_all('a', {'class': 'goToPage'})
                #print (pageNum)
               
                if not pageNum:
                    page_indices = 0
                else:
                    page_indices = pageNum[-1].get_text()
                #time.sleep(random.choice(range(2,5)))
                home_links = bs.find_all('a', {'class': 'cover-all'})
                print (page_indices)
                #break
                if home_links:
                    for home_link in home_links:
                        home_route = home_link['href']
                        print(home_route)
                        home_data = get_home_data(url+home_route)
                        time.sleep(random.choice(range(2,5)))
                    for page_index in range(2, int(page_indices)):
                        print(page_index)
                        err_num = 0
                        processing_i = True
                        while processing_i:
                            try:
                                print (test + '/page-' + str(page_index))
                                response = requests.get(test + '/page-' + str(page_index), proxies = proxy,headers = header, timeout = 2)
                                bs = BeautifulSoup(response.text, "html.parser")
                                home_links = bs.find_all('a', {'class': 'cover-all'})
                                for home_link in home_links:
                                    home_route = home_link['href']
                                    print(home_route)
                                    home_data = get_home_data(url+home_route)
                                    print (home_data)
                                break
                            except requests.exceptions.RequestException as err:
                                err_num_i += 1
                                print("General Error:", err)
                                err_log(":: General Error " + ":: " + str(err) +":: "+ " - get_home_data - "  + url)
                                if err_num_i >=error_cap:
                                    err_num = 0
                                    print(err)
                                    processing_i = False
                                time.sleep(random.choice(range(2,5)))
                            
                        time.sleep(random.choice(range(1,2)))
                break
            except requests.exceptions.HTTPError as err_http:

                print ("Http Error:",err_http)
                err_log(":: Http Error " + ":: " + " - redfin_scrapter - " + link)
                time.sleep(random.choice(range(2,5)))
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
                time.sleep(random.choice(range(2,5)))
            except requests.exceptions.RequestException as err:
                err_num += 1
                print("General Error:", err)
                err_log(":: General Error " + ":: " + " - redfin_scrapter - "  + link)
                if err_num >=error_cap:
                    err_num = 0
                    print(err)
                    processing = False
                time.sleep(random.choice(range(2,5)))
        time.sleep(random.choice(range(2,5)))
        break


        
        
            


if __name__ == "__main__":

    home_data = get_home_data("https://www.redfin.com/CA/San-Francisco/201-Harrison-St-94105/unit-310/home/726549")
    print(home_data)
    #city = redfin_scrapter('AL.json', 'AL')