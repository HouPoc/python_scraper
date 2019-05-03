import requests
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

states = ["AL", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "OH", "OK", "OR", "PA", "RI", "SC", 
          "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI"]


ip_pool = [('108.187.168.148:8000','8e6n1Q','vbuL5E'),('108.187.189.93:8000', '99RRyg','J2uwe6'),('108.187.168.162:8000', 'pAks5P','tRDHT7'),
            ('170.83.232.182:8000','xRkEWV','U4tGNH'),('170.83.235.243:8000','EtXLcY','WUnbJb'),('170.83.235.18:8000', 'tUVaXU','ymty19'),
            ('108.187.189.118:8000','yJwbd3','BcJb08'),('192.238.218.180:8000','yJwbd3','BcJb08'),('108.187.189.251:8000','95vVcC','r45sg4'),
            ('192.238.218.62:8000','95vVcC','r45sg4'),('108.187.189.89:8000','95vVcC','r45sg4'), ('108.187.168.114:8000','HBazYY','195oLD'),
            ('192.238.218.32:8000','HBazYY','195oLD'), ('108.187.204.43:8000','nf3kBg','XqopCH'), ('108.187.204.219:8000','nf3kBg','XqopCH'), ('108.187.168.37:8000','nf3kBg','XqopCH'),
            ('107.178.130.209:8000','R41pL3','PZcL04'), ('23.249.187.154:8000','R41pL3','PZcL04'), ('107.178.128.172:8000','R41pL3','PZcL04'), ('107.178.131.103:8000','R41pL3','PZcL04')]
proxy = None
proxy_ip = None
TIME_OUT = -3
LOCAL_IP_BLACKLISTED = -2
RESOURCE_NOT_AVALIABLE = -1
SUCCESS = 0

def err_log(err):
    fo = open('exception.log', 'a', encoding='utf-8')
    fo.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + err + '\n')
    fo.close() 



def get_home_data(url, error_cap = 2):
    global proxy
    global proxy_ip
    home_data = dict()
    processing = True
    err_num = 0
    while processing:
        try:
            #proxy_ip = None
            #proxy = None
            #if ip_pool:
            #    proxy_ip = random.choice(ip_pool)
            #    proxy = {'https': 'https://'+proxy_ip[1] + ':' +proxy_ip[2]+ '@' + proxy_ip[0]}
            header['user-agent'] = random.choice(USER_AGENTS)
            response = requests.get(url,headers = header, proxies = proxy ,timeout = 2)
            response.raise_for_status()
            bs = BeautifulSoup(response.text, "html.parser")
            # home address
            home_data['stree-address'] = bs.find('span', {'class': 'street-address'}).get_text()
            
            # home zip code
            home_data['postal-code'] = bs.find('span', {'class': 'postal-code'}).get_text()
            
            
            # home county
            home_data['community'] = '-'
            if bs.find('span', text='Community'):
                home_data['community'] = bs.find('span', text='Community').find_next('span').get_text()


            # info block result
            info_block = bs.find_all('div', {'class': 'info-block'})
            l_info_block = len(info_block)
            #   home redfin price
            home_data['redfin_estimate_price'] = '-'
            if  l_info_block >= 1 and info_block[0].find('div', {'class' : 'statsValue'}):
                home_data['redfin_estimate_price'] = info_block[0].find('div', {'class' : 'statsValue'}).get_text()
            #   home sold price
            home_data['sold_price'] = '-'
            if l_info_block >= 2 and info_block[1].find('div', {'class' : 'statsValue'}):
                home_data['sold_price'] = info_block[1].find('div', {'class' : 'statsValue'}).get_text()
            #   home beds
            home_data['beds'] = '-'
            if l_info_block >= 3 and info_block[2].find('div', {'class' : 'statsValue'}):
                home_data['beds'] = info_block[2].find('div', {'class' : 'statsValue'}).get_text()
            #   home baths
            home_data['baths'] = '-'
            if l_info_block >= 4 and info_block[3].find('div', {'class' : 'statsValue'}):
                home_data['baths'] = info_block[3].find('div', {'class' : 'statsValue'}).get_text()
            #   home sqrt
            home_data['sqrt'] = '-'
            if l_info_block >= 5 and info_block[4].find('span', {'class' : 'statsValue'}):
                home_data['sqrt'] = info_block[4].find('span', {'class' : 'statsValue'}).get_text()

            # sold date
            home_data['sold_date'] = '-'
            if bs.find('td', {'class': 'date-col'}):
                home_data['sold_date'] = bs.find('td', {'class': 'date-col'}).get_text()
            
            # built year
            more_info_block = bs.find('div', {'class': 'more-info'})
            home_data['built_year'] = '-'
            if more_info_block and  more_info_block.find('span', {'class': 'value'}):
                home_data['built_year'] = more_info_block.find('span', {'class': 'value'}).get_text()

            # home tax
            tax = bs.find('div', {'class': 'tax-record'})
            home_data['tax'] = '-'
            if tax:
                home_data['tax'] = tax.find('td', {'class': 'value'}).get_text()
            
            facts = bs.find('div', {'class': 'facts-table'})
            home_data['style'] = '-'
            home_data['stories'] = '-'
            home_data['lot'] = '-'
            home_data['county'] = '-'
            if facts:
                # home style
                if facts.find('span', text='Style').find_next('div'):
                    home_data['style'] = facts.find('span', text='Style').find_next('div').get_text()

                # home story
                home_data['stories'] = '1'
                if facts.find('span', text='Stories').find_next('div'):
                    home_data['stories'] = facts.find('span', text='Stories').find_next('div').get_text()
                
                # home lot
                home_data['lot'] = '-1'
                if facts.find('span', text='Lot Size'):
                    home_data['lot'] = facts.find('span', text='Lot Size').find_next('div').get_text()
                
                # home community
                home_data['county'] = '-'
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
            home_data['walkscore'] = '-1'
            if walk_score_div:
                home_data['walkscore'] = walk_score_div.find('span', {'class': 'value'}).get_text()

            # transit score
            transit_score_div = bs.find('div', {'class': 'transitscore'})
            home_data['transitscore'] = '-1'
            if transit_score_div:
                home_data['transitscore'] = transit_score_div.find('span', {'class': 'value'}).get_text()

            # bike score
            bike_score_div = bs.find('div', {'class': 'bikescore'})
            home_data['bikescore'] = '-1'
            if bike_score_div:
                home_data['bikescore'] = bike_score_div.find('span', {'class': 'value'}).get_text()
            return (SUCCESS, home_data)

        #  ------------handle http errors --------- #
        except requests.exceptions.HTTPError as err_http:
            if err_http.response.status_code == 404:
                return (RESOURCE_NOT_AVALIABLE, [])
            if len(ip_pool):
                print('one proxy IP blacklisted, sleeping and then switch to another proxy IP....')
                time.sleep(random.choice(range(1, 5)))
                ip_pool.remove(proxy_ip)
                if len(ip_pool):
                        proxy_ip = random.choice(ip_pool)
                        proxy = {'https': 'https://'+proxy_ip[1] + ':' +proxy_ip[2]+ '@' + proxy_ip[0]}
                else:
                    proxy_ip = None
                    proxy = None
            else:
                proxy = None
                if err_http.response.status_code == 403:
                    return (LOCAL_IP_BLACKLISTED, [])

        #  ------------handle other forms of errors --------- #
        except requests.exceptions.Timeout as err_timeout:
                err_num += 1
                print ("Timeout Error:",err_timeout)
                err_log(":: Timeout Error " + ":: " + " - get_citget_county_codey_code - " + state)
                if err_num >= error_cap:
                    err_num = 0
                    processing = False 
                    return (TIME_OUT, [])
                print ('time out, sleeping.......')    
                time.sleep(random.choice(range(5,10)))
        except requests.exceptions.RequestException as err:
            # for other errors, try second time with a sleep time
            err_num += 1
            print("General Error:", err)
            err_log(":: General Error " + ":: " + str(err) +":: "+ " - get_home_data - "  + url)
            if err_num >=error_cap:
                err_num = 0
                processing = False
                return (RESOURCE_NOT_AVALIABLE, [])
            
        time.sleep(random.choice(range(2,5)))
    

def redfin_scrapter(city_link_file, state,output_file = None, error_cap = 2):
    global proxy
    global proxy_ip
    url = 'https://www.redfin.com'
    if ip_pool:
        proxy_ip = random.choice(ip_pool)
        proxy = {'https': 'https://'+proxy_ip[1] + ':' +proxy_ip[2]+ '@' + proxy_ip[0]}
    with open(city_link_file) as cities_link:
        links = json.load(cities_link)
    for link in links[state]:
        l = link.split('/')
        csv_file = './scrape_result/' + state + '_' + l[-1] +'.csv'
        df = pd.DataFrame()
        err_num = 0
        processing = True
        while processing:
            try:
                header['user-agent'] = random.choice(USER_AGENTS)
                response = requests.get(url+link+'/recently-sold', headers = header,  proxies = proxy, timeout = 2)
                response.raise_for_status()
                bs = BeautifulSoup(response.text, "html.parser")
                pageNum = bs.find_all('a', {'class': 'goToPage'})
                if not pageNum:
                    page_indices = 0
                else:
                    page_indices = pageNum[-1].get_text()
                home_links = bs.find_all('a', {'class': 'cover-all'})

                if home_links:
                    for home_link in home_links:
                        home_route = home_link['href']
                        home_data = get_home_data(url+home_route)
                        if home_data[0] == SUCCESS:
                            home_data[1]['state'] = state
                            home_data[1]['city'] = l[-1]
                            print(home_data[1])
                            df = df.append(home_data[1], ignore_index=True)
                    time.sleep(random.choice(range(2,5)))
                    for page_index in range(2, int(page_indices)):
                        err_num_i = 0
                        processing_i = True
                        while processing_i:
                            try:
                                header['user-agent'] = random.choice(USER_AGENTS)
                                response = requests.get(url+link+'/recently-sold' + '/page-' + str(page_index), proxies = proxy, headers = header, timeout = 2)
                                bs = BeautifulSoup(response.text, "html.parser")
                                home_links = bs.find_all('a', {'class': 'cover-all'})
                                for home_link in home_links:
                                    home_route = home_link['href']
                                    home_data = get_home_data(url+home_route)
                                    if home_data[0] == SUCCESS:
                                        home_data[1]['state'] = state
                                        home_data[1]['city'] = l[-1]
                                        print(home_data[1])
                                        df = df.append(home_data[1], ignore_index=True)

                                break
                            except requests.exceptions.HTTPError as err_http:
                                if err_http.response.status_code == 404:
                                    processing_i = False
                                if len(ip_pool):
                                   
                                    print('one proxy IP blacklisted, sleeping and then switch to another proxy IP....')
                                    time.sleep(random.choice(range(1, 5)))
                                    ip_pool.remove(proxy_ip)
                                    if len(ip_pool):
                                        proxy_ip = random.choice(ip_pool)
                                        proxy = {'https': 'https://'+proxy_ip[1] + ':' +proxy_ip[2]+ '@' + proxy_ip[0]}
                                    else:
                                        proxy_ip = None
                                        proxy = None
                                else:
                                    proxy = None
                                    if err_http.response.status_code == 403:
                                        df.to_csv(csv_file, index = True)
                                        return LOCAL_IP_BLACKLISTED
                                    else:
                                        print('No proxy IP avaibale, using local IP.') 
                            except requests.exceptions.Timeout as err_timeout:
                                    err_num += 1
                                    print ("Timeout Error:",err_timeout)
                                    if err_num >= error_cap:
                                        err_num = 0
                                        processing = False 
                                    time.sleep(random.choice(range(5,10)))
                            except requests.exceptions.RequestException as err:
                                err_num_i += 1
                                print("General Error:", err)
                                err_log(":: General Error " + ":: " + str(err) +":: "+ " - get_home_data - "  + url)
                                if err_num_i >=error_cap:
                                    err_num_i = 0
                                    processing_i = False
                                time.sleep(random.choice(range(2,5)))

                        time.sleep(random.choice(range(2,5)))
                        if page_index % 5 == 0:
                            print('100 results --- sleeping....')
                            time.sleep(random.choice(range(10, 30)))
                        print('->next page')
                break
            except requests.exceptions.HTTPError as err_http:
                if err_http.response.status_code == 404:
                    processing_i = False
                if len(ip_pool):
                    print('one proxy IP blacklisted, sleeping and then switch to another proxy IP....')
                    time.sleep(random.choice(range(1, 5)))
                    ip_pool.remove(proxy_ip)
                    if len(ip_pool):
                        proxy_ip = random.choice(ip_pool)
                        proxy = {'https': 'https://'+proxy_ip[1] + ':' +proxy_ip[2]+ '@' + proxy_ip[0]}
                    else:
                        proxy_ip = None
                        proxy = None
                else:
                    proxy = None
                    proxy_ip = None
                    if err_http.response.status_code == 403:
                        df.to_csv(csv_file, index = True)
                        return LOCAL_IP_BLACKLISTED 
                    else:
                        print('No proxy IP avaibale, using local IP.') 
            except requests.exceptions.Timeout as err_timeout:
                err_num += 1
                print ("Timeout Error:",err_timeout)
                if err_num >= error_cap:
                    err_num = 0
                    processing = False 
                time.sleep(random.choice(range(5,10)))
            except requests.exceptions.RequestException as err:
                err_num += 1
                print("General Error:", err)
                err_log(":: General Error " + ":: " + " - redfin_scrapter - "  + link)
                if err_num >=error_cap:
                    err_num = 0
                    processing = False
                time.sleep(random.choice(range(1,3)))
        print('done one city, sleeping......')
        df.to_csv(csv_file, index = None)
        time.sleep(random.choice(range(20, 60)))
        print('start next city')
    return SUCCESS


            


if __name__ == "__main__":
    while states:
        state = random.choice(states)
        status = redfin_scrapter(state+'.json', state)
        if status == LOCAL_IP_BLACKLISTED:
            print('No proxy IP avaliable and the local ip is blacklisted now, stop scraping')
            break
        print('done one state, sleeping......')
        time.sleep(random.choice(range(20, 60)))
        print('start next state')
        states.remove(state)