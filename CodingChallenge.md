# Coding Challenge 

#### Objectives
 *  Python scraper to scrape _**recently-sold-house**_ real estate information from [Redfin.com](www.redfin.com).
 	* Written in Python 3.
 	* Scrape data that is useful and meaningful.
 	* Set proper rest time to prevent scraper being detected and banned.
 	* Use less run-time memory.
 	* Make the program as efficient as possible.
 	* Format data for uploading to database in future.
 * Dockerize python scraper
 	* Use small-size base image.
 	* Output final result to csv file.

#### Design
 * **Docker**
 	* Dockerfile to set up `python 3.6` environment
 		* Requested libraries: `requests` , `BeautifulSoup4`, and,`pandas`.
 		* `CMD ["python3", "-u", main.py"]` to redirect message to `stdout`.
 	* **Mount a volume** to container folder
 		* `root@localhost: docker run  -v /home/LoftyCode/InternResults/:/container -t xxx`**
 * **Scraper V1** 	 
 	* First, use python library `requests` to get html content from [Redfin.com](www.redfin.com)
 		* Do not use `selenium` because it requires web browser kernels, which increases docker image size.  	
 		* To prevent getting caught, set rest time after getting certain amount results.
 		* Use proxy IP rotation to get html content to balance loads and hide local host. 
 			* `proxy` makes the requests slower  	  
 	* Then, use python library `BeautifulSoup4` to parse html content and select data.
 		* Content may various, check if tag is exists first before query it. 	
 	* Finally, store data into `pandas` data frame and save it to csv file periodically.
 	* URL Template:
 		*  recently sold page: **www.redfin.com/city/:city_code/:state/:city/recently-sold/:page_num**
 			* city_code can be scraped from [Redfin.com](www.redfin.com)
 			* max_page_num can be scraped from recently sold page
 		* home info page: **www.redfin.com/:state/:city/:home_address/home/:home_id** 
 		   * link to home info can be scraped from recently sold page   
   * Selected info:
     * `Street Address`, `City`, `State`, `Postal Code`, `Community`, `County`
     	* Explore how geographical information affects the home price and the trading volume
   	  * `# of bed`, `# of bath`, `# of sqrt`, `Built Year`, `Lot Size`, `# of storis`, `Home Style`
   	  	 * Explore how home features affect home price and trading volumes 
   	  * `Sold Price`, `Sold Date`,`Redfin Estimated Price`, `$ per sqrt`, `Tax`
   	    * Explore how date, price and tax affect the trading volume
   	  * `Near Schools`, `Near School Ratings`, `Near School Distances`
   	  	 * Explore how educational resources affects the home price and the trading volume 
   	  * `Walk Score`, `Transit Score`, `Bike Score`
   	    * Explore how transit condition affects the home price and the trading volume 
 * **Scraper V1 Pseudo-code**
   ```python
   def scraper(state):
      	for city in citys[state]:
          proxy = select_not_blacklisted_proxy_ip(ip_pool)
          if local_ip_blacklisted and proxy == None:
             save_data_to_csv(data_frame)
             return -1
          response = requests.get(Redfin.com/city/recently-sold, proxy, timeout, random_header)
          for house_link in response.text:
             proxy = select_not_blacklisted_proxy_ip(ip_pool)
             if local_ip_blacklisted and proxy == None:
                save_data_to_csv(data_frame)
                return -1
             response = request.get(house_link, proxy, timeout, random_header)
             house_data = select_data_bs4(response.text)
             data_frame.add(house_data)
             take_medium_break_every_20_results(random(2, 5))
             take_medium_break_every_100_results(random(10, 30))
           data_frame.to_csv(state_city_code.csv)
           take_large_break_each_city(random(20, 60))  
     def main():
       	for state in states:
        	sraper(state)
            
   ```
  #### Scraper Facts
   * Docker image size: 117 MB
   * Time for each home with proxy: ~avg 2s
   * Time for each home without proxy: ~avg 1s
   * Output each home at a time to `stdout`
   * CSV files are stored as `/container/scraped_result/{state}_{city}.csv` 
#### Possible future work
   * Solve reCAPTCHA to unblock proxy IP or local IP




 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 