#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 23 10:56:03 2019

@author: varunwalvekar

"""

from requests import get
from bs4 import BeautifulSoup
import re
import urllib.request, os, gzip, pandas as pd
from urllib.parse import urlparse
import shutil
import requests


def scrape_bnb():
    url = 'http://insideairbnb.com/get-the-data.html'
    response = get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    address=[]
    for link in soup.find_all('a', class_ = ''):
        address.append(link.get('href'))
    return address  


def get_unique_cities(allurls):
    def get_bnb_cities(allurls):
        listingsdata = []
        city_list = []
        for i in range(len(allurls)):
            link = allurls[i]
            if ('listings.csv' in link) and ('visualisations' not in link):
                listingsdata.append(link)
        for i in range(len(listingsdata)):
            link = listingsdata[i]
            link_list = link.split('/')
            city = link_list[-4]
            city_list.append(city)
        df = pd.DataFrame(city_list)
        uniq_cities = df[0].unique().tolist()
        city_lower =[]
        for idx,value in enumerate(uniq_cities):
            city_lower.append(value.lower())
        return city_lower
    def get_yelp_cities():
        df = pd.read_json('business.json', lines = True)
        city = df.city.unique().tolist()
        city_lower = []
        for idx,value in enumerate(city):
            city_lower.append(value.lower())
        return city_lower
    
    def get_unique_cities(bnbcities,yelpcities):
        uniquecity =[]
        for city in bnbcities:
            if city in yelpcities:
                uniquecity.append(city)
        return uniquecity
    bnbcitylist = get_bnb_cities(allurls)
    yelpcitylist = get_yelp_cities()
    uniquecitylist = get_unique_cities(bnbcitylist,yelpcitylist)
    
    return uniquecitylist

def get_listing_csv_urls(urllist,uniquecities):
    def get_listing_urls(csvlist,uniquecities):
        listingsdata = []
        for i in range(len(csvlist)):
            link = csvlist[i]
            if ('listings.csv' in link):
                for idx,value in enumerate(uniquecities):
                    if (value in link):
                        listingsdata.append(link)
        return listingsdata
    csvurl = '.*\.csv$'
    csvre = re.compile(csvurl)
    csvlist = []
    for idx,value in enumerate(urllist):
        matchedcsv = csvre.finditer(value)
        for matchcsv in matchedcsv:
            csvlist.append(matchcsv.group(0))     
    listingurls = get_listing_urls(csvlist,uniquecities)
    return listingurls

def extract_listing_sum_data(uniquecities,listingcsv):
    df = pd.DataFrame(listingcsv)
    df = df.rename(columns={0: 'url'})
    #link = df['url'][0]
    with open('all_listings_sum.csv', 'w') as f_out:
        for idx,row in df.iterrows():
            link = row['url']
            url_parsed = urlparse(link)
            city = url_parsed[2].split('/')[3]
            date = url_parsed[2].split('/')[4]
            original_url_file_name = city + '_' + date + '_' + os.path.basename(url_parsed.path) 
            filecontent = requests.get(link, allow_redirects=True)
            file = open(original_url_file_name , 'wb').write(filecontent.content)
            df1 = pd.read_csv(original_url_file_name)
            df1['city'] = city
            df1.to_csv(original_url_file_name, index=False)
            with open(original_url_file_name) as f_in:
                for line in f_in:
                    f_out.write(line)
            os.remove(original_url_file_name)
            
def get_zip_urls(urllist):
    zipurl = '.*\.gz$'
    zipre = re.compile(zipurl)
    ziplist = []
    for idx,value in enumerate(urllist):
        matchedzip = zipre.finditer(value)
        for matchzip in matchedzip:
            ziplist.append(matchzip.group(0))
    return ziplist
                    
def get_list_zip_urls(urllist,uniquecities):
    listingslist = []
    for i in range(len(urllist)):
        link = urllist[i]
        if ('listings.csv' in link ):
            listingslist.append(link)
            
    with open('listingszip.txt', 'w') as f:            
        for i in range(len(listingslist)):
            link = listingslist[i]
            link_list = link.split('/')
            city = link_list[-4]
            if city in (uniquecities):
                f.write("%s\n" % link)
                

def ext_upl_del(zipped,Upload):
    dir_path = os.getcwd()
    df = pd.read_csv(zipped,delimiter=" ", header=None)
    df = df.rename(columns={0: 'url'})
    with open(Upload, 'wb') as f_out:
        for idx,row in df.iterrows():
            link = row['url']
            url_parsed = urlparse(link)
            original_url_file_name = os.path.basename(url_parsed.path)
            final_dir = os.path.join(dir_path, original_url_file_name)
            urllib.request.urlretrieve(link, final_dir)
            with gzip.open(final_dir, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)
    
def extract_listings_detail_data():
    listings = 'listingszip.txt'
    all_data_list = 'all_listing_data.csv'
    ext_upl_del(listings,all_data_list)
    
    


def main():
    all_urls = scrape_bnb()
    unique_cities = get_unique_cities(all_urls)
    listingcsv = get_listing_csv_urls(all_urls,unique_cities)
    
    zip_urls = get_zip_urls(all_urls)
    get_list_zip_urls(zip_urls,unique_cities)
    
    extract_listing_sum_data(unique_cities,listingcsv)
    extract_listings_detail_data()

if __name__ == "__main__":
	main()
