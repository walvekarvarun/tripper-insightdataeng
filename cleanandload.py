#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 12:42:27 2019

@author: varunwalvekar
"""

from requests import get
from bs4 import BeautifulSoup
import re
import urllib.request, os, gzip, boto3, pandas as pd
from urllib.parse import urlparse
import shutil

#Function to read all cities in the yelp business.json dataset
def read_yelp_cities():
    df = pd.read_json('business.json', lines = True)
    city = df.city.unique().tolist()
    city_lower = []
    for idx,value in enumerate(city):
        city_lower.append(value.lower())
    return city_lower

#Function to read all cities in the bnbdataset
def bnb_listings_cities(allurls):
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

#Function to scrape all the urls from bnb dataset
def scrapebnb():
    url = 'http://insideairbnb.com/get-the-data.html'
    response = get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    address=[]
    for link in soup.find_all('a', class_ = ''):
        address.append(link.get('href'))
    return address    

#Function to get a list of all Unique cities
def get_unique_cities(bnbcities,yelpcities):
    uniquecity =[]
    for city in bnbcities:
        if city in yelpcities:
            uniquecity.append(city)
    return uniquecity

#Function to get only the zip urls
def getzipurls(urllist):
    zipurl = '.*\.gz$'
    zipre = re.compile(zipurl)
    ziplist = []
    for idx,value in enumerate(urllist):
        matchedzip = zipre.finditer(value)
        for matchzip in matchedzip:
            ziplist.append(matchzip.group(0))
    return ziplist    

#Fetch csv URL, Download into local system, upload to S3 and delete file
def bnb_uniq_urls(uniquecities,allzipurls):
    listingslist = []
    calendarlist = []
    reviewslist = []
    for i in range(len(allzipurls)):
        link = allzipurls[i]
        if ('listings.csv.gz' in link):
            listingslist.append(link)
        elif ('calendar.csv.gz' in link):
            calendarlist.append(link)
        elif ('reviews.csv.gz' in link):
            reviewslist.append(link)
            
    with open('listingszip.txt', 'w') as f:            
        for i in range(len(listingslist)):
            link = listingslist[i]
            link_list = link.split('/')
            city = link_list[-4]
            if city in (uniquecities):
                f.write("%s\n" % link)
                
    with open('calendarzip.txt', 'w') as f:        
        for i in range(len(calendarlist)):
            link = calendarlist[i]
            link_list = link.split('/')
            city = link_list[-4]
            if city in (uniquecities):
                f.write("%s\n" % link)
                
    with open('reviewszip.txt', 'w') as f:           
        for i in range(len(reviewslist)):
            link = reviewslist[i]
            link_list = link.split('/')
            city = link_list[-4]
            if city in (uniquecities):
                f.write("%s\n" % link)
                
def ext_upl_del(zipped,Upload):
    dir_path = os.getcwd()
    s3 = boto3.client('s3')
    bucket_name = 'bnbdatadump'
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
    all_data_dir = os.path.join(dir_path,Upload)
    s3.upload_file(all_data_dir,bucket_name,Upload)
    print('File Uploaded to S3')
    os.remove(all_data_dir)
    print('File Removed from local')

def extract_all():
    
    listings = 'listingszip.txt'
    all_data_list = 'all_listing_data.txt'
    ext_upl_del(listings,all_data_list)
    
    reviews = 'reviewszip.txt'
    all_data_rev = 'all_review_data.txt'
    ext_upl_del(reviews,all_data_rev)
    
    calendar = 'calendarzip.txt'
    all_data_cal = 'all_review_data.txt'
    ext_upl_del(calendar,all_data_cal)

       
def main():
    allurls = scrapebnb()
    print('scraped the airbnb website and extracted ALL the dataurls')
    
    allzipurls = getzipurls(allurls)
    print('filtered all urls only to get the zipped files as these have detailed data')
    
    bnbcities = bnb_listings_cities(allurls)
    print('Got a list of ALL cities present in the airbnb - to compare with the yelp cities ')
    print(bnbcities)
    
    yelpcities = read_yelp_cities()
    print('Got a list of ALL yelp cities in the yelp business dataset')
    print(yelpcities)
    
    uniquecities = get_unique_cities(bnbcities,yelpcities)
    print('Got a list of Unique cities belonging to both the datasets')
    print(uniquecities)
    
    bnb_uniq_urls(uniquecities,allzipurls)
    print('created 3 text files of the airbnb dataset - listings.txt,review.txt,calendar.txt which has all zipurls of only unique cities' )
    
    print('Reading each zipurl line-by-line - unzipping - loading all data in one text file -- eg - all listings data for all unique cities will be in this text file')
    print('Also uploading this textfile to s3')
    print('Also deleting the file in the local system after uploading to s3')
    extract_all()
    
    
if __name__ == "__main__":
	main()
    