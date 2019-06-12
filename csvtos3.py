#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 12:42:27 2019

@author: varunwalvekar
"""

from requests import get
from bs4 import BeautifulSoup
import re
import urllib.request, os, gzip,zipfile, boto3, pandas as pd, argparse
from urllib.parse import urlparse
import shutil


def read_yelp_cities():
    df = pd.read_json('business.json', lines = True)
    city = df.city.unique().tolist()
    city_lower = []
    for idx,value in enumerate(city):
        city_lower.append(value.lower())
    return city_lower


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

def scrapebnb():
    url = 'http://insideairbnb.com/get-the-data.html'
    response = get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    address=[]
    for link in soup.find_all('a', class_ = ''):
        address.append(link.get('href'))
    return address    

def get_unique_cities(bnbcities,yelpcities):
    uniquecity =[]
    for city in bnbcities:
        if city in yelpcities:
            uniquecity.append(city)
    return uniquecity

def getzipurls(urllist):
    zipurl = '.*\.gz$'
    zipre = re.compile(zipurl)
    ziplist = []
    for idx,value in enumerate(urllist):
        matchedzip = zipre.finditer(value)
        for matchzip in matchedzip:
            ziplist.append(matchzip.group(0))
    return ziplist    

def read_yelp_data():
    business = pd.read_json('business.json', lines = True)
    review = pd.read_json('review.json', lines = True)
    tip = pd.read_json('tip.json', lines = True)
    user = pd.read_json('user.json', lines = True)
    photo = pd.read_json('photo.json', lines = True)

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

def upload_to_s3():
    dir_path = os.getcwd()
    listings = 'listingszip.txt'
    df = pd.read_csv(listings,delimiter=" ", header=None)
    df = df.rename(columns={0: 'url'})
    
    with open('file.txt', 'wb') as f_out:
        for idx,row in df.iterrows():
            link = row['url']
            url_parsed = urlparse(link)
            original_url_file_name = os.path.basename(url_parsed.path)
            final_dir = os.path.join(dir_path, original_url_file_name)
        #original_url_file_name = url_parsed.path.split('/')
        #filename = original_url_file_name[3] + '-' + original_url_file_name[4]+'-'+original_url_file_name[6]
        #final_dir = os.path.join(dir_path, filename)
            urllib.request.urlretrieve(link, final_dir)
            with gzip.open(final_dir, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)
        
## TEST GIT CHANGES
        
        

#    csvfile = 'zipurl.txt'
#    
#    s3 = boto3.client('s3')
#    bucket_name = 'bnbdatadump/'
#    
#    df = pd.read_csv(csvfile, delimiter=" ", header=None)
#    df = df.rename(columns={0: 'url'})
#    dir_path = os.getcwd()
#
#
#    for idx,row in df.iterrows():
#        print('loopstarted')
#        l
#        if ('listings.csv' in link) :
#            url_parsed = urlparse(row['url'])
#            original_url_file_name = url_parsed.path.split('/')
#            filename = original_url_file_name[3] + '-' + original_url_file_name[4]+'-'+original_url_file_name[6]
#            final_dir = os.path.join(dir_path, filename)
#            print('copying to local')
#            urllib.request.urlretrieve(row['url'], final_dir)
#            print('uploading to s3')
#            s3.upload_file(final_dir, bucket_name, filename )
#            print('deleting from local')
#            os.remove(final_dir)
 

       
def main():
    allurls = scrapebnb()
    allzipurls = getzipurls(allurls)
    bnbcities = bnb_listings_cities(allurls)
    yelpcities = read_yelp_cities()
    uniquecities = get_unique_cities(bnbcities,yelpcities)
    print(uniquecities)
    ubnb_uniq_urls(uniquecities,allzipurls)
    
    
if __name__ == "__main__":
	main()
    