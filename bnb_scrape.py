#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  5 19:44:51 2019

@author: varunwalvekar
"""
from requests import get
from bs4 import BeautifulSoup
import re


## Scrape all the URLs from the website 
def scrapeurls(response):
    soup = BeautifulSoup(response.text, 'html.parser')
    address=[]
    for link in soup.find_all('a', class_ = ''):
        address.append(link.get('href'))

    paturl = 'http://data.*'
    urlpatre = re.compile(paturl)
    urllist = []
    for idx,value in enumerate(address):
        matchedurl = urlpatre.finditer(value)
        for match in matchedurl:
            urllist.append(match.group(0))
    return urllist
    #finaldf = pd.DataFrame({'url':urllist})

## Extract only the URLs which are zip files    
def getzipurls(urllist):
    zipurl = '.*\.gz$'
    zipre = re.compile(zipurl)
    ziplist = []
    for idx,value in enumerate(urllist):
        matchedzip = zipre.finditer(value)
        for matchzip in matchedzip:
            ziplist.append(matchzip.group(0))
    return ziplist

## Extract only the URLs which are csv's
def getcsvurls(urllist):
    csvurl = '.*\.csv$'
    csvre = re.compile(csvurl)
    csvlist = []
    for idx,value in enumerate(urllist):
        matchedcsv = csvre.finditer(value)
        for matchcsv in matchedcsv:
            csvlist.append(matchcsv.group(0))
    return csvlist

## Extract only the URLS which are json's
def getjsonurls(urllist):
    jsonurl = '.*\.geojson$'
    jsonre = re.compile(jsonurl)
    jsonlist = []
    for idx,value in enumerate(urllist):
        matchedjson = jsonre.finditer(value)
        for matchjson in matchedjson:
            jsonlist.append(matchjson.group(0))
    return jsonlist

def main():
    url = 'http://insideairbnb.com/get-the-data.html'
    response = get(url)
    urllist = scrapeurls(response)
    ziplist = getzipurls(urllist)
    csvlist = getcsvurls(urllist)
    jsonlist = getjsonurls(urllist)
    #write all csv urls to a file
    with open('csvurl.txt', 'w') as f:
        for item in csvlist:
            f.write("%s\n" % item)
    #write all json urls to a file
    with open('jsonurl.txt', 'w') as f:
        for item in jsonlist:
            f.write("%s\n" % item)
    #write all zip urls to a file
    with open('zipurl.txt', 'w') as f:
        for item in ziplist:
            f.write("%s\n" % item)


if __name__ == "__main__":
	main()
    








