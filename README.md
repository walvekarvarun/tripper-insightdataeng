# TRIPPER

## Problem Statement:
Analyse data to rank highly rated events/restaurants/businesses and suggest the best(highly rated/least cost) place to stay around these events/restaurants/businesses.

## Business Case
Helping Busy Professionals to find the right vacation home based on what they like/want to do!

## My Notes : 
### Week 1 was on Dataset exploration , Finalising the Business Use case/ Engineering challenge
### Week 2 : Airbnb DataFiltering and Uploading to S3
Scraped airbnb for all urls
got list of cities in airbnb and yelp
got all the urls - listings, reviews and calendar for only the unique cities
extracted data from each url and added it to one text file - listings.txt , reviews.txt , calendar.txt
uploaded these 3 files to s3

-- an ec2 instance - m4.large wth 25gb was setup to run the python script to do all the above tasks 
The ec2 instance needs to have an additional outbound security group: allowing: https port 443 requests
In the ec2 instance - run following commands : 
sudo apt-get update
sudo apt install aws cli
aws configure , to configure with your credentials
install requests , request, urllib3, bs4 boto3 and pandas 


## Technology Stack:
S3, Spark, (Maybe Kafka), PostGres + PostGIS, Flask
