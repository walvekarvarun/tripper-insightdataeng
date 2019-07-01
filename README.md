# TRIPPER

## Problem Statement and Business Case:
Analyse big data to rank highly rated businesses and suggest the best(highly rated/least cost) housing option around these businesses.

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
![Tech Stack](https://github.com/walvekarvarun/tripper-insightdataeng/blob/master/Tech%20Stack.png)




## SETUP
1. Script written to scrape data from Airbnb and Yelp Website and load into S3
2. AWS VPC, Subnet and relevant Security Groups setup
3. Spark Cluster has 3 EC2 Instances - M4Large with 25GB of EBS storage each
4. PostgresSQL database setup on 1 EC2 Instance - M4Large with 25 GB of EBS storage
5. Data read from S3 to Spark Master using PySpark - Relevant Drivers to read setup
6. Data cleaned, transformed , joined , processed
  Clean : Only relevant fields picked up , filters applied
  Transformed : 1. Json files - Flattened out schema created 2. Categories split into list
  Joined : Yelp business and reviews joined together and Airbnb Listings and Reviews joined together
  Processed : Haversine Function to calculate the distances between Airbnb and Yelp Business
7. Data pushed into  PostgresSQL tables : yelptable, 
8. 

