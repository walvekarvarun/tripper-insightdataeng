# TRIPPER

## Problem Statement:
Analyse data to rank highly rated events/restaurants/businesses and suggest the best(highly rated/least cost) place to stay around these events/restaurants/businesses.

## Business Case
Helping Busy Professionals to find the right vacation home based on what they like/want to do!

## Technology Stack:
S3, Spark, PostGres, Flask




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

