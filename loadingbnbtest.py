#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 12:26:55 2019

@author: varunwalvekar
"""

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#set sparkConf,sparkContext and sparkSession

def read_and_filter_airbnb(spark):
    df_listing = spark.read.load("s3a://bnbdatadump/all_listing_data.txt",format="csv",header='True',sep=",")
    df_listing = df_listing.selectExpr('id','city','name', 'summary', 'space', 'description', 'experiences_offered','latitude', 'longitude','price','review_scores_rating')
    return df_listing   

def main():
    conf = SparkConf().setAppName("readfroms3").set("spark.executor.memory", "6g").setMaster("spark://10.0.0.26:7077")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    #yelp_df = read_and_filter_yelp(spark)
#    with open('unique_cities.txt','r') as inpt:
#        for line in inpt:
#            city = line
#            update_to_postgres(yelp_df,city)
    #uniquecategories = list(get_all_categories(yelpdf))
    #update_to_postgres(yelpcitydf)
    bnb_df = read_and_filter_airbnb(spark)
    bnb_df.columns
    
    
    
if __name__ == "__main__":
    main()