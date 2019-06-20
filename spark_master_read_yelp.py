#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 20:40:10 2019

@author: varunwalvekar
"""

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#set sparkConf,sparkContext and sparkSession


# Function to flaten the struct in the dataframe
# https://stackoverflow.com/questions/38753898/how-to-flatten-a-struct-in-a-spark-dataframe
def flatten_df(nested_df, layers):
    flat_cols = []
    nested_cols = []
    flat_df = []
    flat_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] != 'struct'])
    nested_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] == 'struct'])
    flat_df.append(nested_df.select(flat_cols[0] +[col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols[0] for c in nested_df.select(nc+'.*').columns]))
    for i in range(1, layers):
        print (flat_cols[i-1])
        flat_cols.append([c[0] for c in flat_df[i-1].dtypes if c[1][:6] != 'struct'])
        nested_cols.append([c[0] for c in flat_df[i-1].dtypes if c[1][:6] == 'struct'])
        flat_df.append(flat_df[i-1].select(flat_cols[i] +[col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols[i] for c in flat_df[i-1].select(nc+'.*').columns]))
    return flat_df[-1]

def read_and_filter_yelp(spark):
    #read business and apply filters
    df_business = spark.read.json("s3a://bnbdatadump/business.json")
    df_business = df_business.selectExpr('address', 'attributes', 'business_id', 'categories', 'city', 'hours', 'is_open', 'latitude', 'longitude', 'name', 'postal_code', 'review_count', 'stars as businessstars', 'state')
    
    df_business = df_business.filter(df_business.is_open == 1)
    df_business = flatten_df(df_business,2)
    
    #read reviews
    df_review = spark.read.json("s3a://bnbdatadump/review.json")
    df_review = df_review.selectExpr('business_id', 'cool', 'date', 'funny', 'review_id', 'stars as reviewstars', 'text', 'useful', 'user_id')
    
    #join on business_id
    df = df_business.join(df_review, ["business_id"])
    
    #selected columns
    df = df.selectExpr('business_id', 'address', 'categories', 'city',  'latitude', 'longitude', 'name', 'review_count', 'businessstars','reviewstars','text')

    return df


#def get_all_categories(df):
#    def split_categories(row):
#        if row.categories != None:
#            return row.categories.split(", ")
#        return []
#    
#    categories = df.rdd.map(split_categories).collect()
#    
#    category_set = set()
#    for row in categories:
#        category_set.update(row)
#    return category_set
    
#def get_city_df(yelp_df):
#    citydf = yelp_df.filter(df.city = city)
#    return citydf

#def update_to_postgres(yelp_df,city):
#    citydf = get_city_df(yelp_df)
#    citydf.write \
#    .format("jdbc") \
#    .option("url", "jdbc:postgresql://10.0.0.XX:XXXX/DBNAME") \
#    .option("dbtable", "yelp_table") \
#    .option("user", "tripper") \
#    .option("password", "tripper") \
#    .save()


def read_and_filter_airbnb(spark):
    df_listing = spark.read.load("s3a://bnbdatadump/all_listing_data.txt",format="csv",header='True',sep=",")
    df_listing = df_listing.selectExpr('id','city','name', 'summary', 'space', 'description', 'experiences_offered','latitude', 'longitude','price','review_scores_rating')
    return df_listing   

#from math import radians, cos, sin, asin, sqrt             
#def haversine(lon1, lat1, lon2, lat2):
#    # convert decimal degrees to radians 
#    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
#    # calculate diffs of lat/lon 
#    dlon = lon2 - lon1 
#    dlat = lat2 - lat1
#    #haversine formula
#    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
#    c = 2 * asin(sqrt(a)) 
#    #set radius of earth
#    r = 3959
#    #return circle distance and multiply by 1.2 to get road distance
#    return (c * r)*1.2
            
def main():
    conf = SparkConf().setAppName("readfroms3").set("spark.executor.memory", "6g").setMaster("spark://10.0.0.26:7077")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    yelp_df = read_and_filter_yelp(spark)
#    with open('unique_cities.txt','r') as inpt:
#        for line in inpt:
#            city = line
#            update_to_postgres(yelp_df,city)
    #uniquecategories = list(get_all_categories(yelpdf))
    #update_to_postgres(yelpcitydf)
    bnb_df = read_and_filter_airbnb(spark)
    
    
    
if __name__ == "__main__":
    main()
