#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 17:28:15 2019

@author: varunwalvekar
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrameWriter, DataFrameReader
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lower
from math import radians, cos, sin, asin, sqrt  
from pyspark.sql.types import DoubleType,FloatType
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf,array
from math import radians, cos, sin, asin, sqrt    
from pyspark.sql.types import *
from pyspark.sql.functions import *


## Function to flatted json file
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

## Function to calculate distances
def haversine(lon1, lat1, lon2, lat2 ):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 3959
    return float((c * r)*1.2)

######### BNB DATAFRAMES
def readbnbsummary(spark):
    # Read Lsitings Summary
    df_listings_sum = spark.read.csv("s3a:<Path to S3 busket>",header='True',sep=",")
    df_listings_sum = df_listings_sum.selectExpr('id as listing_id', 'name', 'latitude', 'longitude', 'price', 'city')
    return df_listings_sum

def readbnbdetail(spark):
    # Read bnb listings detail
    df_listing = spark.read.load("s3a:<Path to S3 busket>",format="csv",header='True',sep=",")
    df_listing = df_listing.selectExpr('id','city','name', 'experiences_offered','latitude','longitude','price','review_scores_rating')
    return df_listing 
    
def readbnbreviewdetail(spark):
    df_review = spark.read.load("s3a://vmwinsight/all_review_data.txt",format="csv",header='True')
    df_review = df_review.selectExpr('listing_id','comments')
    return df_review
    
def joinbnb(bnbsummary,bnbreviewdetail):
    df = bnbreviewdetail.join(bnbsummary, ["listing_id"])
    return df

###### YELP DATAFRAMES

def readyelpbusiness(spark):
    # Read Yelp Business
    df_business = spark.read.json('s3a:<Path to S3 busket>'')
    df_business = df_business.filter(df_business.is_open == 1)
    df_business = df_business.selectExpr('address','business_id', 'attributes','categories', 'city','latitude', 'longitude', 'name', 'review_count', 'stars')
    df_business = flatten_df(df_business,2)
    return df_business
    
def readyelpreview(spark):
    df_review = spark.read.json('s3a:<Path to S3 busket>')
    df_review = df_review.selectExpr('business_id','text','useful')
    return df_review

def joinyelp(yelpbusiness,yelpreview):
    df = yelpreview.join(yelpbusiness, ["business_id"])
    return df
  
############# DISTANCE DATAFRAME    
def calcdistancedf(yelpbusiness,bnbsummary):
    business = yelpbusiness.selectExpr('business_id','city','latitude','longitude')
    listing = bnbsummary.selectExpr('id','city','latitude','longitude')

    columnName="city"
    business=business.withColumn(columnName, lower(col(columnName)));
    business = business.selectExpr('business_id','city','latitude as yelplat','longitude as yelplong')
    listing = listing.selectExpr('id as listing_id','city','latitude as bnblat','longitude as bnblong')
    #Join both dataframes
    df = listing.join(business, ["city"])
    # get correct datatypes to calculate distances
    df = df.withColumn("bnblat", df["bnblat"].cast(FloatType()))
    df = df.withColumn("bnblong", df["bnblong"].cast(FloatType()))
    df = df.withColumn("yelplat", df["yelplat"].cast(FloatType()))
    df = df.withColumn("yelplong", df["yelplong"].cast(FloatType()))
    
    #define a udf and call the haversine function to calculate distances
    udf_haversine = udf(lambda z: haversine(z), FloatType())
    df1 = df.select('city','listing_id','bnblong','bnblat','business_id','yelplong','yelplat',udf_haversine(array('bnblong','bnblat','yelplong','yelplat')).alias('distance'))
    df1 = df1.selectExpr('city','listing_id','business_id','distance')
    return df1


################ GET CATEGORIES
def get_all_categories(df):
    def split_categories(row):
        if row.categories != None:
            return row.categories.split(", ")
        return []
    categories = df_business.rdd.map(split_categories).collect()
    category_set = set()
    for row in categories:
        category_set.update(row)
    return category_set

################# PUSH TO POSTGRES
def pushtopostgres(df,tablename):
    df1.write.format("jdbc").option("url", "jdbc:postgresql://<ip+port>/dbname").option("dbtable", tablename).option("user", <username>).option("password", <password>).save()


#################### MAIN
    
def main():
    ### IMPORTANT!!! : While using spark submit make sure to use --packages org.apache.hadoop:hadoop-aws:2.7.3, for readin from s3
    conf = SparkConf().setAppName(<setappname>).set("spark.executor.memory", <set memory of executor>).setMaster("spark://<masterip>")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    
    #readbnblisting
    bnbsummary = readbnbsummary(spark)
    bnbdetail = readbnbdetail(spark)
    
    #readbnbreview
    bnbreviewsum = readbnbreviewsummary(spark)
    bnbreviewdetail = readbnbreviewdetail(spark)
    
    bnbfinal = joinbnb(bnbsummary,bnbreviewsum)
    pushtopostgres(bnbfinal , bnb)
    
    # Read Yelp Data and Push to Postgres
    yelpbusiness = readyelpbusiness(spark)
    yelpreview = readyelpreview(spark)
    yelpfinal = joinyelp(yelpbusiness,yelpreview)
    pushtopostgres(yelpfinal,yelp)
    
    # Create and push Distance dataframe
    distancedf = calcdistancedf(yelpbusiness,bnbsummary)
    pushtopostgres(distancedf,distance)
    
    # Create and push Categories dataframe
    categories = list(get_all_categories(yelpbusiness))
    sql_context = SQLContext(sc)
    schema = StructType([StructField("id", IntegerType(), False),StructField("name", StringType(), False)])
    category = sql_context.createDataFrame(zip(range(len(categories)), categories), schema)
    pushtopostgres(category,distance)
    
if __name__ == "__main__":
    main()
        
    
    
    


