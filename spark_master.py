#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 14 11:15:47 2019

@author: varunwalvekar
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import spark.session


mentions_df = spark.read.load("s3a://bnbdatadump/all_calendar_data.txt",
                              format="csv",                             
                              header="false",
                              schema=mentions_schema)

#events_df = spark.read.load("s3a://nmduartegdelt/data/20190102000000.export.CSV",
#                            format="csv",
#                            sep="\t",
#                            header="false",
#                            schema=events_schema)

#events_df.select(events_df["GLOBALEVENTID"]).show()

mentions_df.select(mentions_df["listing_id"]).show()
