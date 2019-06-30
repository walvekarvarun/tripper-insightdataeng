# Data Processing


readbnbsummary : to read the bnblistingsummary data and to select only important columns into a dataframe

readbnbdetail : to read the bnbdetaillistings data and to select columns needed for further processing

readreviewdetail : to read the bnbdetailed reviews and select only columns needed for further processing

joinbnb : joining the dataframes based on listing_id 

readyelpbusiness : reading yelpbusiness - filtering - flattening the json

readyelpreview : reading the yelpreview dataset and selecting only important columns

joinyelp : joining the data based on businessid

caldistancedf : get both the joined bnb and yelp data into appropriate format and calculate distances between each using haversine function

get_all_categories : map categories to businesss_id and get categories dataframe

pushtopostgres : 
1. bnbdataframe
2. yelpdataframe
3. ditancedataframe
4. yelpcategory





