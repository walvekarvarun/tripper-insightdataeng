# Ingestion

extract1.py consists of code for the zip urls
extract2.py consists code for the summarydata urls

The process followed is L 
1. Scrape all the URLS from the Airbnb Website -- scrapebnb()
  1.1 : Zip URLS -- getzipurls()
  1.2 : GeoJson URL's
  1.3 CSV URL's
2. Extract City Name from these URL's and load them into a list.
3. Fetch the Cities from the Yelp Datasetv(business.json) and load them into a list -- read_yelp_cities()
4. Get a Unique Cities list - Cities in both the AirBNB and the Yelp Dataset -- get_unique_cities()
5. Extract URL's only for the unique cities from Step 1 and store seperate tect files  -- bnb_uniq_urls , extract_all
6. For the CSV URL's - which are summary data(listings.csv , reviews.csv) - 
  6.1 - Create a master csv file - Extract the data from each url - store it into a temp csv file - copy to master csv file - delete the temp csv file - upload master to s3
7. For the ZIP URL's - which are detailed data(listings.csv.gz , reviews.csv.gz , calendar.csv.gz) - 
  7.1 create a master csv file - fetch url line by line - unzip the file , load the data into a temp file, copy to master , delete temp, upload master to s3.
 8. Upload the business.json and review.json files (Yelp Data) to S3
  
The 3 master csv file for the zipped urls are : 
1. all_listings_data.csv
2. all_reviews_data.csv
3. all_calendar_data.csv

The 2 master csv files for the summary urls are L 
1. all_listings_sum.csv
2. all_reviews_sum.csv
