Indexes on the Tables:

CREATE INDEX id_idx ON uniquecategories;
CREATE INDEX yelp_id_idx ON yelpcategory;
CREATE INDEX listing_id_idx ON bnbsum;
CREATE INDEX business_id_idx ON yelpbusiness;
CREATE INDEX dist_listing_id on distance(listing_id);
CREATE INDEX dist_business_id on distance(business_id);
