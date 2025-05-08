
Create a connector that fetches data from couple of data sources transforms the data and finally saves the output as parquet files. Hence my approach has been to make the codebase easy to undestand and read by making the 'load_and_enrich_ads_data' function declarative in nature.

# Assumptions
Some assumption that I made are following : 

- Source of the ads and revenue data are accessible reliably. In a production ready system this will probably not be true.
- This will be an executable and will be manually triggred from the command line.


# Improvements

- Testing needs to be robust. In the time avaiable I could only figure out 20 unit tests. Ideally we should also have integration tests in a seperate folder.

- In real life the api call might be rate limited or unreliable, so we should have some kind of mechanism in the api_clinet.rs file to deal with rate limiting and retries.

- Benchmarks test to monitor the application's effciency needs to be put in place. (ideally in a bechmarks folder in the application root dir)
