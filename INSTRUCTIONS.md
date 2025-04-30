# Rust Code Assignment

## Assumptions

* The calculated conversion rate metric has just clicks as the divisor.
* That the same (campaign ID, date) combinations are present in both the fetched JSON and in the local Parquet file. 
  * A more reliable solution could be to use a different `JOIN` variant with nulls coalescing to zero.
* That the application is run from the same directory as where the `revenue-<account_id>.parquet` files reside.

## Task: Prepare advertisement data for analysis

At Funnel, we collect and manage advertising data from various platforms to help our customers understand their marketing performance.

### The Goal
Your task is to create an application (Connector), that retrieves data from a mock advertising platform called Sultek provided by Funnel. 
In addition to this, we have provided a parquet file that contains data, which we want you to enrich the Sultek data with.
The API documentation for Sultek is at the bottom of this document.
The application should fetch the data from Sultek, enrich it with the data in the parquet file, do some calculations and store the result for further analysis.

*Useful marketing nomenclature*

 * ROAS (Return on Ad spend):  
	 *Formula*: `Revenue (total income from advertising) / Cost (total ads spend)`
 * Conversion Rate:  
	 *Formula*: `(Conversions / Impressions or Clicks) x 100%`
 * Cost Per Conversion (CPC):  
	 *Formula*: `Ad Spend / Conversions`
 * Impressions to Conversion Ratio:  
	 *Formula*: `Impressions / Conversions`
 * Click-Through Rate (CTR):  
   *Formula*: `(Clicks / Impressions) x 100%`

**Your goal** for this assignment is to help us to understand your level as a developer.
Document your reasoning and assumptions  in the assignment.
Pay close attention to details.
Stay aligned with the core requirements of the assignment.
Think about simplicity, maintainability and readability in your solution.
Implement the solution as you would do to make it production-ready.

If you feel like you have spent too much time on this assignment, document what you would like to do if you had more time. You can always reach out to (code-assignments@funnel.io) if something is unclear and we will clarify and steer you in the right direction. 


### Task: Fetch data from the SultekAPI and enrich it with the data from the parquet file
1. Fetch data from Sultek API for all ad accounts.
2. Enrich the data with the data in the provided `revenue-<ad_account_id>.parquet` file.
3. Calculate the following metrics **for each ad account per day**:
	 * ROAS
	 * Conversion Rate
	 * Cost Per Conversion
	 * Impressions to Conversion Ratio
	 * Click-Through Rate (CTR)
4. Store the enriched data in a new parquet file name `analysis-<account_id>.parquet` further analysis.

#### Appendix 1: API documentation - Sultek advertising platform
Auth Token: `bb6d467a123`  
Host: `https://sultek.data-in-stage.funnel.io`  


Get Ad accounts:
```
GET /demo/adAccounts
authorization: <token>

example response:
200 OK
{
  "ad_accounts":[{
     "id": "Account Id",
     "name": "Account Name"
  }]
}
```

Get data for an ad account:
```
GET /demo/getData/<ad_account_id>
query params:
	start=YYYY-MM-DD
	end=YYYY-MM-DD
authorization: <token>

example response:
200 OK
{
   "data":[
      {
         "campaign_id":"may-campaign-187091",
         "clicks": 459523,
         "conversions": 8912,
         "cost": 9712.85,
         "date": "2024-05-01",
         "impressions": 1295715
      }
   ]
}
```



