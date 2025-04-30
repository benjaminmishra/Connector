use crate::config::Config;
use crate::error::Error;
use chrono::NaiveDate;
use log::error;
use reqwest::{header::AUTHORIZATION, Client, StatusCode, Url};
use serde::Deserialize;

#[async_trait::async_trait]
pub trait AdsApi: Send + Sync + 'static {
    /// Fetches account IDs from the API.
    /// # Returns
    /// A Result containing either a vector of account IDs or an Error.
    async fn fetch_accounts_ids(&self) -> Result<Vec<String>, Error>;

    /// Fetches ads data for the specified account IDs within the given date range.
    /// # Arguments
    /// * `account_ids` - A slice of account IDs to fetch ads data for.
    /// * `start` - The start date for the data range.
    /// * `end` - The end date for the data range.
    /// # Returns
    /// A Result containing either a vector of `CampaignEntryWithAccountId` or an Error.
    /// The `CampaignEntryWithAccountId` struct contains the account ID, campaign ID, clicks, conversions,
    /// cost, date, and impressions.
    async fn fetch_ads_for_accounts(
        &self,
        account_ids: &[String],
        start: &NaiveDate,
        end: &NaiveDate,
    ) -> Result<Vec<CampaignEntryWithAccountId>, Error>;
}

/// Struct to hold the configuration for the API client
#[derive(Deserialize, Debug)]
pub struct CampaignEntryWithAccountId {
    pub account_id: String,
    pub campaign_id: String,
    pub clicks: u64,
    pub conversions: u64,
    pub cost: f64,
    pub(crate) date: String,
    pub impressions: u64,
}

#[derive(Clone)]
pub struct ApiClient {
    client: Client,
    base_url: String,
    token: String,
}


#[derive(Deserialize)]
struct CampaignEntry {
    campaign_id: String,
    clicks: u64,
    conversions: u64,
    cost: f64,
    date: String,
    impressions: u64,
}

#[derive(Deserialize)]
struct AdData {
    data: Vec<CampaignEntry>,
}

#[derive(Deserialize, Clone, PartialEq, Eq)]
struct Account {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct Accounts {
    ad_accounts: Vec<Account>,
}

impl ApiClient {
    pub fn new(config: &Config) -> Self {
        ApiClient {
            client: Client::new(),
            base_url: config.api_url.to_string(),
            token: config.api_token.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl AdsApi for ApiClient {
    async fn fetch_accounts_ids(&self) -> Result<Vec<String>, Error> {
        let url = format!("{}/demo/adAccounts", self.base_url);

        let resp = self
            .client
            .get(&url)
            .header(AUTHORIZATION, &self.token)
            .send()
            .await?
            .error_for_status()?;

        let accounts = resp.json::<Accounts>().await?;

        if accounts.ad_accounts.is_empty() {
            return Err(Error::NoData {
                message: "No accounts found for processing".to_string(),
            });
        }

        Ok(accounts
            .ad_accounts
            .into_iter()
            .map(|acct| acct.id)
            .collect())
    }

    async fn fetch_ads_for_accounts(
        &self,
        account_ids: &[String],
        start: &NaiveDate,
        end: &NaiveDate,
    ) -> Result<Vec<CampaignEntryWithAccountId>, Error> {
        let mut ads_data_for_accounts = vec![];

        for account_id in account_ids.iter() {
            // Construct the URL safely
            let mut url = Url::parse(&self.base_url)?;
            url.path_segments_mut()
                .map_err(|_| Error::UrlParsingFailed(url::ParseError::SetHostOnCannotBeABaseUrl))?
                .extend(&["demo", "getData", account_id]);
            url.query_pairs_mut()
                .append_pair("start", &start.format("%Y-%m-%d").to_string())
                .append_pair("end", &end.format("%Y-%m-%d").to_string());

            let resp = self
                .client
                .get(url)
                .header(AUTHORIZATION, &self.token)
                .send()
                .await?;

            match resp.status() {
                StatusCode::OK => {
                    let response: AdData = resp.json().await?;
                    let campaign_entries_with_account_id: Vec<CampaignEntryWithAccountId> =
                        response
                            .data
                            .into_iter()
                            .map(|ce| CampaignEntryWithAccountId {
                                account_id: account_id.clone(),
                                campaign_id: ce.campaign_id,
                                clicks: ce.clicks,
                                conversions: ce.conversions,
                                cost: ce.cost,
                                date: ce.date,
                                impressions: ce.impressions,
                            })
                            .collect();

                    ads_data_for_accounts.extend(campaign_entries_with_account_id);
                }
                status => {
                    error!("Error fetching ads for account {}: {}", account_id, status);
                }
            }
        }

        if ads_data_for_accounts.is_empty() {
            return Err(Error::NoData {
                message: "No ads data found".to_string(),
            });
        }

        Ok(ads_data_for_accounts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_fetch_ads_for_accounts_invalid_url() {
        let config = Config {
            api_url: String::from("invalid_url"),
            api_token: String::from("test_token"),
            revenue_file_path: String::from("/path/to/revenue/files"),
            output_dir: String::from("/path/to/output"),
        };
        let client = ApiClient::new(&config);
        let start = NaiveDate::from_str("2024-01-01").unwrap();
        let end = NaiveDate::from_str("2024-01-31").unwrap();
        let account_ids = vec![String::from("12345")];

        let result = client
            .fetch_ads_for_accounts(&account_ids, &start, &end)
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::UrlParsingFailed(_)
        ));
    }

    #[tokio::test]
    async fn test_fetch_ads_for_accounts_invalid_response_format() {
        let config = Config {
            api_url: String::from("https://api.example.com"),
            api_token: String::from("test_token"),
            revenue_file_path: String::from("/path/to/revenue/files"),
            output_dir: String::from("/path/to/output"),
        };
        let client = ApiClient::new(&config);
        let start = NaiveDate::from_str("2024-01-01").unwrap();
        let end = NaiveDate::from_str("2024-01-31").unwrap();
        let account_ids = vec![String::from("invalid_response_account")];

        let result = client
            .fetch_ads_for_accounts(&account_ids, &start, &end)
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::ApiFailure(_)
        ));
    }

    #[tokio::test]
    async fn test_fetch_ads_for_accounts_invalid_response_data() {
        let config = Config {
            api_url: String::from("https://api.example.com"),
            api_token: String::from("test_token"),
            revenue_file_path: String::from("/path/to/revenue/files"),
            output_dir: String::from("/path/to/output"),
        };
        let client = ApiClient::new(&config);
        let start = NaiveDate::from_str("2024-01-01").unwrap();
        let end = NaiveDate::from_str("2024-01-31").unwrap();
        let account_ids = vec![String::from("invalid_response_data_account")];

        let result = client
            .fetch_ads_for_accounts(&account_ids, &start, &end)
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::ApiFailure(_)
        ));
    }

    #[tokio::test]
    async fn test_fetch_ads_for_accounts_invalid_response_structure() {
        let config = Config {
            api_url: String::from("https://api.example.com"),
            api_token: String::from("test_token"),
            revenue_file_path: String::from("/path/to/revenue/files"),
            output_dir: String::from("/path/to/output"),
        };
        let client = ApiClient::new(&config);
        let start = NaiveDate::from_str("2024-01-01").unwrap();
        let end = NaiveDate::from_str("2024-01-31").unwrap();
        let account_ids = vec![String::from("invalid_response_structure_account")];

        let result = client
            .fetch_ads_for_accounts(&account_ids, &start, &end)
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::ApiFailure(_)
        ));
    }

    #[tokio::test]
    async fn test_fetch_ads_for_accounts_invalid_response_content() {
        let config = Config {
            api_url: String::from("https://api.example.com"),
            api_token: String::from("test_token"),
            revenue_file_path: String::from("/path/to/revenue/files"),
            output_dir: String::from("/path/to/output"),
        };
        let client = ApiClient::new(&config);
        let start = NaiveDate::from_str("2024-01-01").unwrap();
        let end = NaiveDate::from_str("2024-01-31").unwrap();
        let account_ids = vec![String::from("invalid_response_content_account")];

        let result = client
            .fetch_ads_for_accounts(&account_ids, &start, &end)
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::ApiFailure(_)
        ));
    }
}
