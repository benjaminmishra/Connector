use crate::api_client::{AdsApi, ApiClient};
use crate::config::Config;
use crate::data;
use crate::data::{ads_schema, convert_ads_data_to_df, revenue_schema};
use crate::error::Error;
use crate::revenue_loader::{ParquetRevenueLoader, RevenueLoader};
use chrono::NaiveDate;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub async fn load_and_enrich_ads_data(
    config: Config,
    start: &NaiveDate,
    end: &NaiveDate,
) -> Result<(), Error> {
    if start > end {
        return Err(Error::StartDateAfterEndDate {
            start_date: start.to_string(),
            end_date: end.to_string(),
        });
    }

    let api_client = Arc::new(ApiClient::new(&config));
    let revenue_data_loader = Arc::new(ParquetRevenueLoader::new(&config));
    let ctx = SessionContext::new();
    let ads_schema = ads_schema();
    let rev_schema = revenue_schema();

    let account_ids = api_client.fetch_accounts_ids().await?;

    let campaign_entries = api_client
        .fetch_ads_for_accounts(&account_ids, start, end)
        .await?;

    let ads_df = convert_ads_data_to_df(&campaign_entries, &ctx, ads_schema.clone())?;

    let revenue_df = revenue_data_loader
        .load(&ctx, rev_schema.clone(), &account_ids)
        .await?;

    let enriched_df = data::enrich_ads(ads_df, revenue_df).await?;

    data::partition_and_save_by_account(enriched_df, &config.output_dir).await?;

    Ok(())
}
