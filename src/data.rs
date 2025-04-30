use crate::api_client::CampaignEntryWithAccountId;
use crate::error::Error;
use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{
    Array, Date64Builder, Float64Builder, RecordBatch, StringArray, StringDictionaryBuilder,
    UInt64Builder,
};
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::common::{JoinType, ScalarValue};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::functions_aggregate::sum::sum;
use datafusion::prelude::{col, lit, DataFrame, SessionContext};
use std::sync::Arc;

///
/// Enriches ads data with revenue information.
///
/// # Arguments
/// * `ads_df` - [`DataFrame`] containing the ads data
/// * `revenue_df` - [`DataFrame`] containing the revenue data
///
/// # Returns
/// A Result containing either an enriched [`DataFrame`] or an [`Error`]
pub async fn enrich_ads(ads_df: DataFrame, revenue_df: DataFrame) -> Result<DataFrame, Error> {
    let tmp_campaign_id = "rev_campaign_id";
    let tmp_date = "rev_date";

    let rev_df = revenue_df.select(vec![
        col("campaign_id").alias(tmp_campaign_id),
        col("date").alias(tmp_date),
        col("revenue"),
    ])?;

    let df = ads_df.join(
        rev_df,
        JoinType::Left,
        &["campaign_id", "date"],
        &[tmp_campaign_id, tmp_date],
        None,
    )?;

    let df = df.drop_columns(&[tmp_campaign_id, tmp_date])?;

    // Fill nulls for revenue with 0 that may occur due to the left join
    let df = df.fill_null(ScalarValue::from(0), vec!["revenue".to_owned()])?;

    let enriched_df = df
        .aggregate(
            vec![
                col("date").alias("date"),
                col("account_id").alias("account_id"),
            ],
            vec![
                sum(col("clicks")).alias("clicks"),
                sum(col("conversions")).alias("conversions"),
                sum(col("cost")).alias("cost"),
                sum(col("impressions")).alias("impressions"),
                sum(col("revenue")).alias("revenue"),
            ],
        )?
        .select(vec![
            col("account_id"),
            col("date"),
            col("clicks"),
            col("conversions"),
            col("cost"),
            col("impressions"),
            col("revenue"),
            (col("revenue") / col("cost")).alias("return_on_ad_spend"),
            (lit(100.0) * col("conversions") / col("clicks")).alias("conversion_rate"),
            (col("cost") / col("conversions")).alias("cost_per_conversion"),
            (col("impressions") / col("conversions")).alias("impressions_to_conversion_ratio"),
            (lit(100.0) * col("clicks") / col("impressions")).alias("click_through_rate"),
        ])?;

    Ok(enriched_df)
}

/// Creates the schema for ads data.
///
/// # Returns
/// An [`Arc<Schema>`] representing the ads data schema
/// containing fields such as account_id, campaign_id, date, clicks, conversions, impressions, and cost.
pub fn ads_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "account_id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "campaign_id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("date", DataType::Date64, false),
        Field::new("clicks", DataType::UInt64, false),
        Field::new("conversions", DataType::UInt64, false),
        Field::new("impressions", DataType::UInt64, false),
        Field::new("cost", DataType::Float64, false),
    ]))
}

pub fn revenue_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "campaign_id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("date", DataType::Date64, false),
        Field::new("revenue", DataType::Float64, false),
    ]))
}

/// Converts ads data to a DataFrame.
///
/// # Arguments
/// * `campaign_entries` - A slice of [`CampaignEntryWithAccountId`] containing the ads data
/// * `ctx` - A reference to the [`SessionContext`] for DataFrame operations
/// * `ads_schema` - An [`Arc<Schema>`] representing the schema of the ads data
///
/// # Returns
/// A Result containing either a [`DataFrame`] or an [`Error`]
pub fn convert_ads_data_to_df(
    campaign_entries: &[CampaignEntryWithAccountId],
    ctx: &SessionContext,
    ads_schema: Arc<Schema>,
) -> Result<DataFrame, Error> {
    let ads_record_batch = {
        let num_entries = campaign_entries.len();

        let mut account_id_builder = StringDictionaryBuilder::<Int32Type>::new();
        let mut campaign_id_builder = StringDictionaryBuilder::<Int32Type>::new();
        let mut date_builder = Date64Builder::with_capacity(num_entries);
        let mut clicks_builder = UInt64Builder::with_capacity(num_entries);
        let mut conversions_builder = UInt64Builder::with_capacity(num_entries);
        let mut impressions_builder = UInt64Builder::with_capacity(num_entries);
        let mut cost_builder = Float64Builder::with_capacity(num_entries);

        for campaign_entry in campaign_entries {
            let date_ms = parse_date_as_unix_ms(&campaign_entry.date)?;

            account_id_builder.append_value(&campaign_entry.account_id);
            campaign_id_builder.append(&campaign_entry.campaign_id)?;
            date_builder.append_value(date_ms);
            clicks_builder.append_value(campaign_entry.clicks);
            conversions_builder.append_value(campaign_entry.conversions);
            impressions_builder.append_value(campaign_entry.impressions);
            cost_builder.append_value(campaign_entry.cost);
        }

        RecordBatch::try_new(
            ads_schema.clone(),
            vec![
                Arc::new(account_id_builder.finish()),
                Arc::new(campaign_id_builder.finish()),
                Arc::new(date_builder.finish()),
                Arc::new(clicks_builder.finish()),
                Arc::new(conversions_builder.finish()),
                Arc::new(impressions_builder.finish()),
                Arc::new(cost_builder.finish()),
            ],
        )
    }?;

    let ads_df = ctx.read_batch(ads_record_batch)?;

    Ok(ads_df)
}

/// Partitions the DataFrame by account_id and saves each partition to a Parquet file.
///
/// # Arguments
/// * `df` - The DataFrame to be partitioned
/// * `output_dir` - The directory where the Parquet files will be saved
///
/// # Returns
/// A Result containing either `()` or an [`Error`]
pub async fn partition_and_save_by_account(df: DataFrame, output_dir: &str) -> Result<(), Error> {
    let distinct = df.clone().select(vec![col("account_id")])?.distinct()?;
    let batches: Vec<RecordBatch> = distinct.collect().await?;

    let mut account_ids = Vec::new();
    for batch in batches {
        let col0 = batch.column(0);

        let utf8_col = if col0.data_type() != &DataType::Utf8 {
            cast_with_options(col0.as_ref(), &DataType::Utf8, &CastOptions::default())?
        } else {
            col0.clone()
        };

        let sa = utf8_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("after cast, column must be StringArray");

        for i in 0..sa.len() {
            if sa.is_valid(i) {
                account_ids.push(sa.value(i).to_string());
            }
        }
    }

    for account_id in account_ids {
        let slice = df
            .clone()
            .filter(col("account_id").eq(lit(account_id.as_str())))?
            .drop_columns(&["account_id"])?;

        let path = format!("{}/analysis-{}.parquet", output_dir, account_id);
        slice
            .write_parquet(&path, DataFrameWriteOptions::default(), None)
            .await?;
    }

    Ok(())
}

fn parse_date_as_unix_ms(date: &str) -> Result<i64, Error> {
    let parsed_date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| Error::InvalidDate {
            date: date.to_string(),
        })?;
    let unix_duration = parsed_date - NaiveDateTime::UNIX_EPOCH.date();

    Ok(unix_duration.num_milliseconds())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::array::{Date64Array, Float64Array},
        datasource::MemTable,
        error::DataFusionError,
        prelude::SessionContext,
    };

    pub async fn make_mock_ads_df() -> Result<DataFrame, DataFusionError> {
        // Revenue schema, duplicate for now
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "campaign_id",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("date", DataType::Date64, false),
            Field::new("revenue", DataType::Float64, false),
        ]));

        let mut dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        dict_builder.append("cmp_1")?;
        dict_builder.append("cmp_2")?;
        dict_builder.append("456")?;
        let campaign_ids = dict_builder.finish();

        let dates = Date64Array::from(vec![
            Some(1609459200000),
            Some(1609545600000),
            Some(1609632000000),
        ]);

        let revenues = Float64Array::from(vec![123.4, 567.8, 90.12]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(campaign_ids), Arc::new(dates), Arc::new(revenues)],
        )?;

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("mock_ads", Arc::new(table))?;

        // 7) Return the DataFrame for further testing
        ctx.table("mock_ads").await
    }

    #[tokio::test]
    async fn test_ads_schema() {
        let schema = ads_schema();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), "account_id");
        assert_eq!(schema.field(1).name(), "campaign_id");
        assert_eq!(schema.field(2).name(), "date");
        assert_eq!(schema.field(3).name(), "clicks");
        assert_eq!(schema.field(4).name(), "conversions");
        assert_eq!(schema.field(5).name(), "impressions");
        assert_eq!(schema.field(6).name(), "cost");
    }

    #[tokio::test]
    async fn test_revenue_schema() {
        let schema = revenue_schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "campaign_id");
        assert_eq!(schema.field(1).name(), "date");
        assert_eq!(schema.field(2).name(), "revenue");
    }

    #[tokio::test]
    async fn test_parse_date_as_unix_ms() {
        let date = "2023-10-01";
        let unix_ms = parse_date_as_unix_ms(date).unwrap();
        assert_eq!(unix_ms, 1696118400000);
    }

    #[tokio::test]
    async fn test_convert_ads_data_to_df() {
        let ctx = SessionContext::new();
        let ads_schema = ads_schema();
        let campaign_entries = vec![
            CampaignEntryWithAccountId {
                account_id: "123".to_string(),
                campaign_id: "456".to_string(),
                clicks: 100,
                conversions: 10,
                cost: 50.0,
                date: "2023-10-01".to_string(),
                impressions: 1000,
            },
            CampaignEntryWithAccountId {
                account_id: "123".to_string(),
                campaign_id: "789".to_string(),
                clicks: 200,
                conversions: 20,
                cost: 100.0,
                date: "2023-10-02".to_string(),
                impressions: 2000,
            },
        ];

        let df = convert_ads_data_to_df(&campaign_entries, &ctx, ads_schema).unwrap();
        let result = df.collect().await.unwrap();
        assert_eq!(result.len(), 1); // One batch
        assert_eq!(result[0].num_rows(), 2); // Two rows
    }

    #[tokio::test]
    async fn test_partition_and_save_by_account() -> Result<(), Error> {
        let ctx = SessionContext::new();
        let ads_schema = ads_schema();
        let campaign_entries = vec![CampaignEntryWithAccountId {
            account_id: "123".to_string(),
            campaign_id: "456".to_string(),
            clicks: 100,
            conversions: 10,
            cost: 50.0,
            date: "2023-10-01".to_string(),
            impressions: 1000,
        }];

        let df = convert_ads_data_to_df(&campaign_entries, &ctx, ads_schema).unwrap();
        let output_dir = "./test_output";
        partition_and_save_by_account(df, output_dir).await?;
        assert!(std::path::Path::new(&format!("{}/analysis-123.parquet", output_dir)).exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_enrich_ads() -> Result<(), Error> {
        let ctx = SessionContext::new();

        // Create test data
        let ads_schema = ads_schema();

        let ads_entries = vec![CampaignEntryWithAccountId {
            account_id: "123".to_string(),
            campaign_id: "456".to_string(),
            clicks: 100,
            conversions: 10,
            cost: 50.0,
            date: "2023-10-01".to_string(),
            impressions: 1000,
        }];

        let ads_df = convert_ads_data_to_df(&ads_entries, &ctx, ads_schema)?;
        let revenue_df = make_mock_ads_df().await?;

        let enriched_df = enrich_ads(ads_df, revenue_df).await?;
        let result = enriched_df.collect().await?;

        assert_eq!(result.len(), 1); // One batch
        assert_eq!(result[0].num_rows(), 1); // One row

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_date() {
        let date = "2023-13-01";
        let result = parse_date_as_unix_ms(date);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidDate { date: d }) => assert_eq!(d, date),
            _ => panic!("Expected InvalidDate error"),
        }
    }

    #[tokio::test]
    async fn test_invalid_date_format() {
        let date = "2023-10-32";
        let result = parse_date_as_unix_ms(date);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidDate { date: d }) => assert_eq!(d, date),
            _ => panic!("Expected InvalidDate error"),
        }
    }

    #[tokio::test]
    async fn test_partition_and_save_by_account_no_accounts() {
        let ctx = SessionContext::new();
        let ads_schema = ads_schema();
        let campaign_entries: Vec<CampaignEntryWithAccountId> = vec![];

        let df = convert_ads_data_to_df(&campaign_entries, &ctx, ads_schema).unwrap();
        let output_dir = "./test_output_no_accounts";
        partition_and_save_by_account(df, output_dir).await.unwrap();
    }
    #[tokio::test]
    async fn test_partition_and_save_by_account_multiple_accounts() -> Result<(), Error> {
        let ctx = SessionContext::new();
        let ads_schema = ads_schema();
        let campaign_entries = vec![
            CampaignEntryWithAccountId {
                account_id: "123".to_string(),
                campaign_id: "456".to_string(),
                clicks: 100,
                conversions: 10,
                cost: 50.0,
                date: "2023-10-01".to_string(),
                impressions: 1000,
            },
            CampaignEntryWithAccountId {
                account_id: "789".to_string(),
                campaign_id: "012".to_string(),
                clicks: 200,
                conversions: 20,
                cost: 100.0,
                date: "2023-10-01".to_string(),
                impressions: 2000,
            },
        ];

        let df = convert_ads_data_to_df(&campaign_entries, &ctx, ads_schema).unwrap();
        let output_dir = "./test_output_multiple";
        partition_and_save_by_account(df, output_dir).await?;

        assert!(std::path::Path::new(&format!("{}/analysis-123.parquet", output_dir)).exists());
        assert!(std::path::Path::new(&format!("{}/analysis-789.parquet", output_dir)).exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_enrich_ads_no_revenue() -> Result<(), Error> {
        let ctx = SessionContext::new();

        // Create test data
        let ads_schema = ads_schema();

        let ads_entries = vec![CampaignEntryWithAccountId {
            account_id: "123".to_string(),
            campaign_id: "456".to_string(),
            clicks: 100,
            conversions: 10,
            cost: 50.0,
            date: "2023-10-01".to_string(),
            impressions: 1000,
        }];

        let ads_df = convert_ads_data_to_df(&ads_entries, &ctx, ads_schema)?;
        let revenue_df = make_mock_ads_df().await?;
        let enriched_df = enrich_ads(ads_df, revenue_df).await?;
        let result = enriched_df.collect().await?;

        assert_eq!(result.len(), 1); // One batch
        assert_eq!(result[0].num_rows(), 1); // One row

        Ok(())
    }

    #[tokio::test]
    async fn test_enrich_ads_no_ads() -> Result<(), Error> {
        let ctx = SessionContext::new();
        let ads_schema = ads_schema();

        let ads_entries: Vec<CampaignEntryWithAccountId> = vec![];

        let ads_df = convert_ads_data_to_df(&ads_entries, &ctx, ads_schema)?;
        let revenue_df = make_mock_ads_df().await?;

        let enriched_df = enrich_ads(ads_df, revenue_df).await?;
        let result = enriched_df.collect().await?;

        assert_eq!(result.len(), 0);

        Ok(())
    }
}
