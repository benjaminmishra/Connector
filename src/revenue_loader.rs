use crate::config::Config;
use crate::error::Error;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::{DataFrame, SessionContext};
use std::fs;

#[async_trait::async_trait]
pub trait RevenueLoader: Send + Sync + 'static {
    /// Loads revenue data from a specified source.
    ///
    /// # Arguments
    /// * `ctx` - The session context for the DataFrame operations.
    /// * `schema` - The schema of the DataFrame.
    /// * `account_ids` - A list of account IDs to load revenue data for.
    ///
    /// # Returns
    /// A Result containing either a DataFrame with the loaded revenue data or an Error.
    async fn load(
        &self,
        ctx: &SessionContext,
        schema: SchemaRef,
        account_ids: &[String],
    ) -> Result<DataFrame, Error>;
}

#[derive(Clone)]
pub struct ParquetRevenueLoader {
    file_path: String,
}

impl ParquetRevenueLoader {
    pub fn new(config: &Config) -> Self {
        ParquetRevenueLoader {
            file_path: config.revenue_file_path.clone(),
        }
    }
}

#[async_trait::async_trait]
impl RevenueLoader for ParquetRevenueLoader {
    async fn load(
        &self,
        ctx: &SessionContext,
        schema: SchemaRef,
        account_ids: &[String],
    ) -> Result<DataFrame, Error> {
        let mut merged_df: Option<DataFrame> = None;

        for account_id in account_ids {
            let parquet_path = format!("{}/revenue-{}.parquet", self.file_path, account_id);

            if fs::metadata(&parquet_path).is_err() {
                return Err(Error::RevenueFileNotFound {
                    account_id: account_id.to_string(),
                });
            }

            let revenue_df = ctx
                .read_parquet(
                    &parquet_path,
                    datafusion::prelude::ParquetReadOptions::new().schema(&schema),
                )
                .await?;

            merged_df = match merged_df {
                Some(df) => Some(df.union(revenue_df)?),
                None => Some(revenue_df),
            };
        }

        merged_df.ok_or(Error::NoData {
            message: "No revenue data found for processing".to_string(),
        })
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_parquet_revenue_loader_success() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().to_str().unwrap().to_string();

        let config = Config {
            revenue_file_path: file_path.clone(),
            api_url: "dummy_url".to_string(),
            api_token: "dummy_token".to_string(),
            output_dir: "dummy_output".to_string(),
        };

        let loader = ParquetRevenueLoader::new(&config);

        // Create test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("revenue", DataType::Float64, false),
            Field::new("account_id", DataType::Utf8, false),
        ]));

        // Create test parquet files
        let account_ids = vec!["123".to_string(), "456".to_string()];
        for account_id in &account_ids {
            let test_file = format!("{}/revenue-{}.parquet", file_path, account_id);
            fs::write(&test_file, "dummy data").unwrap();
        }

        let ctx = SessionContext::new();

        let result = loader.load(&ctx, schema, &account_ids).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_parquet_revenue_loader_missing_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().to_str().unwrap().to_string();

        let config = Config {
            revenue_file_path: file_path,
            api_token: "dummy_token".to_string(),
            api_url: "dummy_url".to_string(),
            output_dir: "dummy_output".to_string(),
        };

        let loader = ParquetRevenueLoader::new(&config);

        let schema = Arc::new(Schema::new(vec![
            Field::new("revenue", DataType::Float64, false),
        ]));

        let ctx = SessionContext::new();
        let account_ids = vec!["nonexistent".to_string()];

        let result = loader.load(&ctx, schema, &account_ids).await;
        assert!(matches!(
            result.unwrap_err(),
            Error::RevenueFileNotFound { account_id } if account_id == "nonexistent"
        ));
    }

    #[tokio::test]
    async fn test_parquet_revenue_loader_empty_accounts() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().to_str().unwrap().to_string();

        let config = Config {
            revenue_file_path: file_path,
            api_token: "dummy_token".to_string(),
            api_url: "dummy_url".to_string(),
            output_dir: "dummy_output".to_string(),
        };

        let loader = ParquetRevenueLoader::new(&config);

        let schema = Arc::new(Schema::new(
            vec![Field::new("revenue", DataType::Float64, false)]));

        let ctx = SessionContext::new();
        let account_ids: Vec<String> = vec![];

        let result = loader.load(&ctx, schema, &account_ids).await;
        assert!(matches!(
            result.unwrap_err(),
            Error::NoData { message } if message == "No revenue data found for processing"
        ));
    }
}
