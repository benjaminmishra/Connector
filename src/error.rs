use datafusion::{arrow::error::ArrowError, error::DataFusionError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("DataFusion: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("Arrow: {0}")]
    Arrow(#[from] ArrowError),

    #[error("'The date supplied {date} is invalid'")]
    InvalidDate { date: String },

    #[error("The start date: '{start_date}' is greater than the end date: '{end_date}'")]
    StartDateAfterEndDate {
        start_date: String,
        end_date: String,
    },

    #[error("API {0} responded with error: {0}")]
    ApiFailure(#[from] reqwest::Error),

    #[error("Failed to parse URL: {0}")]
    UrlParsingFailed(#[from] url::ParseError),

    #[error("Revenue file for account {account_id} not found")]
    RevenueFileNotFound { account_id: String },

    #[error("{message}")]
    NoData { message: String },
}
