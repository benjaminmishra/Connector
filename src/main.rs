mod api_client;
mod config;
mod data;
mod error;
mod revenue_loader;
mod runner;

use chrono::NaiveDate;
use clap::{Parser, Subcommand};
use config::Config;
use error::Error;
use log::error;

#[derive(Parser)]
struct Args {
    #[command(flatten)]
    config: Config,

    #[command(subcommand)]
    command: Fetch,
}

#[derive(Subcommand)]
enum Fetch {
    Fetch {
        #[arg(help = "Date should be in the form YYYY-MM-DD", value_parser = validate_date)]
        start: NaiveDate,

        #[arg(help = "Date should be in the form YYYY-MM-DD", value_parser = validate_date)]
        end: NaiveDate,
    },
}

fn validate_date(s: &str) -> Result<NaiveDate, String> {
    let error_message = "Invalid date, expected YYYY-MM-DD";

    let parts = s
        .split("-")
        .map(|part| part.parse::<u16>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| error_message)?;

    match parts.as_slice() {
        &[year, month, day] if month <= 12 && day <= 31 => {
            Ok(
                NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                    .ok_or(error_message)?,
            )
        }
        _ => Err(error_message.to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();

    env_logger::init();

    match &args.command {
        Fetch::Fetch { start, end } => {
            if let Err(err) = runner::load_and_enrich_ads_data(args.config, start, end).await {
                error!("failed to load and enrich data: {}", err);
                std::process::exit(1);
            }
        }
    };

    Ok(())
}
