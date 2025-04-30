use clap::Args as ClapArgs;

const DEFAULT_API_URL: &str = "https://sultek.data-in-stage.funnel.io";
const OUTPUT_DIR: &str = "./";
const REVENUE_FILE_PATH: &str = "./";

#[derive(ClapArgs)]
pub struct Config {
    #[arg(long, default_value=DEFAULT_API_URL, env = "API_URL")]
    pub(crate) api_url: String,

    #[arg(long, env = "API_TOKEN")]
    pub(crate) api_token: String,

    #[arg(long, default_value=REVENUE_FILE_PATH, env = "REVENUE_FILE_PATH")]
    pub(crate) revenue_file_path: String,

    #[arg(long, default_value=OUTPUT_DIR, env = "OUTPUT_DIR")]
    pub(crate) output_dir: String,
}
