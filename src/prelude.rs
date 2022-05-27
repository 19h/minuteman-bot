use std::env;

pub const GLOBAL_CSS: &str = include_str!("./assets/global.css");

pub const MAX_FILE_SIZE: i64 = 1024 * 1024 * 50; // 50 MB

pub static JOB_SLEEP_INTERVAL: u64 = 2_000u64;

#[inline(always)]
pub fn get_telegram_api_token() -> String {
    env::var("TELEGRAM_API_TOKEN")
        .unwrap()
}

#[derive(Debug)]
pub enum MinutemanError {
    LockError(String),
    DBError(String),
    TelegramError(String),
    ParseError(String),
    Utf8Error(String),
    Other(String),
}

impl warp::reject::Reject for MinutemanError {}
