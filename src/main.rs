#![feature(backtrace)]
#![feature(async_closure)]
#![feature(thread_id_value)]

use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;

use futures::StreamExt;
use pw_telegram_bot_fork::*;
use rocksdb::{DBAccess, DBWithThreadMode, MultiThreaded, SingleThreaded, ThreadMode};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

mod workers;
mod utils;
mod renderer;

const MAX_FILE_SIZE: i64 = 1024 * 1024 * 50; // 50 MB

static JOB_SLEEP_INTERVAL: u64 = 2_000u64;

const TELEGRAM_API_TOKEN: &str = "";

#[inline(always)]
fn get_telegram_api_token() -> String {
    env::var("TELEGRAM_API_TOKEN")
        .unwrap_or_else(|_| TELEGRAM_API_TOKEN.to_string())
}

#[macro_export]
macro_rules! some_or_return {
    ( $x:expr $(,)? ) => {
        match $x {
            Some(x) => x,
            None => return,
        }
    };
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().pretty().init();

    let mut db =
        Arc::new(
            Mutex::new(
                rocksdb::DB::open_default("db")
                    .unwrap(),
            ),
        );

    let server_db = db.clone();

    thread::spawn(
        move || {
            let db = server_db.clone();

            loop {
                let db = db.clone();

                let th = thread::spawn(
                    move || {
                        println!(
                            "[{}] server_handler online",
                            thread::current().id().as_u64(),
                        );

                        if let Ok(rt) = Runtime::new() {
                            rt.block_on(
                                workers::server_handler::spawn_worker(
                                    db.clone(),
                                ),
                            );
                        }
                    }
                );

                let thread_id = th.thread().id().as_u64();

                th.join();

                println!(
                    "[{}] server_handler died, restarting..",
                    thread_id,
                );
            }
        }
    );

    let telegram_db = db.clone();

    thread::spawn(
        move || {
            let db = telegram_db.clone();

            loop {
                let db = db.clone();

                let th = thread::spawn(
                    move || {
                        println!(
                            "[{}] telegram_handler online",
                            thread::current().id().as_u64(),
                        );

                        if let Ok(rt) = Runtime::new() {
                            rt.block_on(
                                workers::telegram_handler::spawn_worker(
                                    db.clone(),
                                ),
                            );
                        }
                    }
                );

                let thread_id = th.thread().id().as_u64();

                th.join();

                println!(
                    "[{}] telegram_handler died, restarting..",
                    thread_id,
                );
            }
        }
    ).join().unwrap();

    Ok(())
}
