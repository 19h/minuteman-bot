use std::sync::{Arc, Mutex};

use pw_telegram_bot_fork::UserId;
use rocksdb::{Direction, IteratorMode, ReadOptions};

use minuteman::workers::telegram_handler::LogItem;

#[tokio::main]
async fn main() {
    let mut db =
        rocksdb::DB::open_default("./db")
            .unwrap();

    let mut opts = ReadOptions::default();

    let lower_bound = format!("chat:").as_bytes().to_vec();

    opts.set_iterate_upper_bound(format!("chat:\x7f").as_bytes().to_vec());

    let mut iter =
        db.iterator_opt(
            IteratorMode::From(&lower_bound, Direction::Forward),
            opts,
        );

    for (k, v) in iter {
        if let Ok(val) = String::from_utf8(v.to_vec()) {
            let val = serde_json::from_str::<LogItem>(&val).unwrap();

            if let Ok(k) = String::from_utf8(k.to_vec()) {
                let keys = k.split(':').collect::<Vec<&str>>();

                if let Ok(date) = keys[keys.len() - 1].parse::<i64>() {
                    let date_key = date / 86_400;

                    db.put(
                        &format!("chat_index:{}:{}", keys[1], date_key),
                        "\0",
                    );
                }
            }
        }
    }
}
