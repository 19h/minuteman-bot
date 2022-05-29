use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDateTime, Utc};
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, ReadOptions};

use crate::{GLOBAL_CSS, MinutemanError, ok_or_continue};
use crate::components::header::HeaderBar;
use crate::config::get_version;
use crate::utils::resolve_chat_name;
use crate::workers::telegram_handler::ChatMeta;

pub async fn chat_index(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    chat_id: String,
) -> Result<impl warp::Reply, warp::Rejection> {
    let dbi =
        db.lock()
            .map_err(|err|
                warp::reject::custom(
                    MinutemanError::LockError(
                        format!("{:?}", err),
                    ),
                )
            )?;

    let mut out =
        vec!(
            "<!DOCTYPE html><html lang=\"en\">".to_string(),
            "<style type=\"text/css\">".to_string(),
            GLOBAL_CSS.to_string(),
            "</style>".to_string(),
            "<head><title>channel index</title></head><body>".to_string(),
            "<div class=\"index\"><ul>".to_string(),
        );

    let mut opts = ReadOptions::default();

    let lower_bound = format!("chat_index:{}:", &chat_id).as_bytes().to_vec();
    let upper_bound = format!("chat_index:{}:\x7f", &chat_id).as_bytes().to_vec();

    opts.set_iterate_upper_bound(upper_bound.clone());
    opts.set_iterate_lower_bound(lower_bound.clone());

    let mut iter =
        dbi.iterator_opt(
            IteratorMode::From(&upper_bound, Direction::Reverse),
            opts,
        );

    let chat_name =
        resolve_chat_name(
            &dbi,
            &chat_id,
        );

    out.push(
        format!(
            "<div class=\"navigation\"><span class=\"title\">{}</span> | <span class=\"nolink\">index</span> | <a href=\"/chat/{}/latest\">latest</a></div>",
            &chat_name,
            &chat_id,
        ),
    );

    let mut i = 0;

    for (key, _) in iter {
        let key = key.to_vec();
        let key = String::from_utf8(key).unwrap();
        let key = key.split(":").collect::<Vec<&str>>();

        if key.len() != 3 {
            continue;
        }

        let day = key[key.len() - 1];

        let day = ok_or_continue!(day.parse::<i64>()) * 86_400;
        let day = NaiveDateTime::from_timestamp(day, 0);
        let day: DateTime<Utc> = DateTime::from_utc(day, Utc);
        let day = day.format("%Y-%m-%d");

        out.push(
            format!(
                "<li><a href=\"/chat/{}/{}\">{}</a>{}</li>",
                &chat_id,
                &day,
                &day,
                if i == 0 {
                    format!(
                        " (<a href=\"/chat/{}/latest\">latest</a>)",
                        &chat_id,
                    )
                } else {
                    "".to_string()
                },
            ),
        );

        i += 1;
    }

    out.push("</ul></div></body></html>".to_string());

    Ok(
        warp::reply::html(
            out.join(""),
        ),
    )
}
