use std::ops::Add;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, ReadOptions};
use serde_json::{json, Value};
use warp::Reply;

use crate::{GLOBAL_CSS, MinutemanError, ok_or_continue, ok_or_return, ok_or_return_none, some_or_continue};
use crate::components::header::{HeaderBar, HeaderItem};
use crate::utils::{find_latest_chat_day, resolve_chat_name, resolve_user};
use crate::workers::telegram_handler::{LogItem, LogItemMediaType, LogItemMembershipType, UserMeta};

pub fn chat_listing_iter(
    dbi: &DBWithThreadMode<MultiThreaded>,
    chat_id: &str,
    time_start: &str,
    time_end: &str,
    mut cb: impl FnMut(&str, &[u8]) -> (),
) {
    let mut opts = ReadOptions::default();

    let lower_bound = format!("chat:{}:{}", &chat_id, time_start).as_bytes().to_vec();
    let upper_bound = format!("chat:{}:{}", &chat_id, time_end).as_bytes().to_vec();

    opts.set_iterate_upper_bound(upper_bound.clone());
    opts.set_iterate_lower_bound(lower_bound.clone());

    let mut iter =
        dbi.iterator_opt(
            IteratorMode::From(&upper_bound, Direction::Reverse),
            opts,
        );

    for (key, val) in iter {
        let key = key.to_vec();
        let key = String::from_utf8(key).unwrap();
        let key = key.split(":").collect::<Vec<&str>>();

        if key.len() != 3 {
            continue;
        }

        let timestamp = key[key.len() - 1];

        cb(
            timestamp,
            &val,
        );
    }
}

pub async fn chat_listing(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    chat_id: String,
    date_query: String,
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

    let mut out_format = "html";

    let date =
        if date_query.starts_with("latest") {
            match find_latest_chat_day(&dbi, &chat_id) {
                Some(day) => day,
                None =>
                    return Ok(
                        warp::reply::html(
                            format!(
                                "<!DOCTYPE html><html lang=\"en\"><style>{}</style><body>{}",
                                GLOBAL_CSS,
                                HeaderBar::new()
                                    .with_link(
                                        "<- home",
                                        Some("/".into()),
                                    )
                                    .with_title(
                                        resolve_chat_name(
                                            &dbi,
                                            &chat_id,
                                        )
                                    )
                                    .with_link(
                                        "index",
                                        Some(format!("/chat/{}", chat_id)),
                                    )
                                    .with_link(
                                        "latest",
                                        Some(format!("/chat/{}/latest", chat_id)),
                                    )
                                    .to_string(),
                            ),
                        ).into_response(),
                    ),
            }
        } else {
            date_query.clone()
        };

    if date_query.ends_with(".json") {
        out_format = "json";
    }

    let chat_name =
        resolve_chat_name(
            &dbi,
            &chat_id,
        );

    let time =
        NaiveDate::parse_from_str(
            &date,
            "%Y-%m-%d",
        );

    if time.is_err() {
        return if out_format == "html" {
            Ok(
                warp::reply::html(
                    "invalid date".to_string(),
                ).into_response()
            )
        } else {
            Ok(
                warp::reply::json(
                    &json!({
                            "status": format!("invalid date (got {})", &date),
                            "error": true,
                            "data": null
                        }),
                ).into_response()
            )
        }
    }

    let time_start = {
        let time =
            NaiveDateTime::new(
                time.unwrap(),
                NaiveTime::from_hms(0, 0, 0),
            );

        let time_start =
            time.timestamp_millis()
                .to_string();

        time_start.trim_end_matches(|c: char| c == '0').to_string()
    };

    let time_end = {
        let time =
            NaiveDateTime::new(
                time.unwrap().add(chrono::Duration::days(1)),
                NaiveTime::from_hms(0, 0, 0),
            );

        let time_start =
            time.timestamp_millis()
                .to_string();

        time_start.trim_end_matches(|c: char| c == '0').to_string()
    };

    if out_format == "json" {
        let mut out = Vec::<Value>::new();

        chat_listing_iter(
            &dbi,
            &chat_id,
            &time_start,
            &time_end,
            |timestamp, val| {
                if let Ok(val) = String::from_utf8(
                    val.to_vec(),
                ) {
                    let val =
                        &serde_json::from_str::<LogItem>(
                            &val,
                        )
                            .ok()
                            .map(|mut val|
                                match val {
                                    LogItem::Media { ref mut files, ref media_type, .. } => {
                                        match media_type {
                                            LogItemMediaType::Image { .. }
                                            | LogItemMediaType::Sticker { .. } => {
                                                files
                                                    .iter_mut()
                                                    .for_each(|file| {
                                                        *file = format!("/file/image/{}", file)
                                                    });

                                                val
                                            },
                                            _ => val,
                                        }
                                    }
                                    _ => val,
                                }
                            )
                            .map(|val| serde_json::to_value(val).ok())
                            .flatten();

                    if let Some(val) = val {
                        out.push(val.clone());
                    }
                }
            },
        );

        return Ok(
            warp::reply::json(
                &Value::Array(out),
            ).into_response(),
        );
    }

    let mut out =
        vec!(
            "<!DOCTYPE html><html lang=\"en\">".to_string(),
            "<style type=\"text/css\">".to_string(),
            GLOBAL_CSS.to_string(),
            "</style>".to_string(),
            format!(
                "<head><title>{} - {}</title></head><body>",
                &chat_name,
                &date,
            ),
        );

    out.push(
        HeaderBar::new()
            .with_link(
                "<- home",
                Some("/".into()),
            )
            .with_title(format!("{} - {}", &chat_name, &date))
            .with_link(
                "index",
                Some(format!("/chat/{}", chat_id)),
            )
            .with_link("previous", None)
            .with_link("next", None)
            .with_link(
                "latest",
                Some(format!("/chat/{}/latest", chat_id)),
            )
            .into()
    );

    out.push(
        "<div class=\"log\"><table class=\"log\"><tbody>".to_string(),
    );

    let mut i = 0;

    chat_listing_iter(
        &dbi,
        &chat_id,
        &time_start,
        &time_end,
        |timestamp, val| {
            let day =
                ok_or_return!(
                    timestamp.parse::<i64>(),
                );

            let day_opt = NaiveDateTime::from_timestamp_opt(day, 0);

            if day_opt.is_none() {
                return;
            }

            let day: DateTime<Utc> = DateTime::from_utc(day_opt.unwrap(), Utc);
            let day = day.format("%H:%M:%S").to_string();

            let msg =
                if let Some(msg) = String::from_utf8(val.to_vec())
                    .ok()
                    .map(|s| serde_json::from_str::<LogItem>(&s).ok())
                    .flatten() {
                    msg
                } else {
                    return;
                };


            match msg {
                LogItem::Message { ref text, ref user_id, .. } => {
                    let username =
                        if let Some(user_id) = user_id {
                            resolve_user(
                                &dbi,
                                user_id,
                                false,
                            )
                        } else {
                            "Unknown".to_string()
                        };

                    out.push(
                        format!(
                            "<tr class=\"message\">\
                            <td class=\"time\">\
                                <a>{}</a>\
                            <td>\
                            <td class=\"nick\">{}</td>\
                            <td class=\"content\">{}</td>\
                        </tr>",
                            day,
                            &username,
                            text,
                        )
                    );
                },
                LogItem::Media { ref files, ref user_id, ref caption, .. } => {
                    let file_uris =
                        files
                            .iter()
                            .last()
                            .map(|file| format!("/file/image/{}", file))
                            .map(|file| format!("<img src=\"{}\" style=\"max-height: 300px; max-width: 300px;\" loading=\"lazy\"/>", file))
                            .map(|file| vec!(file))
                            .unwrap_or(vec!());

                    let username =
                        if let Some(user_id) = user_id {
                            resolve_user(
                                &dbi,
                                user_id,
                                false,
                            )
                        } else {
                            "Unknown".to_string()
                        };

                    let media_caption =
                        if let Some(caption) = caption {
                            &*caption
                        } else {
                            "<span class=\"note\">Message has no caption.</span>"
                        };

                    out.push(
                        format!(
                            "<tr class=\"message action\">\
                            <td class=\"time\">\
                                <a>{}</a>\
                            <td>\
                            <td class=\"nick\">{}</td>\
                            <td class=\"content\">{}</td>\
                        </tr>",
                            day,
                            &username,
                            format!(
                                "{} <br/> {}",
                                &media_caption,
                                file_uris.join(" "),
                            ),
                        )
                    );
                },
                LogItem::Membership { ref user_id, ref membership_type, .. } => {
                    dbg!(&user_id, &membership_type);

                    let username =
                        if let Some(user_id) = user_id {
                            resolve_user(
                                &dbi,
                                user_id,
                                false,
                            )
                        } else {
                            "Unknown".to_string()
                        };

                    out.push(
                        format!(
                            "<tr class=\"{}\">\
                            <td class=\"time\">\
                                <a>{}</a>\
                            <td>\
                            <td class=\"nick\">{}</td>\
                            <td class=\"content\"><span class=\"reason\">{}</span></td>\
                        </tr>",
                            match membership_type {
                                LogItemMembershipType::Joined => "join",
                                LogItemMembershipType::Left => "leave",
                            },
                            day,
                            &username,
                            match membership_type {
                                LogItemMembershipType::Joined => "joined the chat",
                                LogItemMembershipType::Left => "left the chat",
                            },
                        )
                    );
                }
                _ => {}
            }

            i += 1;
        },
    );

    out.push("</ul></div></body></html>".to_string());

    Ok(
        warp::reply::html(
            out.join(""),
        ).into_response()
    )
}
