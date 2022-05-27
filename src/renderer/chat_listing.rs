use std::ops::Add;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, ReadOptions};

use crate::{GLOBAL_CSS, MinutemanError, ok_or_continue, ok_or_return, ok_or_return_none, some_or_continue};
use crate::utils::resolve_chat_name;
use crate::workers::telegram_handler::{LogItem, UserMeta};

pub async fn chat_listing(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    chat_id: String,
    date: String,
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

    let date = {
        if date == "latest" {
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

            match iter.next()
                .map(|(k, v)| String::from_utf8(k.to_vec()).ok())
                .flatten()
                .map(|key|
                    key
                        .split(':')
                        .last()
                        .map(|s|
                            s
                                .to_string()
                                .clone()
                        )
                )
                .flatten()
                .map(|day| {
                    let day = ok_or_return_none!(day.parse::<i64>()) * 86_400;
                    let day = NaiveDateTime::from_timestamp(day, 0);
                    let day: DateTime<Utc> = DateTime::from_utc(day, Utc);

                    Some(day.format("%Y-%m-%d"))
                })
                .flatten() {
                Some(day) => day.to_string(),
                None =>
                    return Ok(
                        warp::reply::html(
                            format!(
                                "<!DOCTYPE html><html lang=\"en\">{}<body><div class=\"navigation\"><span class=\"title\">{}</span> | <span class=\"nolink\">index</span> | <a href=\"/chat/{}/latest\">latest</a></div>",
                                GLOBAL_CSS,
                                resolve_chat_name(
                                    &dbi,
                                    &chat_id,
                                ),
                                chat_id,
                            ),
                        ),
                    ),
            }
        } else {
            date
        }
    };

    let mut out =
        vec!(
            "<!DOCTYPE html><html lang=\"en\">".to_string(),
            "<style type=\"text/css\">".to_string(),
            GLOBAL_CSS.to_string(),
            "</style>".to_string(),
            format!(
                "<head><title>{} - {}</title></head><body>",
                &chat_id,
                &date,
            ),
        );

    let time =
        NaiveDate::parse_from_str(
            &date,
            "%Y-%m-%d",
        );

    if time.is_err() {
        return Ok(
            warp::reply::html(
                "invalid date".to_string(),
            )
        );
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

    let mut opts = ReadOptions::default();

    let lower_bound = format!("chat:{}:{}", &chat_id, time_start).as_bytes().to_vec();

    opts.set_iterate_upper_bound(format!("chat:{}:{}", &chat_id, time_end).as_bytes().to_vec());

    let mut iter =
        dbi.iterator_opt(
            IteratorMode::From(&lower_bound, Direction::Forward),
            opts,
        );

    out.push(
        format!(
            "<div class=\"navigation\"><span class=\"title\">{} - {}</span> | <span class=\"nolink\">index</span> | {} | {} | {}</div>",
            &chat_id,
            &date,
            "<span class=\"nolink\">previous (none)</span>",
            "<span class=\"nolink\">next (none)</span>",
            &format!("<a href=\"/chat/{}/latest\">latest</a>", &chat_id),
        ),
    );

    out.push(
        "<div class=\"log\"><table class=\"log\"><tbody>".to_string(),
    );

    let mut i = 0;

    for (key, val) in iter {
        let key = key.to_vec();
        let key = String::from_utf8(key).unwrap();
        let key = key.split(":").collect::<Vec<&str>>();

        if key.len() != 3 {
            continue;
        }

        let day = key[key.len() - 1];

        let day = ok_or_continue!(day.parse::<i64>());

        let day_opt = NaiveDateTime::from_timestamp_opt(day, 0);

        if day_opt.is_none() {
            return Ok(
                warp::reply::html(
                    format!("failed to parse {} as NaiveDateTime", day),
                )
            );
        }

        let day: DateTime<Utc> = DateTime::from_utc(day_opt.unwrap(), Utc);
        let day = day.format("%H:%M:%S").to_string();

        let msg =
            ok_or_continue!(
                serde_json::from_str::<LogItem>(
                    &ok_or_continue!(
                        String::from_utf8(val.to_vec())
                    ).as_str()
                )
            );

        match msg {
            LogItem::Message { ref text, ref user_id, .. } => {
                let user =
                    dbi.get(
                        format!("user:meta:{}", user_id).as_bytes(),
                    )
                        .ok()
                        .flatten()
                        .map(|val|
                            serde_json::from_slice::<UserMeta>(
                                &val
                            )
                            .ok()
                        )
                        .flatten()
                        .unwrap_or(
                            UserMeta::default()
                                .with_id(
                                    &user_id.clone(),
                                )
                                .with_username(
                                    &user_id.clone(),
                                ),
                        );

                let username =
                    user.username
                        .as_ref()
                        .unwrap_or(&user_id);

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
            LogItem::Media { ref files, ref user_id, ref caption, .. } if caption.is_some() => {
                dbg!(&files);

                let file_uris =
                    files
                        .iter()
                        .filter_map(
                            |file|
                                dbi.get(
                                    format!("file:chat:{}", file).as_bytes(),
                                )
                                    .ok()
                                    .flatten()
                        )
                        .map(|file| base64::encode(file))
                        .map(|file| format!("data:image/jpeg;base64,{}", file))
                        .map(|file| format!("<img src=\"{}\" style=\"max-height: 300px; max-width: 300px;\"/>", file))
                        .collect::<Vec<String>>();

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
                        user_id,
                        format!(
                            "{} <br/> {}",
                            caption.as_ref().unwrap().clone(),
                            file_uris.join(" "),
                        ),
                    )
                );
            },
            _ => {}
        }

        i += 1;
    }

    out.push("</ul></div></body></html>".to_string());

    Ok(
        warp::reply::html(
            out.join(""),
        ),
    )
}
