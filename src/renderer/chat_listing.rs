use std::ops::Add;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, ReadOptions};

use crate::{GLOBAL_CSS, MinutemanError, ok_or_continue, ok_or_return, ok_or_return_none, some_or_continue};
use crate::components::header::{HeaderBar, HeaderItem};
use crate::utils::{find_latest_chat_day, resolve_chat_name, resolve_user};
use crate::workers::telegram_handler::{LogItem, LogItemMembershipType, UserMeta};

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
            match find_latest_chat_day(&dbi, &chat_id) {
                Some(day) => day.to_string(),
                None =>
                    return Ok(
                        warp::reply::html(
                            format!(
                                "<!DOCTYPE html><html lang=\"en\"><style>{}</style><body><div class=\"navigation\"><span class=\"title\">{}</span> | <span class=\"nolink\">index</span> | <a href=\"/chat/{}/latest\">latest</a></div>",
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

    let chat_name =
        resolve_chat_name(
            &dbi,
            &chat_id,
        );

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
    let upper_bound = format!("chat:{}:{}", &chat_id, time_end).as_bytes().to_vec();

    opts.set_iterate_upper_bound(upper_bound.clone());
    opts.set_iterate_lower_bound(lower_bound.clone());

    let mut iter =
        dbi.iterator_opt(
            IteratorMode::From(&upper_bound, Direction::Reverse),
            opts,
        );

    out.push(
        HeaderBar::new()
            .with_link(
                "<- home",
                Some("/".into()),
            )
            .with_title(format!("{} - {}", chat_name, date))
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

    for (key, val) in iter {
        let key = key.to_vec();
        let key = String::from_utf8(key).unwrap();
        let key = key.split(":").collect::<Vec<&str>>();

        if key.len() != 3 {
            continue;
        }

        let timestamp = key[key.len() - 1];

        let day = ok_or_continue!(timestamp.parse::<i64>());

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
    }

    out.push("</ul></div></body></html>".to_string());

    Ok(
        warp::reply::html(
            out.join(""),
        ),
    )
}
