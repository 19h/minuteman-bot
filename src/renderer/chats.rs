use std::sync::{Arc, Mutex};
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, ReadOptions};
use crate::{GLOBAL_CSS, MinutemanError};
use crate::utils::resolve_chat_name;

pub async fn chats(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
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
            "<div class=\"channels\"><ul>".to_string(),
        );

    let mut opts = ReadOptions::default();

    let lower_bound = b"chat_rel:".to_vec();

    opts.set_iterate_upper_bound(b"chat_rel:\xff".to_vec());

    let mut iter =
        dbi.iterator_opt(
            IteratorMode::From(&lower_bound, Direction::Forward),
            opts,
        );

    for (key, _) in iter {
        let key = key.to_vec();
        let key = String::from_utf8(key).unwrap();
        let key = key.split(":").collect::<Vec<&str>>();

        if key.len() != 2 {
            continue;
        }

        let key = key[key.len() - 1];

        let chat_name =
            resolve_chat_name(
                &dbi,
                &key,
            );

        out.push(
            format!(
                "<li><a href=\"/chat/{}/latest\">{}</a> (<a href=\"/chat/{}\">index</a> | <a href=\"/chat/{}/latest\">latest</a>)</li>",
                &key,
                &chat_name,
                &key,
                &key,
            ),
        );
    }

    out.push("</ul></div></body></html>".to_string());

    Ok(
        warp::reply::html(
            out.join(""),
        ),
    )
}
