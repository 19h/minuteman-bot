use std::sync::{Arc, Mutex};
use rocksdb::{DBWithThreadMode, MultiThreaded};

pub async fn chats(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(
        warp::reply::html(
            "yo",
        ),
    )
}
