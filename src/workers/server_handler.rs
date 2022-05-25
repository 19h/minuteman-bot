use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use warp::{Error, Filter};

use crate::{JOB_SLEEP_INTERVAL, renderer};

fn with_db(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
) -> impl Filter<Extract=(Arc<Mutex<DBWithThreadMode<MultiThreaded>>>, ), Error=Infallible> + Clone {
    warp::any().map(move || db.clone())
}

async fn run(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let default = warp::path::end()
        .map(|| {
            warp::reply::html(
                "yo",
            )
        });

    let chats =
        warp::path("chats")
            .and(with_db(db.clone()))
            .and_then(renderer::chats::chats);

    let routes =
        warp::get()
            .and(default)
            .or(
                chats
            );

    println!("Ain't gonna need to tell the truth, tell no lies");
    println!("Everything you think, do, and say");
    println!("Is in the pill you took today");
    println!("▪");
    println!("listening on port 2525");

    warp::serve(routes)
        .run(([0, 0, 0, 0], 12525))
        .await;

    Ok(())
}

pub async fn spawn_worker(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
) {
    loop {
        if let Err(err) = run(
            db.clone(),
        ).await {
            dbg!(err);
        }

        tokio::time::sleep(
            Duration::from_millis(
                JOB_SLEEP_INTERVAL,
            ),
        ).await;
    }
}
