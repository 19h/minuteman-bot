use rocksdb::{DBWithThreadMode, MultiThreaded};

use crate::workers::telegram_handler::{ChatMeta, UserMeta};

#[macro_export]
macro_rules! ok_or_continue {
    ( $x:expr $(,)? ) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                dbg!(e);

                continue;
            },
        }
    };
}

#[macro_export]
macro_rules! some_or_continue {
    ( $x:expr $(,)? ) => {
        match $x {
            Some(x) => x,
            None => continue,
        }
    };
}

#[macro_export]
macro_rules! ok_or_return_none {
    ( $x:expr $(,)? ) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                dbg!(e);

                return None;
            },
        }
    };
}

#[macro_export]
macro_rules! some_or_return_none {
    ( $x:expr $(,)? ) => {
        match $x {
            Some(x) => x,
            None => return None,
        }
    };
}

#[macro_export]
macro_rules! ok_or_return {
    ( $x:expr $(,)? ) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                dbg!(e);

                return;
            },
        }
    };
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

#[macro_export]
macro_rules! respawning_threaded_async {
    ( $x:expr, $online_msg:expr, $offline_msg:expr ) => {
        thread::spawn(
            move || {
                loop {
                    let th = thread::spawn(
                        move || {
                            println!(
                                $online_msg,
                                thread::current().id().as_u64(),
                            );

                            if let Ok(rt) = Runtime::new() {
                                rt.block_on(
                                    $x(),
                                );
                            }
                        }
                    );

                    let thread_id = th.thread().id().as_u64();

                    th.join();

                    println!(
                        $offline_msg,
                        thread_id,
                    );
                }
            }
        )
    };
}

pub fn resolve_user_meta(
    user: &UserMeta,
) -> String {
    if user.username.is_some() {
        format!(
            "{}",
            &user.username.as_ref().unwrap(),
        )
    } else if user.last_name.is_some() {
        format!(
            "{} {}",
            user.first_name,
            user.last_name.as_ref().unwrap(),
        )
    } else {
        format!(
            "{}",
            &user.first_name,
        )
    }
}

pub fn resolve_user_meta_with_id(
    user: &UserMeta,
) -> String {
    if user.username.is_some() {
        format!(
            "{} ({})",
            &user.username.as_ref().unwrap(),
            &user.id,
        )
    } else if user.last_name.is_some() {
        format!(
            "{} {} ({})",
            user.first_name,
            user.last_name.as_ref().unwrap(),
            &user.id,
        )
    } else {
        format!(
            "{} ({})",
            &user.first_name,
            &user.id,
        )
    }
}

pub fn resolve_user(
    db: &DBWithThreadMode<MultiThreaded>,
    user_id: &str,
    with_id: bool,
) -> String {
    db.get(
        format!("user:meta:{}", user_id)
            .as_bytes(),
    )
        .ok()
        .flatten()
        .map(|v|
            serde_json::from_slice::<UserMeta>(
                &v,
            )
                .ok()
        )
        .flatten()
        .map(|user|
                 if with_id {
                     resolve_user_meta_with_id(
                         &user,
                     )
                 } else {
                     resolve_user_meta(
                         &user,
                     )
                 },
        )
        .unwrap_or(
            user_id.to_string(),
        )
}

pub fn resolve_chat_name(
    db: &DBWithThreadMode<MultiThreaded>,
    chat_id: &str,
) -> String {
    db.get(
        format!("chat:meta:{}", chat_id)
            .as_bytes(),
    )
        .ok()
        .flatten()
        .map(|v|
            serde_json::from_slice::<ChatMeta>(
                &v,
            )
                .ok()
        )
        .flatten()
        .map(|meta|
            match meta {
                ChatMeta::User(user) =>
                    if user.username.is_some() {
                        format!(
                            "{} ({})",
                            &user.username.unwrap(),
                            &user.id,
                        )
                    } else if user.last_name.is_some() {
                        format!(
                            "{} {} ({})",
                            user.first_name,
                            user.last_name.unwrap(),
                            &user.id,
                        )
                    } else {
                        format!(
                            "{} ({})",
                            &user.first_name,
                            &user.id,
                        )
                    },
                ChatMeta::Group(group) =>
                    format!(
                        "{} ({})",
                        &group.title,
                        &group.id,
                    ),
                ChatMeta::SuperGroup(group) =>
                    format!(
                        "{} ({})",
                        group
                            .username
                            .unwrap_or(
                                group.title.clone()
                            ),
                        &group.id,
                    ),
                ChatMeta::Unknown(raw_chat) =>
                    if raw_chat.username.is_some() {
                        format!(
                            "{} ({})",
                            &raw_chat.username.unwrap(),
                            &raw_chat.id,
                        )
                    } else if raw_chat.last_name.is_some() && raw_chat.first_name.is_some() {
                        format!(
                            "{} {} ({})",
                            &raw_chat.first_name.unwrap(),
                            &raw_chat.last_name.unwrap(),
                            &raw_chat.id,
                        )
                    } else if raw_chat.first_name.is_some() {
                        format!(
                            "{} ({})",
                            &raw_chat.first_name.unwrap(),
                            &raw_chat.id,
                        )
                    } else if raw_chat.title.is_some() {
                        format!(
                            "{} ({})",
                            &raw_chat.title.unwrap(),
                            &raw_chat.id,
                        )
                    } else {
                        format!(
                            "{}",
                            &raw_chat.id,
                        )
                    },
            }
        )
        // try treating it as username
        .unwrap_or_else(
            || resolve_user(&db, chat_id, true),
        )
}
