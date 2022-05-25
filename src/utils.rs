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
