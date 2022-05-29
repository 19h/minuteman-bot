pub fn get_version() -> String {
    let version = env!("CARGO_PKG_VERSION");

    format!(
        "v{}",
        version,
    )
}
