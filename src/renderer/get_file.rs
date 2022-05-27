use std::num::NonZeroU16;
use std::sync::{Arc, Mutex};

use image::ImageFormat;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use warp::http::{header, HeaderValue};
use warp::http::Response;
use warp::hyper::Body;
use warp::Reply;

use crate::MinutemanError;

#[derive(Debug, Eq, PartialEq)]
pub enum FileRequestType {
    User,
    Image,
    Document,
    Video,
    VideoThumb,
    Unknown,
}

impl From<String> for FileRequestType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "user" => FileRequestType::User,
            "image" => FileRequestType::Image,
            "document" => FileRequestType::Document,
            "video" => FileRequestType::Video,
            "video_thumb" => FileRequestType::VideoThumb,
            _ => FileRequestType::Unknown,
        }
    }
}

pub async fn get_file(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    file_request_type: String,
    file_id: String,
) -> Result<impl warp::Reply, warp::Rejection> {
    let file_request_type: FileRequestType = file_request_type.into();

    if file_request_type == FileRequestType::Unknown {
        return Ok(
            Response::builder()
                .status(warp::http::status::StatusCode::NOT_FOUND)
                .body(Body::from("Unknown file request type"))
                .unwrap(),
        );
    }

    let dbi =
        db.lock()
            .map_err(|err|
                         warp::reject::custom(
                             MinutemanError::LockError(
                                 format!("{:?}", err),
                             ),
                         ),
            )?;

    let file_key =
        format!(
            "file:{}:{}",
            match file_request_type {
                FileRequestType::User => "user",
                FileRequestType::VideoThumb => "video_thumb",
                _ => "chat",
            },
            file_id,
        );

    let file_key = file_key.as_bytes();

    let file =
        dbi
            .get(file_key)
            .ok()
            .flatten();

    if file.is_none() {
        return Err(
            warp::reject::not_found(),
        );
    }

    match file {
        None =>
            Err(
                warp::reject::not_found(),
            ),
        Some(file) =>
            Ok(
                Response::builder()
                    .header(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static(
                            match file_request_type {
                                FileRequestType::User |
                                FileRequestType::Image |
                                FileRequestType::VideoThumb => {
                                    image::guess_format(
                                        file.as_slice(),
                                    )
                                        .ok()
                                        .map(|format|
                                            match format {
                                                ImageFormat::Avif => Some("image/avif"),
                                                ImageFormat::Jpeg => Some("image/jpeg"),
                                                ImageFormat::Png => Some("image/png"),
                                                ImageFormat::Gif => Some("image/gif"),
                                                ImageFormat::WebP => Some("image/webp"),
                                                ImageFormat::Tiff => Some("image/tiff"),
                                                ImageFormat::Tga => Some("image/x-tga"),
                                                ImageFormat::Dds => Some("image/vnd-ms.dds"),
                                                ImageFormat::Bmp => Some("image/bmp"),
                                                ImageFormat::Ico => Some("image/x-icon"),
                                                ImageFormat::Hdr => Some("image/vnd.radiance"),
                                                ImageFormat::OpenExr => Some("image/x-exr"),
                                                _ => None,
                                            }
                                        )
                                        .flatten()
                                        .unwrap_or("application/octet-stream")
                                }
                                FileRequestType::Document => "application/octet-stream",
                                FileRequestType::Video => "application/octet-stream",
                                FileRequestType::Unknown => "application/octet-stream",
                            }
                        ),
                    )
                    .body(Body::from(file))
                    .unwrap(),
            ),
    }
}
