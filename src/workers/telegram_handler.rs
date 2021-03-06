use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use pw_telegram_bot_fork::*;
use pw_telegram_bot_fork::{Api, GetUserProfilePhotos, Message, MessageEntityKind, MessageKind, MessageText, PhotoSize, PollType, ToFileRef, ToMessageId, UpdateKind, User};
use rocksdb::{DBWithThreadMode, MultiThreaded};
use serde::{Deserialize, Serialize};

use crate::{get_telegram_api_token, JOB_SLEEP_INTERVAL, MAX_FILE_SIZE, ok_or_continue, ok_or_return_none, some_or_return_none};

pub fn build_file_url(
    file_path: &str,
) -> String {
    format!(
        "https://api.telegram.org/file/bot{}/{}",
        get_telegram_api_token(),
        file_path,
    )
}

pub async fn get_file(
    file_path: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let url = build_file_url(file_path);
    let mut response = reqwest::get(&url).await?;

    let mut out = response.bytes_stream();

    let mut buffer = Vec::new();

    while let Some(chunk) = out.next().await {
        buffer.extend(chunk?);
    }

    Ok(buffer)
}

pub async fn get_file_path(
    api: &Api,
    file: &impl ToFileRef,
) -> Option<String> {
    api.send(
        GetFile::new(
            file,
        ),
    )
        .await
        .ok()
        .map(|file| file.file_path)
        .flatten()
}

pub async fn extract_file_paths(
    api: &Api,
    message: &InterMessage,
) -> Vec<(String, String)> {
    let mut file_refs = Vec::<(String, String)>::new();

    match message.kind {
        MessageKind::Audio { ref data, .. } => {
            match data.file_size {
                Some(x) if x <= MAX_FILE_SIZE => {}
                Some(x) if x > MAX_FILE_SIZE => return file_refs,
                _ => return file_refs,
            }

            file_refs.push(
                (
                    data.file_id.clone(),
                    match get_file_path(&api, &data).await {
                        Some(x) => x,
                        None => return file_refs,
                    },
                ));
        }
        MessageKind::Voice { ref data, .. } => {
            match data.file_size {
                Some(x) if x <= MAX_FILE_SIZE => {}
                Some(x) if x > MAX_FILE_SIZE => return file_refs,
                _ => return file_refs,
            }

            file_refs.push(
                (
                    data.file_id.clone(),
                    match get_file_path(&api, &data).await {
                        Some(x) => x,
                        None => return file_refs,
                    },
                ));
        }
        MessageKind::Photo { ref data, .. } => {
            for photo in data {
                match photo.file_size {
                    Some(x) if x <= MAX_FILE_SIZE => {}
                    Some(x) if x > MAX_FILE_SIZE => continue,
                    _ => continue,
                }

                file_refs.push(
                    (
                        photo.file_id.clone(),
                        match get_file_path(&api, &photo).await {
                            Some(x) => x,
                            None => continue,
                        },
                    ));
            }
        }
        // this doesn't implement support for videos on purpose
        // because they're usually huge and we're not doing any
        // streaming here
        _ => {}
    }

    file_refs
}

pub async fn get_files(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    api: &Api,
    message: &InterMessage,
) -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::<(String, Vec<u8>)>::new();

    for (file_id, file_path) in extract_file_paths(&api, message).await {
        let file = {
            let db = db.lock().unwrap();

            if db.key_may_exist(&file_id) {
                println!("{} already exists, returning from db", &file_id);

                db.get(
                    build_file_key(
                        FileEntryType::User,
                        &file_id,
                    ),
                )
                    .ok()
                    .flatten()
            } else {
                None
            }
        };

        let file =
            if file.is_none() {
                get_file(
                    &file_path,
                )
                    .await
                    .ok()
            } else {
                file
            };

        file.map(|entry| {
            files.push(
                (
                    file_id,
                    entry,
                ),
            );
        });
    }

    files
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserMeta {
    pub id: String,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub is_bot: bool,
    pub language_code: Option<String>,
}

impl Default for UserMeta {
    fn default() -> Self {
        Self {
            id: String::new(),
            first_name: String::new(),
            last_name: None,
            username: None,
            is_bot: false,
            language_code: None,
        }
    }
}

impl ToUserId for UserMeta {
    fn to_user_id(&self) -> UserId {
        UserId(
            self.id.clone().parse::<i64>()
                .unwrap_or_else(|e| {
                    eprintln!(
                        "WARNING: ERROR: TRIED TO PARSE USER ID, BUT FAILED: {} -- {:?}",
                        self.id,
                        e,
                    );

                    0i64
                }),
        )
    }
}

impl UserMeta {
    pub fn with_id(
        self,
        user_id: &str,
    ) -> Self {
        let mut meta = self;

        meta.id = user_id.to_string().clone();

        meta
    }

    pub fn with_username(
        self,
        username: &str,
    ) -> Self {
        let mut meta = self;

        meta.username = Some(username.to_string().clone());

        meta
    }
}

impl From<User> for UserMeta {
    fn from(user: User) -> Self {
        Self {
            id: user.id.to_string(),
            first_name: user.first_name,
            last_name: user.last_name,
            username: user.username,
            is_bot: user.is_bot,
            language_code: user.language_code,
        }
    }
}

impl From<&User> for UserMeta {
    fn from(user: &User) -> Self {
        user.clone().into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMeta {
    pub id: String,
    pub title: String,
    pub all_members_are_administrators: bool,
    pub invite_link: Option<String>,
}

impl From<Group> for GroupMeta {
    fn from(group: Group) -> Self {
        Self {
            id: group.id.to_string(),
            title: group.title,
            all_members_are_administrators: group.all_members_are_administrators,
            invite_link: group.invite_link,
        }
    }
}

impl From<&Group> for GroupMeta {
    fn from(group: &Group) -> Self {
        group.clone().into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuperGroupMeta {
    pub id: String,
    pub title: String,
    pub username: Option<String>,
    pub invite_link: Option<String>,
}

impl From<Supergroup> for SuperGroupMeta {
    fn from(group: Supergroup) -> Self {
        Self {
            id: group.id.to_string(),
            title: group.title,
            username: group.username,
            invite_link: group.invite_link,
        }
    }
}

impl From<&Supergroup> for SuperGroupMeta {
    fn from(group: &Supergroup) -> Self {
        group.clone().into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelMeta {
    pub id: String,
    pub title: String,
    pub username: Option<String>,
    pub invite_link: Option<String>,
}

impl From<Channel> for ChannelMeta {
    fn from(chan: Channel) -> Self {
        Self {
            id: chan.id.clone().to_string(),
            title: chan.title,
            username: chan.username,
            invite_link: chan.invite_link,
        }
    }
}

impl From<&Channel> for ChannelMeta {
    fn from(chan: &Channel) -> Self {
        chan.clone().into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawChatMeta {
    pub id: String,
    pub chat_type: String,
    pub title: Option<String>,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub invite_link: Option<String>,
    pub language_code: Option<String>,
    pub all_members_are_administrators: Option<bool>,
}

impl From<RawChat> for RawChatMeta {
    fn from(raw_chat: RawChat) -> Self {
        Self {
            id: raw_chat.id.to_string(),
            chat_type: raw_chat.type_,
            title: raw_chat.title,
            username: raw_chat.username,
            first_name: raw_chat.first_name,
            last_name: raw_chat.last_name,
            invite_link: raw_chat.invite_link,
            language_code: raw_chat.language_code,
            all_members_are_administrators: raw_chat.all_members_are_administrators,
        }
    }
}

impl From<&RawChat> for RawChatMeta {
    fn from(chat: &RawChat) -> Self {
        chat.clone().into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChatMeta {
    User(UserMeta),
    Group(GroupMeta),
    SuperGroup(SuperGroupMeta),
    Channel(ChannelMeta),
    Unknown(RawChatMeta),
}

impl ChatMeta {
    pub fn id(&self) -> String {
        match self {
            ChatMeta::User(user) => user.id.clone(),
            ChatMeta::Group(group) => group.id.clone(),
            ChatMeta::SuperGroup(group) => group.id.clone(),
            ChatMeta::Channel(channel) => channel.id.clone(),
            ChatMeta::Unknown(raw_chat) => raw_chat.id.clone(),
        }
    }
}

impl From<MessageChat> for ChatMeta {
    fn from(chat: MessageChat) -> Self {
        match chat {
            MessageChat::Private(user) => ChatMeta::User(user.into()),
            MessageChat::Group(group) => ChatMeta::Group(group.into()),
            MessageChat::Supergroup(group) => ChatMeta::SuperGroup(group.into()),
            MessageChat::Unknown(raw_chat) => ChatMeta::Unknown(raw_chat.into()),
        }
    }
}

impl From<&MessageChat> for ChatMeta {
    fn from(chat: &MessageChat) -> Self {
        chat.clone().into()
    }
}

impl From<Channel> for ChatMeta {
    fn from(chan: Channel) -> Self {
        ChatMeta::Channel(chan.into())
    }
}

impl From<&Channel> for ChatMeta {
    fn from(chan: &Channel) -> Self {
        ChatMeta::Channel(chan.clone().into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForwardFromMeta {
    User {
        user: UserMeta,
    },
    Channel {
        channel: ChannelMeta,
        message_id: i64,
    },
    ChannelHiddenUser {
        sender_name: String,
    },
    HiddenGroupAdmin {
        chat_id: String,
        title: String,
    },
}

impl From<ForwardFrom> for ForwardFromMeta {
    fn from(forward_from: ForwardFrom) -> Self {
        match forward_from {
            ForwardFrom::User { ref user } =>
                ForwardFromMeta::User {
                    user: user.into(),
                },

            ForwardFrom::Channel { ref channel, ref message_id } =>
                ForwardFromMeta::Channel {
                    channel: channel.into(),
                    message_id: *message_id,
                },

            ForwardFrom::ChannelHiddenUser { ref sender_name } =>
                ForwardFromMeta::ChannelHiddenUser {
                    sender_name: sender_name.clone(),
                },

            ForwardFrom::HiddenGroupAdmin { ref chat_id, ref title } =>
                ForwardFromMeta::HiddenGroupAdmin {
                    chat_id: chat_id.to_string().clone(),
                    title: title.clone(),
                },
        }
    }
}

impl From<&ForwardFrom> for ForwardFromMeta {
    fn from(forward_from: &ForwardFrom) -> Self {
        forward_from.clone().into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardMeta {
    pub date: i64,
    pub from: ForwardFromMeta,
}

impl From<Forward> for ForwardMeta {
    fn from(forward: Forward) -> Self {
        ForwardMeta {
            date: forward.date,
            from: forward.from.into(),
        }
    }
}

impl From<&Forward> for ForwardMeta {
    fn from(forward: &Forward) -> Self {
        forward.clone().into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterMessage {
    pub id: MessageId,
    pub from: Option<UserMeta>,
    pub date: i64,
    pub chat: ChatMeta,
    pub forward: Option<ForwardMeta>,
    pub reply_to_message: Option<Box<InterMessage>>,
    pub edit_date: Option<i64>,
    pub kind: MessageKind,
}

impl From<Message> for InterMessage {
    fn from(msg: Message) -> Self {
        InterMessage {
            id: msg.id,
            from: Some(msg.from.into()),
            date: msg.date,
            chat: msg.chat.into(),
            forward:
            msg.forward
                .as_ref()
                .map(|forward|
                         forward.into(),
                ),
            reply_to_message:
            msg
                .reply_to_message
                .map(|reply| -> InterMessage {
                    reply.into()
                })
                .map(|val| Box::new(val)),
            edit_date: msg.edit_date,
            kind: msg.kind,
        }
    }
}

impl From<&Message> for InterMessage {
    fn from(msg: &Message) -> Self {
        msg.clone().into()
    }
}

impl From<ChannelPost> for InterMessage {
    fn from(msg: ChannelPost) -> Self {
        InterMessage {
            id: msg.id,
            from: None,
            date: msg.date,
            chat: msg.chat.into(),
            forward:
            msg
                .forward
                .as_ref()
                .map(|forward|
                         forward.into(),
                ),
            reply_to_message:
            msg
                .reply_to_message
                .map(|reply| -> InterMessage {
                    reply.into()
                })
                .map(|val| Box::new(val)),
            edit_date: msg.edit_date,
            kind: msg.kind,
        }
    }
}

impl From<&ChannelPost> for InterMessage {
    fn from(chan: &ChannelPost) -> Self {
        chan.clone().into()
    }
}

impl From<Box<MessageOrChannelPost>> for InterMessage {
    fn from(msg: Box<MessageOrChannelPost>) -> Self {
        match msg.as_ref() {
            MessageOrChannelPost::Message(message) =>
                message.into(),
            MessageOrChannelPost::ChannelPost(post) =>
                post.into(),
        }
    }
}

impl From<&Box<MessageOrChannelPost>> for InterMessage {
    fn from(msg: &Box<MessageOrChannelPost>) -> Self {
        msg.clone().into()
    }
}

pub enum FileEntryType {
    Chat,
    VideoThumb,
    User,
}

pub fn build_file_key(
    file_entry_type: FileEntryType,
    file_id: &str,
) -> String {
    match file_entry_type {
        FileEntryType::Chat => format!("file:chat:{}", file_id),
        FileEntryType::VideoThumb => format!("file:video_thumb:{}", file_id),
        FileEntryType::User => format!("file:user:{}", file_id),
    }
}

pub fn find_biggest_photo(
    photos: &Vec<PhotoSize>,
) -> PhotoSize {
    let mut photo: Option<&PhotoSize> = None;

    let mut last_size = 0;

    for size in photos.iter() {
        if last_size == 0 {
            photo = Some(size);
            last_size = size.width * size.height;

            continue;
        }

        if size.width * size.height > last_size {
            photo = Some(size);
            last_size = size.width * size.height;
        }
    }

    photo.unwrap().clone()
}

pub async fn process_user_profile_picture(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    api: &Api,
    user: &UserMeta,
) -> Result<(), Box<dyn std::error::Error>> {
    let user_profile_photos =
        api.send(
            GetUserProfilePhotos::new(
                &user,
            ),
        ).await?;

    if user_profile_photos.photos.len() == 0 {
        return Ok(());
    }

    if let Some(photo_sizes) = user_profile_photos.photos.first() {
        let photo =
            find_biggest_photo(
                &photo_sizes,
            );

        if let Some(file_path) = get_file_path(&api, &photo).await {
            let file =
                get_file(
                    &file_path,
                ).await?;

            // check image integrity
            if image::load_from_memory(&file).is_ok() {
                let db = db.lock().unwrap();

                db.put(
                    build_file_key(
                        FileEntryType::User,
                        &user.id.to_string(),
                    ),
                    &file,
                )?;
            }
        }
    }

    Ok(())
}

pub async fn process_user_meta(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    user: &UserMeta,
) -> Result<UserMeta, Box<dyn std::error::Error>> {
    let user = user.clone();

    let user_meta =
        UserMeta {
            id: user.id.to_string(),
            first_name: user.first_name,
            last_name: user.last_name,
            username: user.username,
            is_bot: user.is_bot,
            language_code: user.language_code,
        };

    let db = db.lock().unwrap();

    db.put(
        format!("user:meta:{}", user.id),
        &serde_json::to_string(&user_meta)?,
    )?;

    Ok(user_meta)
}

pub async fn process_user(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    api: &Api,
    user: &UserMeta,
) -> Result<(), Box<dyn std::error::Error>> {
    process_user_profile_picture(db.clone(), api, user).await;
    process_user_meta(db.clone(), user).await;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogItemMediaType {
    Image {
        width: i64,
        height: i64,
    },
    Video {
        duration: i64,
        width: i64,
        height: i64,
        thumb_file_id: Option<String>,
        mime_type: Option<String>,
    },
    Audio {
        duration: i64,
        performer: Option<String>,
        title: Option<String>,
        mime_type: Option<String>,
    },
    Voice {
        duration: i64,
        mime_type: Option<String>,
    },
    VideoNote {
        duration: i64,
        thumb_file_id: Option<String>,
    },
    Document {
        file_name: Option<String>,
        mime_type: Option<String>,
    },
    Sticker {
        emoji: Option<String>,
        set_name: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogItemMembershipType {
    Left,
    Joined,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct LogItemSpecialTypeLocation {
    pub longitude: f32,
    pub latitude: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct LogItemSpecialTypePollOption {
    pub text: String,
    pub voter_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogItemMessageEntityKind {
    Mention,
    Hashtag,
    BotCommand,
    Url,
    Email,
    Bold,
    Italic,
    Code,
    Pre,
    TextLink(String),
    //TextMention(User),
    TextMention(String),
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct LogItemSpecialTypePollMessageEntity {
    pub offset: i64,
    pub length: i64,
    pub kind: LogItemMessageEntityKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogItemSpecialType {
    Contact {
        user_id: Option<i64>,
        phone_number: String,
        first_name: String,
        last_name: Option<String>,
    },
    Location {
        latitude: f32,
        longitude: f32,
    },
    Venue {
        location: LogItemSpecialTypeLocation,
        title: String,
        address: String,
        foursquare_id: Option<String>,
    },
    Poll {
        id: String,
        question: String,
        options: Vec<LogItemSpecialTypePollOption>,
        total_voter_count: i64,
        is_closed: bool,
        is_anonymous: bool,
        poll_type: PollType,
        allows_multiple_answers: bool,
        correct_option_id: Option<i64>,
        explanation: Option<String>,
        explanation_entities: Option<Vec<LogItemSpecialTypePollMessageEntity>>,
        open_period: Option<i64>,
        close_date: Option<i64>,
    },
    PinnnedMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogItemChatType {
    NewTitle {
        title: String,
    },
    NewPhoto {
        file_id: Option<String>,
    },
    DeletePhoto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogItemMessageEntity {
    pub offset: i64,
    pub length: i64,
    pub kind: LogItemMessageEntityKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogItem {
    Message {
        user_id: Option<String>,
        time: i64,
        text: String,
        entities: Vec<LogItemMessageEntity>,
        source: Option<InterMessage>,
    },
    Media {
        user_id: Option<String>,
        time: i64,
        caption: Option<String>,
        #[serde(rename = "type")]
        media_type: LogItemMediaType,
        files: Vec<String>,
        source: Option<InterMessage>,
    },
    Special {
        user_id: Option<String>,
        time: i64,
        #[serde(rename = "type")]
        special_type: LogItemSpecialType,
        source: Option<InterMessage>,
    },
    Membership {
        user_id: Option<String>,
        time: i64,
        #[serde(rename = "type")]
        membership_type: LogItemMembershipType,
        source: Option<InterMessage>,
    },
    Chat {
        user_id: Option<String>,
        time: i64,
        #[serde(rename = "type")]
        chat_type: LogItemChatType,
        source: Option<InterMessage>,
    },
    Pin {
        user_id: Option<String>,
        time: i64,
        message: Option<String>,
        message_id: String,
        source: Option<InterMessage>,
    },
    Unimplemented(String, Option<String>, i64, Option<InterMessage>),
}

async fn process_files(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    api: &Api,
    message: &InterMessage,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let file_refs =
        get_files(
            db.clone(),
            api,
            message,
        ).await;

    for (file_id, file) in file_refs.iter() {
        let db = db.lock().unwrap();

        db.put(
            build_file_key(
                FileEntryType::Chat,
                &file_id.to_string(),
            ),
            &file,
        )?;
    }

    Ok(
        file_refs
            .iter()
            .map(|(file_id, _)|
                file_id.to_string()
            )
            .collect(),
    )
}

pub async fn process_photosize(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    api: &Api,
    photo_size: &PhotoSize,
    file_id: Option<&str>,
) -> Option<String> {
    let file_key =
        build_file_key(
            FileEntryType::VideoThumb,
            file_id
                .unwrap_or(
                    &photo_size
                        .file_id
                        .to_string(),
                ),
        );

    {
        let db = db.lock().unwrap();

        if db.key_may_exist(&file_key) {
            println!("{} already exists, not trying to fetch", file_key);

            return Some(photo_size.file_id.clone());
        }
    }

    if let Some(file_path) = get_file_path(&api, &photo_size).await {
        let file =
            match get_file(
                &file_path,
            ).await {
                Ok(file) => file,
                Err(_) => return None,
            };

        // check image integrity
        if image::load_from_memory(&file).is_ok() {
            let db = db.lock().unwrap();

            db.put(
                file_key,
                &file,
            ).ok()?;

            Some(photo_size.file_id.clone())
        } else {
            None
        }
    } else {
        None
    }
}

pub fn map_entity_kind(
    kind: &MessageEntityKind,
) -> LogItemMessageEntityKind {
    match kind {
        MessageEntityKind::Mention =>
            LogItemMessageEntityKind::Mention,
        MessageEntityKind::Hashtag =>
            LogItemMessageEntityKind::Hashtag,
        MessageEntityKind::BotCommand =>
            LogItemMessageEntityKind::BotCommand,
        MessageEntityKind::Url =>
            LogItemMessageEntityKind::Url,
        MessageEntityKind::Email =>
            LogItemMessageEntityKind::Email,
        MessageEntityKind::Bold =>
            LogItemMessageEntityKind::Bold,
        MessageEntityKind::Italic =>
            LogItemMessageEntityKind::Italic,
        MessageEntityKind::Code =>
            LogItemMessageEntityKind::Code,
        MessageEntityKind::Pre =>
            LogItemMessageEntityKind::Pre,
        MessageEntityKind::TextLink(v) =>
            LogItemMessageEntityKind::TextLink(v.clone()),
        MessageEntityKind::TextMention(v) =>
            LogItemMessageEntityKind::TextMention(v.id.to_string().clone()),
        MessageEntityKind::Unknown(_) =>
            LogItemMessageEntityKind::Unknown,
    }
}

pub async fn build_log_item(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    api: &Api,
    message: &InterMessage,
    files: &Vec<String>,
) -> LogItem {
    let msg_from_id =
        message
            .from
            .as_ref()
            .map(|from|
                     from.id
                         .to_string()
                         .clone(),
            );

    match message.kind {
        MessageKind::Text {
            ref data,
            ref entities,
        } => {
            LogItem::Message {
                user_id: msg_from_id,
                time: message.date,
                text: data.clone(),
                entities:
                entities
                    .clone()
                    .iter()
                    .map(|entity|
                        LogItemMessageEntity {
                            offset: entity.offset,
                            length: entity.length,
                            kind: map_entity_kind(&entity.kind),
                        }
                    )
                    .collect(),
                source: Some(message.clone()),
            }
        }

        MessageKind::Audio { ref data } => {
            LogItem::Media {
                user_id: msg_from_id,
                time: message.date,
                caption: None,
                media_type:
                LogItemMediaType::Audio {
                    duration: data.duration,
                    performer: data.performer.clone(),
                    title: data.title.clone(),
                    mime_type: data.mime_type.clone(),
                },
                files: files.clone(),
                source: Some(message.clone()),
            }
        }

        MessageKind::Document {
            ref data,
            ref caption,
        } => {
            LogItem::Media {
                user_id: msg_from_id,
                time: message.date,
                caption: (*caption).clone(),
                media_type:
                LogItemMediaType::Document {
                    file_name: data.file_name.clone(),
                    mime_type: data.mime_type.clone(),
                },
                files: files.clone(),
                source: Some(message.clone()),
            }
        }

        MessageKind::Photo {
            ref data,
            ref caption,
            ..
        } => {
            let photo =
                find_biggest_photo(
                    &data.clone(),
                );

            LogItem::Media {
                user_id: msg_from_id,
                time: message.date,
                caption: (*caption).clone(),
                media_type:
                LogItemMediaType::Image {
                    width: photo.width,
                    height: photo.height,
                },
                files: files.clone(),
                source: Some(message.clone()),
            }
        }

        MessageKind::Sticker {
            ref data,
        } => {
            LogItem::Media {
                user_id: msg_from_id,
                time: message.date,
                caption: None,
                media_type:
                LogItemMediaType::Sticker {
                    emoji: data.emoji.clone(),
                    set_name: data.set_name.clone(),
                },
                files: files.clone(),
                source: Some(message.clone()),
            }
        }

        MessageKind::Video {
            ref data,
            ref caption,
            ..
        } => {
            let thumb_file_id =
                match data.thumb {
                    Some(ref thumb) => {
                        process_photosize(
                            db.clone(),
                            &api,
                            thumb,
                            None,
                        ).await
                    }
                    _ => None,
                };

            LogItem::Media {
                user_id: msg_from_id,
                time: message.date,
                caption: (*caption).clone(),
                media_type:
                LogItemMediaType::Video {
                    duration: data.duration,
                    width: data.width,
                    height: data.height,
                    thumb_file_id,
                    mime_type: data.mime_type.clone(),
                },
                files: files.clone(),
                source: Some(message.clone()),
            }
        }

        MessageKind::Voice {
            ref data,
        } => {
            LogItem::Media {
                user_id: msg_from_id,
                time: message.date,
                caption: None,
                media_type:
                LogItemMediaType::Voice {
                    duration: data.duration,
                    mime_type: data.mime_type.clone(),
                },
                files: files.clone(),
                source: Some(message.clone()),
            }
        }

        MessageKind::VideoNote {
            ref data,
        } => {
            let thumb_file_id =
                match data.thumb {
                    Some(ref thumb) => {
                        process_photosize(
                            db.clone(),
                            &api,
                            thumb,
                            None,
                        ).await
                    }
                    _ => None,
                };

            LogItem::Media {
                user_id: msg_from_id,
                time: message.date,
                caption: None,
                media_type:
                LogItemMediaType::VideoNote {
                    duration: data.duration,
                    thumb_file_id,
                },
                files: files.clone(),
                source: Some(message.clone()),
            }
        }

        MessageKind::Contact {
            ref data,
        } => {
            LogItem::Special {
                user_id: msg_from_id,
                time: message.date,
                special_type:
                LogItemSpecialType::Contact {
                    user_id: data.user_id.clone(),
                    phone_number: data.phone_number.clone(),
                    first_name: data.first_name.clone(),
                    last_name: data.last_name.clone(),
                },
                source: Some(message.clone()),
            }
        }

        MessageKind::Location {
            ref data,
        } => {
            LogItem::Special {
                user_id: msg_from_id,
                time: message.date,
                special_type:
                LogItemSpecialType::Location {
                    latitude: data.latitude,
                    longitude: data.longitude,
                },
                source: Some(message.clone()),
            }
        }

        MessageKind::Poll {
            ref data,
        } => {
            LogItem::Special {
                user_id: msg_from_id,
                time: message.date,
                special_type:
                LogItemSpecialType::Poll {
                    id: data.id.clone(),
                    question: data.question.clone(),
                    options:
                    data.options
                        .iter()
                        .map(|option|
                            LogItemSpecialTypePollOption {
                                text: option.text.clone(),
                                voter_count: option.voter_count,
                            }
                        )
                        .collect(),
                    total_voter_count: data.total_voter_count,
                    is_closed: data.is_closed,
                    is_anonymous: data.is_anonymous,
                    poll_type: data.type_.clone(),
                    allows_multiple_answers: data.allows_multiple_answers,
                    correct_option_id: data.correct_option_id.clone(),
                    explanation: data.explanation.clone(),
                    explanation_entities:
                    data.explanation_entities
                        .clone()
                        .map(|entities|
                            entities
                                .iter()
                                .map(|entity|
                                    LogItemSpecialTypePollMessageEntity {
                                        offset: entity.offset,
                                        length: entity.length,
                                        kind: map_entity_kind(&entity.kind),
                                    }
                                )
                                .collect::<Vec<LogItemSpecialTypePollMessageEntity>>()
                        ),
                    open_period: data.open_period.clone(),
                    close_date: data.close_date.clone(),
                },
                source: Some(message.clone()),
            }
        }

        MessageKind::Venue {
            ref data,
        } => {
            LogItem::Special {
                user_id: msg_from_id,
                time: message.date,
                special_type:
                LogItemSpecialType::Venue {
                    location:
                    LogItemSpecialTypeLocation {
                        longitude: data.location.longitude,
                        latitude: data.location.latitude,
                    },
                    title: data.title.clone(),
                    address: data.address.clone(),
                    foursquare_id: data.foursquare_id.clone(),
                },
                source: Some(message.clone()),
            }
        }

        MessageKind::NewChatMembers { .. } => {
            LogItem::Membership {
                user_id: msg_from_id,
                time: message.date,
                membership_type: LogItemMembershipType::Joined,
                source: Some(message.clone()),
            }
        }

        MessageKind::LeftChatMember { .. } => {
            LogItem::Membership {
                user_id: msg_from_id,
                time: message.date,
                membership_type: LogItemMembershipType::Left,
                source: Some(message.clone()),
            }
        }

        MessageKind::NewChatTitle {
            ref data,
        } => {
            LogItem::Chat {
                user_id: msg_from_id,
                time: message.date,
                chat_type:
                LogItemChatType::NewTitle {
                    title: data.clone(),
                },
                source: Some(message.clone()),
            }
        }

        MessageKind::NewChatPhoto {
            ref data,
        } => {
            let photo_size =
                find_biggest_photo(
                    data,
                );

            let photo =
                process_photosize(
                    db.clone(),
                    api,
                    &photo_size,
                    None,
                ).await;

            LogItem::Chat {
                user_id: msg_from_id,
                time: message.date,
                chat_type:
                LogItemChatType::NewPhoto {
                    file_id: photo.as_ref().map(|p| p.clone()),
                },
                source: Some(message.clone()),
            }
        }

        MessageKind::DeleteChatPhoto => {
            LogItem::Chat {
                user_id: msg_from_id,
                time: message.date,
                chat_type: LogItemChatType::DeletePhoto,
                source: Some(message.clone()),
            }
        }

        MessageKind::PinnedMessage {
            ref data,
        } => {
            LogItem::Pin {
                user_id: msg_from_id,
                time: message.date,
                message: data.text(),
                message_id: data.to_message_id().to_string(),
                source: Some(message.clone()),
            }
        }
        MessageKind::GroupChatCreated => {
            LogItem::Unimplemented(
                "GroupChatCreated".to_string(),
                msg_from_id,
                message.date,
                Some(message.clone()),
            )
        }

        MessageKind::SupergroupChatCreated => {
            LogItem::Unimplemented(
                "SupergroupChatCreated".to_string(),
                msg_from_id,
                message.date,
                Some(message.clone()),
            )
        }

        MessageKind::ChannelChatCreated => {
            LogItem::Unimplemented(
                "ChannelChatCreated".to_string(),
                msg_from_id,
                message.date,
                Some(message.clone()),
            )
        }

        MessageKind::MigrateToChatId { .. } => {
            LogItem::Unimplemented(
                "MigrateToChatId".to_string(),
                msg_from_id,
                message.date,
                Some(message.clone()),
            )
        }

        MessageKind::MigrateFromChatId { .. } => {
            LogItem::Unimplemented(
                "MigrateFromChatId".to_string(),
                msg_from_id,
                message.date,
                Some(message.clone()),
            )
        }

        MessageKind::Unknown { .. } => {
            dbg!(&message);

            LogItem::Unimplemented(
                "Unknown".to_string(),
                msg_from_id,
                message.date,
                Some(message.clone()),
            )
        }
    }
}

pub async fn handle_message(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    api: &Api,
    message: &InterMessage,
    files: &Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let log_item =
        build_log_item(
            db.clone(),
            api,
            message,
            files,
        ).await;

    let db = db.lock().unwrap();

    let established_date =
        message
            .forward
            .as_ref()
            .map(|original_message|
                     original_message
                         .date,
            )
            .unwrap_or(
                message
                    .date,
            );

    let use_forwarded_chat =
        match &message.chat {
            ChatMeta::Group(_)
            | ChatMeta::SuperGroup(_)
            | ChatMeta::Channel(_) => false,
            ChatMeta::User(_)
            | ChatMeta::Unknown(_) => true,
        };

    let chat_id =
        message
            .forward
            .as_ref()
            .map(|original_message| {
                if !use_forwarded_chat {
                    return None;
                }

                match original_message.from {
                    ForwardFromMeta::User { ref user } => Some(user.id.clone()),
                    ForwardFromMeta::Channel { ref channel, .. } => Some(channel.id.clone()),
                    ForwardFromMeta::ChannelHiddenUser { .. } => None,
                    ForwardFromMeta::HiddenGroupAdmin { ref chat_id, .. } => Some(chat_id.clone()),
                }
            })
            .flatten()
            .unwrap_or(
                message
                    .chat
                    .id(),
            );

    // store actual message

    {
        let message_key =
            format!(
                "chat:{}:{}",
                &chat_id,
                established_date.to_string(),
            );

        let message_value = serde_json::to_string(&log_item)?;

        db.put(
            &message_key,
            &message_value,
        )?;
    }

    // store chat index (days since start of epoch)

    {
        let chat_index_key =
            format!(
                "chat_index:{}:{}",
                &chat_id,
                (established_date / 86400).to_string(),
            );

        db.put(
            &chat_index_key,
            &b"\0",
        )?;
    }

    // store chat so that it can be iterated upon

    {
        let chat_key =
            format!(
                "chat_rel:{}",
                &chat_id,
            );

        db.put(
            &chat_key,
            &b"\0",
        )?;
    }

    // store chat by message id so that it allows direct lookup

    {
        let message_ref_key =
            format!(
                "chat_ref:{}:{}",
                &chat_id,
                message.id.to_string(),
            );

        let message_ref_value = established_date.to_string();

        db.put(
            &message_ref_key,
            &message_ref_value,
        )?;
    }

    // store chat metadata

    {
        let chat_meta_key =
            format!(
                "chat:meta:{}",
                &chat_id,
            );

        let chat_meta_value =
            message
                .forward
                .as_ref()
                .map(|forward| {
                    if !use_forwarded_chat {
                        return None;
                    }

                    match forward.from {
                        ForwardFromMeta::User { ref user } =>
                            Some(serde_json::to_string(&user)),
                        ForwardFromMeta::Channel { ref channel, .. } =>
                            Some(serde_json::to_string(&channel)),
                        _ => None,
                    }
                })
                .flatten()
                .unwrap_or(
                    serde_json::to_string(&message.chat),
                )?;

        db.put(
            &chat_meta_key,
            &chat_meta_value,
        )?;
    }

    Ok(())
}

pub async fn handle_inter_message(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
    api: &Api,
    inter_msg: &InterMessage,
) -> Result<(), Box<dyn std::error::Error>> {
    let files =
        match inter_msg.kind {
            MessageKind::Audio { .. }
            | MessageKind::Voice { .. }
            | MessageKind::Photo { .. }
            | MessageKind::Document { .. }
            | MessageKind::Sticker { .. }
            | MessageKind::Video { .. }
            | MessageKind::VideoNote { .. } =>
                process_files(
                    db.clone(),
                    &api,
                    &inter_msg,
                ).await?,

            _ => Vec::new(),
        };

    handle_message(
        db.clone(),
        &api,
        dbg!(&inter_msg),
        &files,
    ).await?;

    if let Some(ref from) = inter_msg.from {
        process_user(
            db.clone(),
            &api,
            dbg!(from),
        ).await?;
    }

    Ok(())
}

async fn run(
    db: Arc<Mutex<DBWithThreadMode<MultiThreaded>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let api = Api::new(get_telegram_api_token());

    let mut stream = api.stream();

    while let Some(update) = stream.next().await {
        let db = db.clone();

        let update = update?;

        dbg!(&update);

        match update.kind {
            UpdateKind::Message(ref message)
            | UpdateKind::EditedMessage(ref message) => {
                let inter_msg = InterMessage::from(message);

                if let Some(reply_to_message) = inter_msg.reply_to_message.as_ref() {
                    handle_inter_message(
                        db.clone(),
                        &api,
                        &reply_to_message.as_ref(),
                    ).await?;
                }

                handle_inter_message(
                    db.clone(),
                    &api,
                    &inter_msg,
                ).await?;
            },
            UpdateKind::ChannelPost(ref post)
            | UpdateKind::EditedChannelPost(ref post) => {
                let inter_msg = InterMessage::from(post);

                if let Some(reply_to_message) = inter_msg.reply_to_message.as_ref() {
                    handle_inter_message(
                        db.clone(),
                        &api,
                        &reply_to_message.as_ref(),
                    ).await?;
                }

                handle_inter_message(
                    db.clone(),
                    &api,
                    &inter_msg,
                ).await?;
            }
            _ => {},
        }
    }

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
