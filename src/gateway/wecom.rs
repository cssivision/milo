use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use aws_lc_rs::{
    cipher::{AES_256, AES_CBC_IV_LEN, DecryptingKey, DecryptionContext, UnboundCipherKey},
    iv::FixedLength,
};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use serde::{Deserialize, Deserializer, Serialize, de, de::DeserializeOwned};
use tokio::time::{interval, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

use crate::{
    config::{AccessPolicy, WecomConfig},
    gateway::{
        AgentEvent, Channel, GatewayAttachment, GatewayEventType, GatewayInboundMessageType,
        GatewayMessageSource, GatewayOutboundMessage, GatewayOutboundMessageType, GatewayRuntime,
        InboundMessage, attachment::download_attachment,
    },
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
const MAX_TEXT_CHARS: usize = 4_000;

#[derive(Clone)]
pub struct WecomBot {
    config: WecomConfig,
    gateway: GatewayRuntime,
}

#[derive(Debug, Deserialize)]
struct IncomingFrame {
    #[serde(default)]
    cmd: String,
    #[serde(default)]
    headers: FrameHeaders,
    #[serde(default)]
    body: serde_json::Value,
    #[serde(default)]
    errcode: i64,
    #[serde(default)]
    errmsg: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct FrameHeaders {
    #[serde(default)]
    req_id: String,
}

#[derive(Debug)]
struct IncomingMessage {
    msgid: String,
    chatid: String,
    chattype: String,
    from: Sender,
    payload: IncomingPayload,
    quote: Option<Box<QuotedMessage>>,
}

#[derive(Debug)]
struct QuotedMessage {
    payload: IncomingPayload,
    quote: Option<Box<QuotedMessage>>,
}

#[derive(Debug)]
enum IncomingPayload {
    Text(TextContent),
    Voice(TextContent),
    Image(Box<MediaContent>),
    File(Box<MediaContent>),
    Video(Box<MediaContent>),
    Mixed(Box<MixedContent>),
    AppMessage(Box<AppMessageContent>),
}

#[derive(Debug, Deserialize)]
struct WireEnvelope {
    #[serde(default)]
    msgid: String,
    #[serde(default)]
    chatid: String,
    #[serde(default)]
    chattype: String,
    #[serde(default)]
    msgtype: String,
    #[serde(default)]
    from: Sender,
    #[serde(flatten)]
    fields: HashMap<String, serde_json::Value>,
}

impl<'de> Deserialize<'de> for IncomingMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let envelope = WireEnvelope::deserialize(deserializer)?;
        let quote = decode_quote(envelope.fields.get("quote")).map_err(de::Error::custom)?;
        let payload =
            decode_payload(&envelope.msgtype, &envelope.fields).map_err(de::Error::custom)?;
        Ok(Self {
            msgid: envelope.msgid,
            chatid: envelope.chatid,
            chattype: envelope.chattype,
            from: envelope.from,
            payload,
            quote,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
struct Sender {
    #[serde(default)]
    userid: String,
}

#[derive(Debug, Default, Deserialize)]
struct TextContent {
    #[serde(default)]
    content: String,
}

#[derive(Debug, Default, Deserialize)]
struct MediaContent {
    #[serde(default)]
    url: String,
    #[serde(default)]
    aeskey: String,
    #[serde(default, alias = "filename", alias = "file_name")]
    name: String,
    #[serde(default)]
    media_id: String,
    #[serde(default)]
    base64: String,
}

impl MediaContent {
    fn is_empty(&self) -> bool {
        self.url.is_empty() && self.base64.is_empty()
    }
}

#[derive(Debug, Default, Deserialize)]
struct MixedContent {
    #[serde(default, alias = "msg_item")]
    items: Vec<MixedItem>,
}

#[derive(Debug, Default, Deserialize)]
struct MixedItem {
    #[serde(default)]
    msgtype: String,
    #[serde(default)]
    text: TextContent,
    #[serde(default)]
    image: MediaContent,
    #[serde(default)]
    file: MediaContent,
    #[serde(default)]
    video: MediaContent,
}

#[derive(Debug, Default, Deserialize)]
struct AppMessageContent {
    #[serde(default)]
    title: String,
    #[serde(default)]
    file: MediaContent,
    #[serde(default)]
    image: MediaContent,
    #[serde(default)]
    video: MediaContent,
}

#[derive(Debug)]
struct ParsedContent {
    text: String,
    media: Vec<(&'static str, MediaContent)>,
}

impl ParsedContent {
    fn empty() -> Self {
        Self {
            text: String::new(),
            media: Vec::new(),
        }
    }
}

fn decode_payload(
    msgtype: &str,
    fields: &HashMap<String, serde_json::Value>,
) -> std::result::Result<IncomingPayload, String> {
    fn field<T: DeserializeOwned>(
        fields: &HashMap<String, serde_json::Value>,
        name: &str,
    ) -> std::result::Result<T, String> {
        let value = fields
            .get(name)
            .cloned()
            .ok_or_else(|| format!("WeCom {name} message has no {name} payload"))?;
        serde_json::from_value(value)
            .map_err(|error| format!("parse WeCom {name} payload: {error}"))
    }

    match msgtype {
        "text" => field(fields, "text").map(IncomingPayload::Text),
        "voice" => field(fields, "voice").map(IncomingPayload::Voice),
        "image" => field(fields, "image")
            .map(Box::new)
            .map(IncomingPayload::Image),
        "file" => field(fields, "file")
            .map(Box::new)
            .map(IncomingPayload::File),
        "video" => field(fields, "video")
            .map(Box::new)
            .map(IncomingPayload::Video),
        "mixed" => field(fields, "mixed")
            .map(Box::new)
            .map(IncomingPayload::Mixed),
        "appmsg" => field(fields, "appmsg")
            .map(Box::new)
            .map(IncomingPayload::AppMessage),
        other => Err(format!("unsupported WeCom message type {other:?}")),
    }
}

fn decode_quote(
    value: Option<&serde_json::Value>,
) -> std::result::Result<Option<Box<QuotedMessage>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let mut fields = value
        .as_object()
        .cloned()
        .ok_or_else(|| "WeCom quote must be an object".to_owned())?;
    let msgtype = fields
        .remove("msgtype")
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| "WeCom quote has no msgtype".to_owned())?;
    let fields = fields.into_iter().collect::<HashMap<_, _>>();
    Ok(Some(Box::new(QuotedMessage {
        payload: decode_payload(&msgtype, &fields)?,
        quote: decode_quote(fields.get("quote"))?,
    })))
}

fn parse_payload(payload: IncomingPayload) -> Result<ParsedContent> {
    let content = match payload {
        IncomingPayload::Text(text) | IncomingPayload::Voice(text) => ParsedContent {
            text: text.content,
            media: Vec::new(),
        },
        IncomingPayload::Image(media) => ParsedContent {
            text: String::new(),
            media: vec![("image", *media)],
        },
        IncomingPayload::File(media) => ParsedContent {
            text: String::new(),
            media: vec![("file", *media)],
        },
        IncomingPayload::Video(media) => ParsedContent {
            text: String::new(),
            media: vec![("video", *media)],
        },
        IncomingPayload::Mixed(mixed) => {
            let mixed = *mixed;
            let mut content = ParsedContent::empty();
            for item in mixed.items {
                match item.msgtype.as_str() {
                    "text" => content.text.push_str(&item.text.content),
                    "image" => content.media.push(("image", item.image)),
                    "file" => content.media.push(("file", item.file)),
                    "video" => content.media.push(("video", item.video)),
                    other => bail!("unsupported mixed message item {other:?}"),
                }
            }
            content
        }
        IncomingPayload::AppMessage(appmsg) => {
            let appmsg = *appmsg;
            let mut content = ParsedContent {
                text: appmsg.title,
                media: Vec::new(),
            };
            if !appmsg.file.is_empty() {
                content.media.push(("file", appmsg.file));
            }
            if !appmsg.image.is_empty() {
                content.media.push(("image", appmsg.image));
            }
            if !appmsg.video.is_empty() {
                content.media.push(("video", appmsg.video));
            }
            content
        }
    };
    Ok(content)
}

fn append_quote(
    mut content: ParsedContent,
    quote: Option<Box<QuotedMessage>>,
) -> Result<ParsedContent> {
    if let Some(quote) = quote {
        let quoted = append_quote(parse_payload(quote.payload)?, quote.quote)?;
        let marker = if quoted.text.trim().is_empty() {
            "media attachment"
        } else {
            quoted.text.trim()
        };
        content.text = format!("[Quote: {marker}]\n{}", content.text)
            .trim()
            .to_owned();
        content.media.extend(quoted.media);
    }
    Ok(content)
}

async fn normalize_message(message: IncomingMessage, chat_id: String) -> Result<InboundMessage> {
    let content = append_quote(parse_payload(message.payload)?, message.quote)?;
    let mut attachments = Vec::with_capacity(content.media.len());
    for (kind, media) in content.media {
        attachments.push(resolve_media(kind, media).await?);
    }
    if content.text.trim().is_empty() && attachments.is_empty() {
        bail!("WeCom message has no text or attachments");
    }

    Ok(InboundMessage {
        channel_type: Channel::Wecom,
        platform_message_id: message.msgid,
        msg_type: GatewayInboundMessageType::Text,
        text: content.text,
        attachments,
        source: GatewayMessageSource {
            chat_id,
            sender_id: Some(message.from.userid),
            metadata: HashMap::new(),
        },
        metadata: HashMap::new(),
    })
}

async fn resolve_media(kind: &str, media: MediaContent) -> Result<GatewayAttachment> {
    let (encrypted, content_type) = if media.base64.is_empty() {
        if media.url.is_empty() {
            bail!("WeCom media has no URL or inline data");
        }
        download_attachment(&media.url).await?
    } else {
        let payload = media
            .base64
            .split_once(',')
            .map_or(media.base64.as_str(), |(_, data)| data);
        let payload = format!("{payload}{}", "=".repeat((4 - payload.len() % 4) % 4));
        (
            base64::engine::general_purpose::STANDARD
                .decode(payload)
                .context("decode WeCom inline media")?,
            None,
        )
    };
    if media.aeskey.is_empty() {
        bail!("WeCom media has no aeskey");
    }
    let data = decrypt_media(&encrypted, &media.aeskey)?;
    let file_name = (!media.name.is_empty())
        .then_some(media.name)
        .or_else(|| file_name_from_url(&media.url));
    let content_type = content_type
        .or_else(|| {
            file_name
                .as_deref()
                .map(mime_guess::from_path)
                .and_then(|guess| guess.first_raw())
                .map(str::to_owned)
        })
        .or_else(|| sniff_media_type(&data))
        .or_else(|| {
            file_name
                .as_deref()
                .filter(|name| kind == "video" && name.ends_with(".mp4"))
                .map(|_| "video/mp4".to_owned())
        });
    let content_type = content_type.or_else(|| match kind {
        "image" => Some("image/jpeg".to_owned()),
        "video" => Some("video/mp4".to_owned()),
        _ => None,
    });
    let content_type = content_type.context("cannot determine WeCom attachment content type")?;
    Ok(GatewayAttachment {
        attachment_id: (!media.media_id.is_empty()).then_some(media.media_id),
        content_type: Some(content_type),
        file_name,
        data: Some(data),
        uri: None,
        metadata: HashMap::from([(
            "wecom_type".to_owned(),
            serde_json::Value::String(kind.to_owned()),
        )]),
    })
}

fn decrypt_media(encrypted: &[u8], aeskey: &str) -> Result<Vec<u8>> {
    let aeskey = format!("{aeskey}{}", "=".repeat((4 - aeskey.len() % 4) % 4));
    let key = base64::engine::general_purpose::STANDARD
        .decode(aeskey)
        .context("decode WeCom aeskey")?;
    if key.len() != 32 {
        bail!("WeCom aeskey must decode to 32 bytes");
    }
    let mut payload = encrypted.to_vec();
    let cipher_key =
        UnboundCipherKey::new(&AES_256, &key).map_err(|_| anyhow!("initialize WeCom AES key"))?;
    let decryptor =
        DecryptingKey::cbc(cipher_key).map_err(|_| anyhow!("initialize WeCom AES decryptor"))?;
    let iv: FixedLength<AES_CBC_IV_LEN> = key[..AES_CBC_IV_LEN]
        .try_into()
        .map_err(|_| anyhow!("initialize WeCom AES IV"))?;
    let decrypted = decryptor
        .decrypt(&mut payload, DecryptionContext::Iv128(iv))
        .map_err(|_| anyhow!("decrypt WeCom media"))?;
    let padding_len = usize::from(*decrypted.last().context("decrypted WeCom media is empty")?);
    if !(1..=32).contains(&padding_len) || padding_len > decrypted.len() {
        bail!("invalid WeCom PKCS#7 padding");
    }
    if !decrypted[decrypted.len() - padding_len..]
        .iter()
        .all(|byte| usize::from(*byte) == padding_len)
    {
        bail!("invalid WeCom PKCS#7 padding");
    }
    Ok(decrypted[..decrypted.len() - padding_len].to_vec())
}

fn file_name_from_url(url: &str) -> Option<String> {
    let path = url.split('?').next()?.trim_end_matches('/');
    path.rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
}

fn sniff_media_type(data: &[u8]) -> Option<String> {
    if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png".into())
    } else if data.starts_with(b"\xff\xd8\xff") {
        Some("image/jpeg".into())
    } else if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
        Some("image/gif".into())
    } else if data.get(..4) == Some(b"RIFF") && data.get(8..12) == Some(b"WEBP") {
        Some("image/webp".into())
    } else if data.starts_with(b"%PDF") {
        Some("application/pdf".into())
    } else if data.get(4..8) == Some(b"ftyp") {
        Some("video/mp4".into())
    } else {
        None
    }
}

impl WecomBot {
    pub fn new(config: WecomConfig, gateway: GatewayRuntime) -> Self {
        Self { config, gateway }
    }

    pub async fn run_forever(&self) {
        let mut retry_delay = Duration::from_secs(1);
        loop {
            match self.run_connection().await {
                Ok(()) => {
                    warn!("WeCom Gateway connection ended; reconnecting");
                    retry_delay = Duration::from_secs(1);
                }
                Err(error) => warn!("WeCom Gateway connection failed: {error}"),
            }
            sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(MAX_RECONNECT_DELAY);
        }
    }

    async fn run_connection(&self) -> Result<()> {
        info!("connecting WeCom Gateway to {}", self.config.websocket_url);
        let (socket, _) = timeout(CONNECT_TIMEOUT, connect_async(&self.config.websocket_url))
            .await
            .context("timed out connecting to WeCom Bot WebSocket")??;
        let (mut writer, mut reader) = socket.split();

        let subscribe_id = request_id("subscribe");
        send_json(
            &mut writer,
            &serde_json::json!({
                "cmd": "aibot_subscribe",
                "headers": { "req_id": subscribe_id },
                "body": {
                    "bot_id": self.config.bot_id,
                    "secret": self.config.secret,
                }
            }),
        )
        .await?;

        let handshake = timeout(CONNECT_TIMEOUT, reader.next())
            .await
            .context("timed out waiting for WeCom subscription response")?
            .ok_or_else(|| anyhow!("WeCom closed the WebSocket during subscription"))??;
        validate_subscription(handshake, &subscribe_id)?;
        info!("WeCom Gateway subscribed");

        let mut heartbeat = interval(HEARTBEAT_INTERVAL);
        heartbeat.tick().await;
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    send_json(
                        &mut writer,
                        &serde_json::json!({
                            "cmd": "ping",
                            "headers": { "req_id": request_id("ping") },
                            "body": {}
                        }),
                    ).await?;
                }
                next = reader.next() => {
                    let message = next.ok_or_else(|| anyhow!("WeCom closed the WebSocket"))??;
                    if let Some(frame) = parse_frame(message)? {
                        self.handle_frame(&mut writer, frame).await?;
                    }
                }
            }
        }
    }

    async fn handle_frame<S>(&self, writer: &mut S, frame: IncomingFrame) -> Result<()>
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        if frame.cmd != "aibot_msg_callback" {
            return Ok(());
        }
        if frame.headers.req_id.is_empty() {
            bail!("received WeCom message callback without headers.req_id");
        }

        let message: IncomingMessage =
            serde_json::from_value(frame.body).context("parse WeCom message callback")?;
        if !self.accepts(&message) {
            debug!(
                "WeCom message rejected by access policy: message_id={}, chat_type={}",
                message.msgid, message.chattype
            );
            return Ok(());
        }

        let chat_id = session_id(&message);
        let outbound_context = (chat_id.clone(), Some(message.msgid.clone()));
        let inbound = match normalize_message(message, chat_id).await {
            Ok(inbound) => inbound,
            Err(error) => {
                warn!("WeCom message rejected: {error:#}");
                self.respond_text(writer, &frame.headers.req_id, "暂不支持该消息类型或附件。")
                    .await?;
                return Ok(());
            }
        };
        let mut stream = self.gateway.dispatch_stream(inbound);
        let stream_id = request_id("stream");
        let mut content = String::new();
        let mut finished = false;

        while let Some(event) = stream.next().await {
            match event? {
                AgentEvent::TextDelta(delta) => {
                    let outbound = GatewayOutboundMessage::event(
                        outbound_context.0.clone(),
                        outbound_context.1.clone(),
                        GatewayEventType::TextDelta,
                        GatewayOutboundMessageType::Text,
                        delta,
                        HashMap::new(),
                    );
                    content.push_str(&outbound.text);
                    self.respond_stream(writer, &frame.headers.req_id, &stream_id, &content, false)
                        .await?;
                }
                AgentEvent::ReasoningDelta(_)
                | AgentEvent::ToolCall(_)
                | AgentEvent::ToolResult(_) => {}
                AgentEvent::CompletionCall(_) => {}
                AgentEvent::Retried { .. } => content.clear(),
                AgentEvent::Completed(final_content) => {
                    let outbound = GatewayOutboundMessage::event(
                        outbound_context.0.clone(),
                        outbound_context.1.clone(),
                        GatewayEventType::Completed,
                        GatewayOutboundMessageType::Text,
                        final_content,
                        HashMap::new(),
                    );
                    self.respond_stream(
                        writer,
                        &frame.headers.req_id,
                        &stream_id,
                        &outbound.text,
                        true,
                    )
                    .await?;
                    finished = true;
                    break;
                }
                AgentEvent::Error(message) => {
                    let outbound = GatewayOutboundMessage::event(
                        outbound_context.0.clone(),
                        outbound_context.1.clone(),
                        GatewayEventType::Error,
                        GatewayOutboundMessageType::Text,
                        message,
                        HashMap::new(),
                    );
                    self.respond_stream(
                        writer,
                        &frame.headers.req_id,
                        &stream_id,
                        &outbound.text,
                        true,
                    )
                    .await?;
                    finished = true;
                    break;
                }
            }
        }

        if !finished {
            self.respond_stream(writer, &frame.headers.req_id, &stream_id, &content, true)
                .await?;
        }
        Ok(())
    }

    async fn respond_text<S>(&self, writer: &mut S, req_id: &str, content: &str) -> Result<()>
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        self.respond_outbound(
            writer,
            req_id,
            &GatewayOutboundMessage {
                chat_id: String::new(),
                text: content.to_owned(),
                reply_to_message_id: None,
                msg_type: GatewayOutboundMessageType::Text,
                media_id: None,
                description: None,
                metadata: HashMap::new(),
            },
        )
        .await
    }

    async fn respond_outbound<S>(
        &self,
        writer: &mut S,
        req_id: &str,
        message: &GatewayOutboundMessage,
    ) -> Result<()>
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        let body = match message.msg_type {
            GatewayOutboundMessageType::Text => serde_json::json!({
                "msgtype": "text",
                "text": { "content": message.text }
            }),
            GatewayOutboundMessageType::Image => media_body("image", "image", message)?,
            GatewayOutboundMessageType::File => media_body("file", "file", message)?,
            GatewayOutboundMessageType::Voice => media_body("voice", "voice", message)?,
            GatewayOutboundMessageType::Video => media_body("video", "video", message)?,
            _ => bail!("WeCom does not support outbound {:?}", message.msg_type),
        };
        send_json(
            writer,
            &serde_json::json!({
                "cmd": "aibot_respond_msg",
                "headers": { "req_id": req_id },
                "body": body
            }),
        )
        .await
    }

    async fn respond_stream<S>(
        &self,
        writer: &mut S,
        req_id: &str,
        stream_id: &str,
        content: &str,
        finish: bool,
    ) -> Result<()>
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        let content: String = content.chars().take(MAX_TEXT_CHARS).collect();
        send_json(
            writer,
            &stream_reply_payload(req_id, stream_id, &content, finish),
        )
        .await
    }

    fn accepts(&self, message: &IncomingMessage) -> bool {
        let (policy, allow_from) = if message.chattype == "group" {
            (self.config.group_policy, &self.config.group_allow_from)
        } else {
            (self.config.dm_policy, &self.config.dm_allow_from)
        };
        match policy {
            AccessPolicy::Open => true,
            AccessPolicy::Disabled => false,
            AccessPolicy::Allowlist => allow_from.iter().any(|user| user == &message.from.userid),
        }
    }
}

async fn send_json<S>(writer: &mut S, payload: &serde_json::Value) -> Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    writer
        .send(Message::Text(payload.to_string().into()))
        .await
        .context("send WeCom WebSocket frame")
}

fn validate_subscription(message: Message, expected_request_id: &str) -> Result<()> {
    let Some(frame) = parse_frame(message)? else {
        bail!("WeCom returned an empty subscription response");
    };
    if frame.headers.req_id != expected_request_id {
        bail!("unexpected WeCom subscription response request id");
    }
    if frame.errcode != 0 {
        bail!(
            "WeCom subscription failed ({}): {}",
            frame.errcode,
            frame.errmsg
        );
    }
    Ok(())
}

fn parse_frame(message: Message) -> Result<Option<IncomingFrame>> {
    match message {
        Message::Text(text) => Ok(Some(
            serde_json::from_str(&text).context("parse WeCom JSON frame")?,
        )),
        Message::Binary(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).context("parse WeCom binary JSON frame")?,
        )),
        Message::Ping(_) | Message::Pong(_) => Ok(None),
        Message::Close(_) => bail!("WeCom closed the WebSocket"),
        Message::Frame(_) => Ok(None),
    }
}

fn request_id(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4())
}

fn stream_reply_payload(
    req_id: &str,
    stream_id: &str,
    content: &str,
    finish: bool,
) -> serde_json::Value {
    serde_json::json!({
        "cmd": "aibot_respond_msg",
        "headers": { "req_id": req_id },
        "body": {
            "msgtype": "stream",
            "stream": {
                "id": stream_id,
                "finish": finish,
                "content": content
            }
        }
    })
}

fn session_id(message: &IncomingMessage) -> String {
    if message.chattype == "group" {
        format!("group:{}", message.chatid)
    } else {
        format!("dm:{}", message.from.userid)
    }
}

fn media_body(
    msg_type: &str,
    field: &str,
    message: &GatewayOutboundMessage,
) -> Result<serde_json::Value> {
    let media_id = message
        .media_id
        .as_deref()
        .context("WeCom media reply requires media_id")?;
    let mut media = serde_json::Map::from_iter([(
        "media_id".to_owned(),
        serde_json::Value::String(media_id.to_owned()),
    )]);
    if let Some(description) = &message.description {
        media.insert(
            "description".to_owned(),
            serde_json::Value::String(description.clone()),
        );
    }
    Ok(serde_json::json!({
        "msgtype": msg_type,
        field: media,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lc_rs::cipher::{EncryptingKey, EncryptionContext};

    #[test]
    fn parses_text_message_callback() {
        let frame = parse_frame(Message::Text(
            r#"{"cmd":"aibot_msg_callback","headers":{"req_id":"req-1"},"body":{"msgid":"m1","chatid":"c1","chattype":"single","msgtype":"text","from":{"userid":"u1"},"text":{"content":"hello"}}}"#
                .into(),
        ))
        .unwrap()
        .unwrap();
        let message: IncomingMessage = serde_json::from_value(frame.body).unwrap();

        assert!(matches!(
            message.payload,
            IncomingPayload::Text(TextContent { ref content }) if content == "hello"
        ));
        assert_eq!(session_id(&message), "dm:u1");
    }

    #[test]
    fn subscription_rejects_an_error_response() {
        let message = Message::Text(
            r#"{"headers":{"req_id":"subscribe-1"},"errcode":40013,"errmsg":"invalid secret"}"#
                .into(),
        );

        assert!(validate_subscription(message, "subscribe-1").is_err());
    }

    #[test]
    fn stream_reply_uses_the_callback_request_and_stream_ids() {
        let payload = stream_reply_payload("callback-1", "stream-1", "partial", false);

        assert_eq!(payload["headers"]["req_id"], "callback-1");
        assert_eq!(payload["body"]["msgtype"], "stream");
        assert_eq!(payload["body"]["stream"]["id"], "stream-1");
        assert_eq!(payload["body"]["stream"]["content"], "partial");
        assert_eq!(payload["body"]["stream"]["finish"], false);
    }

    #[test]
    fn decrypts_unpadded_aeskey() {
        let key = [7_u8; 32];
        let aeskey = base64::engine::general_purpose::STANDARD.encode(key);
        let aeskey = aeskey.trim_end_matches('=');
        let plaintext = b"example media";
        let padding_len = 32 - plaintext.len() % 32;
        let mut encrypted = plaintext.to_vec();
        encrypted.extend(std::iter::repeat_n(padding_len as u8, padding_len));
        let cipher_key = UnboundCipherKey::new(&AES_256, &key).unwrap();
        let encryptor = EncryptingKey::cbc(cipher_key).unwrap();
        let iv: FixedLength<AES_CBC_IV_LEN> = key[..AES_CBC_IV_LEN].try_into().unwrap();
        encryptor
            .less_safe_encrypt(&mut encrypted, EncryptionContext::Iv128(iv))
            .unwrap();

        assert_eq!(decrypt_media(&encrypted, aeskey).unwrap(), plaintext);
    }

    #[test]
    fn identifies_common_media_and_url_file_names() {
        assert_eq!(
            sniff_media_type(b"\x89PNG\r\n\x1a\npayload").as_deref(),
            Some("image/png")
        );
        assert_eq!(
            file_name_from_url("https://cdn.example/object.mp4?token=secret").as_deref(),
            Some("object.mp4")
        );
    }
}
