use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::time::{interval, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

use crate::{
    config::{AccessPolicy, WecomConfig},
    gateway::{AgentEvent, Channel, GatewayRuntime, InboundMessage},
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

#[derive(Debug, Deserialize)]
struct IncomingMessage {
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
    #[serde(default)]
    text: TextContent,
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
        if message.msgtype != "text" || message.text.content.trim().is_empty() {
            self.respond_text(writer, &frame.headers.req_id, "目前仅支持文本消息。")
                .await?;
            return Ok(());
        }
        if !self.accepts(&message) {
            debug!(
                "WeCom message rejected by access policy: message_id={}, chat_type={}",
                message.msgid, message.chattype
            );
            return Ok(());
        }

        let mut stream = self.gateway.dispatch_stream(InboundMessage {
            channel: Channel::Wecom,
            session_id: session_id(&message),
            sender_id: message.from.userid,
            message_id: message.msgid,
            text: message.text.content.trim().to_owned(),
        });
        let stream_id = request_id("stream");
        let mut content = String::new();
        let mut finished = false;

        while let Some(event) = stream.next().await {
            match event? {
                AgentEvent::TextDelta(delta) => {
                    content.push_str(&delta);
                    self.respond_stream(writer, &frame.headers.req_id, &stream_id, &content, false)
                        .await?;
                }
                AgentEvent::ReasoningDelta(_)
                | AgentEvent::ToolCall(_)
                | AgentEvent::ToolResult(_) => {}
                AgentEvent::CompletionCall(_) => {}
                AgentEvent::Retried { .. } => content.clear(),
                AgentEvent::Completed(final_content) => {
                    self.respond_stream(
                        writer,
                        &frame.headers.req_id,
                        &stream_id,
                        &final_content,
                        true,
                    )
                    .await?;
                    finished = true;
                    break;
                }
                AgentEvent::Error(message) => {
                    self.respond_stream(writer, &frame.headers.req_id, &stream_id, &message, true)
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
        send_json(
            writer,
            &serde_json::json!({
                "cmd": "aibot_respond_msg",
                "headers": { "req_id": req_id },
                "body": {
                    "msgtype": "text",
                    "text": { "content": content }
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_text_message_callback() {
        let frame = parse_frame(Message::Text(
            r#"{"cmd":"aibot_msg_callback","headers":{"req_id":"req-1"},"body":{"msgid":"m1","chatid":"c1","chattype":"single","msgtype":"text","from":{"userid":"u1"},"text":{"content":"hello"}}}"#
                .into(),
        ))
        .unwrap()
        .unwrap();
        let message: IncomingMessage = serde_json::from_value(frame.body).unwrap();

        assert_eq!(message.text.content, "hello");
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
}
