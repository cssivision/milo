mod attachment;
mod http;
mod wecom;

use std::{collections::HashMap, pin::Pin, sync::Arc};

use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{Stream, StreamExt};
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use attachment::resolve_attachments;
pub use http::serve as serve_http;
pub use wecom::WecomBot;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Wecom,
    Http,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GatewayAttachment {
    #[serde(default)]
    pub attachment_id: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(
        default,
        serialize_with = "serialize_base64",
        deserialize_with = "deserialize_base64"
    )]
    pub data: Option<Vec<u8>>,
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GatewayMessageSource {
    pub chat_id: String,
    #[serde(default)]
    pub sender_id: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayInboundMessageType {
    #[default]
    Text,
    Raw,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GatewayInboundMessage {
    pub channel_type: Channel,
    pub platform_message_id: String,
    #[serde(default)]
    pub msg_type: GatewayInboundMessageType,
    pub text: String,
    #[serde(default)]
    pub attachments: Vec<GatewayAttachment>,
    pub source: GatewayMessageSource,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayOutboundMessageType {
    Text,
    Markdown,
    Card,
    Image,
    File,
    Voice,
    Video,
    Raw,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GatewayOutboundMessage {
    pub chat_id: String,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to_message_id: Option<String>,
    pub msg_type: GatewayOutboundMessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,
}

impl GatewayOutboundMessage {
    pub fn event(
        chat_id: impl Into<String>,
        reply_to_message_id: Option<String>,
        event_type: GatewayEventType,
        msg_type: GatewayOutboundMessageType,
        text: impl Into<String>,
        mut metadata: HashMap<String, Value>,
    ) -> Self {
        metadata.insert(
            "event".to_owned(),
            Value::String(event_type.as_str().to_owned()),
        );
        Self {
            chat_id: chat_id.into(),
            text: text.into(),
            reply_to_message_id,
            msg_type,
            media_id: None,
            description: None,
            metadata,
        }
    }
}

pub type InboundMessage = GatewayInboundMessage;

fn serialize_base64<S>(
    data: &Option<Vec<u8>>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    data.as_ref()
        .map(|data| STANDARD.encode(data))
        .serialize(serializer)
}

fn deserialize_base64<'de, D>(deserializer: D) -> std::result::Result<Option<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let data = Option::<String>::deserialize(deserializer)?;
    data.map(|data| STANDARD.decode(data).map_err(serde::de::Error::custom))
        .transpose()
}

#[derive(Debug, Clone)]
pub enum AgentEvent {
    TextDelta(String),
    ReasoningDelta(String),
    ToolCall(ToolCallEvent),
    ToolResult(ToolResultEvent),
    CompletionCall(CompletionCallEvent),
    Retried { turn: usize },
    Completed(String),
    Error(String),
}

#[derive(Debug, Clone, Copy)]
pub enum GatewayEventType {
    TextDelta,
    ReasoningDelta,
    ToolCall,
    ToolResult,
    CompletionCall,
    Retried,
    Completed,
    Error,
}

impl GatewayEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TextDelta => "text_delta",
            Self::ReasoningDelta => "reasoning_delta",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::CompletionCall => "completion_call",
            Self::Retried => "retried",
            Self::Completed => "completed",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolCallEvent {
    pub part: Option<usize>,
    pub id: Option<String>,
    pub name: Option<String>,
    pub arguments: Value,
    pub completed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolResultEvent {
    pub call_id: String,
    pub name: String,
    pub content: Vec<Value>,
    pub is_error: bool,
    pub index: Option<usize>,
    pub count: Option<usize>,
    pub completed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompletionCallEvent {
    pub call_index: usize,
    pub usage: Value,
    pub finish_reason: Option<Value>,
}

pub type AgentStream = Pin<Box<dyn Stream<Item = Result<AgentEvent>> + Send>>;

pub trait Agent: Send + Sync {
    fn respond_stream(&self, message: InboundMessage) -> AgentStream;
}

#[derive(Clone)]
pub struct GatewayRuntime {
    agent: Arc<dyn Agent>,
}

impl GatewayRuntime {
    pub fn new(agent: impl Agent + 'static) -> Self {
        Self {
            agent: Arc::new(agent),
        }
    }

    pub fn dispatch_stream(&self, message: InboundMessage) -> AgentStream {
        let mut stream = self.agent.respond_stream(message);
        Box::pin(async_stream::stream! {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => yield Ok(event),
                    Err(error) => {
                        warn!("agent stream failed: {error}");
                        yield Ok(AgentEvent::Error(
                            "抱歉，暂时无法生成回复，请稍后重试。".to_owned(),
                        ));
                        return;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoAgent;

    impl Agent for EchoAgent {
        fn respond_stream(&self, message: InboundMessage) -> AgentStream {
            Box::pin(async_stream::stream! {
                yield Ok(AgentEvent::TextDelta(message.text.clone()));
                yield Ok(AgentEvent::Completed(message.text));
            })
        }
    }

    #[tokio::test]
    async fn dispatches_a_normalized_message_to_the_agent() {
        let gateway = GatewayRuntime::new(EchoAgent);
        let mut stream = gateway.dispatch_stream(InboundMessage {
            channel_type: Channel::Wecom,
            platform_message_id: "message-1".into(),
            msg_type: GatewayInboundMessageType::Text,
            text: "hello".into(),
            attachments: Vec::new(),
            source: GatewayMessageSource {
                chat_id: "dm:user-1".into(),
                sender_id: Some("user-1".into()),
                metadata: HashMap::new(),
            },
            metadata: HashMap::new(),
        });
        let reply = stream.next().await.unwrap().unwrap();

        assert!(matches!(reply, AgentEvent::TextDelta(text) if text == "hello"));
    }

    #[test]
    fn attachment_data_is_base64_encoded_in_json() {
        let attachment: GatewayAttachment =
            serde_json::from_str(r#"{"content_type":"image/png","data":"AQID"}"#).unwrap();
        assert_eq!(attachment.data, Some(vec![1, 2, 3]));
        assert_eq!(
            serde_json::to_value(attachment).unwrap()["data"],
            serde_json::Value::String("AQID".to_owned())
        );
    }

    #[test]
    fn outbound_events_include_their_event_type() {
        let message = GatewayOutboundMessage::event(
            "chat",
            Some("message".into()),
            GatewayEventType::TextDelta,
            GatewayOutboundMessageType::Text,
            "hello",
            HashMap::new(),
        );

        assert_eq!(message.metadata["event"], "text_delta");
        assert_eq!(message.text, "hello");
    }
}
