mod http;
mod wecom;

use std::{pin::Pin, sync::Arc};

use anyhow::Result;
use futures_util::{Stream, StreamExt};
use log::warn;
use serde::Serialize;
use serde_json::Value;

pub use http::serve as serve_http;
pub use wecom::WecomBot;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    Wecom,
    Http,
}

#[derive(Debug, Clone)]
pub struct InboundMessage {
    pub channel: Channel,
    pub session_id: String,
    pub sender_id: String,
    pub message_id: String,
    pub text: String,
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
            channel: Channel::Wecom,
            session_id: "dm:user-1".into(),
            sender_id: "user-1".into(),
            message_id: "message-1".into(),
            text: "hello".into(),
        });
        let reply = stream.next().await.unwrap().unwrap();

        assert!(matches!(reply, AgentEvent::TextDelta(text) if text == "hello"));
    }
}
