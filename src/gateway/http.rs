use std::{collections::HashMap, convert::Infallible, net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{Json, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
};
use futures_util::StreamExt;
use log::{info, warn};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    config::HttpConfig,
    gateway::{
        AgentEvent, Channel, GatewayAttachment, GatewayEventType, GatewayInboundMessageType,
        GatewayMessageSource, GatewayOutboundMessage, GatewayOutboundMessageType, GatewayRuntime,
        InboundMessage, resolve_attachments,
    },
};

#[derive(Clone)]

struct HttpState {
    gateway: GatewayRuntime,
    bearer_token: Arc<str>,
}

#[derive(Debug, Deserialize)]
struct ChatRequest {
    message: String,
    #[serde(default)]
    session_id: Option<String>,
    profile_id: String,
    #[serde(default)]
    attachments: Vec<GatewayAttachment>,
    #[serde(default)]
    metadata: HashMap<String, serde_json::Value>,
}

pub async fn serve(config: HttpConfig, gateway: GatewayRuntime) -> Result<()> {
    let listen_addr: SocketAddr = config
        .listen_addr
        .parse()
        .context("parse HTTP Gateway listen_addr")?;
    let state = HttpState {
        gateway,
        bearer_token: Arc::from(config.bearer_token),
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/chat/stream", post(chat_stream))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("bind HTTP Gateway to {listen_addr}"))?;
    info!("HTTP Gateway listening on http://{listen_addr}");

    axum::serve(listener, app)
        .await
        .context("serve HTTP Gateway")
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn chat_stream(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<ChatRequest>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    if !is_authorized(&headers, &state.bearer_token) {
        warn!("HTTP Gateway rejected an unauthorized request");
        return Err(StatusCode::UNAUTHORIZED);
    }
    let message = request.message.trim();
    if message.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let session_id = request
        .session_id
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| format!("http:{}", Uuid::new_v4()));
    let profile_id = request.profile_id.trim();
    if profile_id.is_empty() {
        log::error!("profile_id is required");
        return Err(StatusCode::BAD_REQUEST);
    }
    let attachments = resolve_attachments(request.attachments)
        .await
        .map_err(|error| {
            warn!("HTTP Gateway rejected attachments: {error}");
            StatusCode::BAD_REQUEST
        })?;
    info!("HTTP Gateway started a stream: session_id={session_id}");
    let inbound = InboundMessage {
        channel_type: Channel::Http,
        platform_message_id: Uuid::new_v4().to_string(),
        msg_type: GatewayInboundMessageType::Text,
        text: message.to_owned(),
        attachments,
        source: GatewayMessageSource {
            chat_id: session_id,
            sender_id: Some(profile_id.to_owned()),
            metadata: HashMap::new(),
        },
        metadata: request.metadata,
    };
    let outbound_context = (
        inbound.source.chat_id.clone(),
        Some(inbound.platform_message_id.clone()),
    );
    let stream = state.gateway.dispatch_stream(inbound).map(move |event| {
        let (event_type, msg_type, text, metadata) = match event {
            Ok(AgentEvent::TextDelta(text)) => (
                GatewayEventType::TextDelta,
                GatewayOutboundMessageType::Text,
                text,
                HashMap::new(),
            ),
            Ok(AgentEvent::ReasoningDelta(text)) => (
                GatewayEventType::ReasoningDelta,
                GatewayOutboundMessageType::Raw,
                text,
                HashMap::new(),
            ),
            Ok(AgentEvent::ToolCall(tool_call)) => raw_event(
                GatewayEventType::ToolCall,
                "tool_call",
                serde_json::json!(tool_call),
            ),
            Ok(AgentEvent::ToolResult(tool_result)) => raw_event(
                GatewayEventType::ToolResult,
                "tool_result",
                serde_json::json!(tool_result),
            ),
            Ok(AgentEvent::CompletionCall(call)) => raw_event(
                GatewayEventType::CompletionCall,
                "completion_call",
                serde_json::json!(call),
            ),
            Ok(AgentEvent::Retried { turn }) => {
                raw_event(GatewayEventType::Retried, "turn", serde_json::json!(turn))
            }
            Ok(AgentEvent::Completed(text)) => (
                GatewayEventType::Completed,
                GatewayOutboundMessageType::Text,
                text,
                HashMap::new(),
            ),
            Ok(AgentEvent::Error(message)) => (
                GatewayEventType::Error,
                GatewayOutboundMessageType::Text,
                message,
                HashMap::new(),
            ),
            Err(_) => (
                GatewayEventType::Error,
                GatewayOutboundMessageType::Text,
                "抱歉，暂时无法生成回复，请稍后重试。".to_owned(),
                HashMap::new(),
            ),
        };
        let outbound = GatewayOutboundMessage::event(
            outbound_context.0.clone(),
            outbound_context.1.clone(),
            event_type,
            msg_type,
            text,
            metadata,
        );
        Ok::<Event, Infallible>(sse_event(event_type, serde_json::json!(outbound)))
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn is_authorized(headers: &HeaderMap, token: &str) -> bool {
    let expected = format!("Bearer {token}");
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected)
}

fn sse_event(event: GatewayEventType, data: serde_json::Value) -> Event {
    Event::default()
        .event(event.as_str())
        .data(data.to_string())
}

fn raw_event(
    event_type: GatewayEventType,
    key: &str,
    value: serde_json::Value,
) -> (
    GatewayEventType,
    GatewayOutboundMessageType,
    String,
    HashMap<String, serde_json::Value>,
) {
    (
        event_type,
        GatewayOutboundMessageType::Raw,
        String::new(),
        HashMap::from([(key.to_owned(), value)]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_matching_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Bearer secret".parse().unwrap());

        assert!(is_authorized(&headers, "secret"));
        assert!(!is_authorized(&headers, "wrong"));
    }

    #[test]
    fn profile_id_is_required() {
        assert!(serde_json::from_str::<ChatRequest>(r#"{"message":"hello"}"#).is_err());
        assert!(
            serde_json::from_str::<ChatRequest>(r#"{"message":"hello","profile_id":""}"#).is_ok()
        );
    }

    #[test]
    fn parses_base64_attachments() {
        let request: ChatRequest = serde_json::from_str(
            r#"{
                "message":"look",
                "profile_id":"alice",
                "attachments":[{"content_type":"image/png","data":"AQID"}]
            }"#,
        )
        .unwrap();

        assert_eq!(request.attachments[0].data, Some(vec![1, 2, 3]));
    }
}
