use std::{convert::Infallible, net::SocketAddr, sync::Arc};

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
    gateway::{AgentEvent, Channel, GatewayRuntime, InboundMessage},
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
    conversation_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
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

    let conversation_id = request
        .conversation_id
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| format!("http:{}", Uuid::new_v4()));
    let sender_id = request
        .user_id
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| "http".to_owned());
    info!("HTTP Gateway started a stream: conversation_id={conversation_id}");
    let inbound = InboundMessage {
        channel: Channel::Http,
        sender_id,
        message_id: Uuid::new_v4().to_string(),
        conversation_id,
        text: message.to_owned(),
    };
    let stream = state.gateway.dispatch_stream(inbound).map(|event| {
        let event = match event {
            Ok(AgentEvent::TextDelta(text)) => {
                sse_event("delta", serde_json::json!({ "text": text }))
            }
            Ok(AgentEvent::Reset) => sse_event("reset", serde_json::json!({})),
            Ok(AgentEvent::Finished(text)) => {
                sse_event("done", serde_json::json!({ "text": text }))
            }
            Err(_) => sse_event(
                "done",
                serde_json::json!({
                    "text": "抱歉，暂时无法生成回复，请稍后重试。"
                }),
            ),
        };
        Ok::<Event, Infallible>(event)
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

fn sse_event(event: &str, data: serde_json::Value) -> Event {
    Event::default().event(event).data(data.to_string())
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
}
