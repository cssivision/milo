# milo

`milo` is a channel-agnostic agent runtime powered by a
[Rig](https://rig.rs/) Agent and an OpenAI Chat Completions compatible model
API. Included Gateways are WeCom Bot WebSocket and an authenticated HTTP SSE
endpoint.

```text
WeCom ↔ WeCom Gateway ↔ Gateway Runtime ↔ Rig Agent ↔ Model API
```

The Gateway owns channel protocol, reconnects, access policies, and conversion
between WeCom frames and normalized messages. The Agent owns model inference
and conversation history. They communicate through an in-process, channel
independent interface, so a future HTTP or WebSocket Agent client can replace
the Rig implementation without changing the WeCom adapter.

## Configure

Create a WeCom intelligent Bot and obtain its `bot_id` and `secret`. Copy the
example configuration and fill in every placeholder:

```sh
cp config.toml.example config.toml
```

`config.toml` is the sole source of configuration, including credentials, and
is ignored by Git. Configure channels with `[[gateway]]` entries. A
`type = "wecom"` entry supports `open`, `allowlist`, and `disabled` access
policies for direct messages and group messages. An `allowlist` policy requires
its matching `*_allow_from` list to contain one or more WeCom user IDs.

A `type = "http"` entry requires `listen_addr` and `bearer_token`. Its
`listen_addr` may use any IP address and port, such as `127.0.0.1:3000` or
`0.0.0.0:8080`.

The `[model]` endpoint must implement OpenAI's Chat Completions API
(`POST /v1/chat/completions`). Set `base_url` to the API's versioned base URL,
for example `https://api.openai.com/v1`.

Set `[log].level` to `off`, `error`, `warn`, `info`, `debug`, or `trace`.
The application records lifecycle, connection, authentication, and error events
but does not log credentials or message content.

## Run

```sh
cargo run --release -- -c config.toml
```

## Gateways

Each `[[gateway]]` entry starts one Gateway instance. All Gateway instances
share the same Agent Runtime but preserve their own channel protocol,
credentials, and access policy.

### WeCom Gateway

Create an intelligent Bot in the WeCom management console, enable API long
connection mode, then copy its `bot_id` and `secret` into a
`type = "wecom"` Gateway entry:

```toml
[[gateway]]
type = "wecom"
bot_id = "your_bot_id"
secret = "your_bot_secret"
websocket_url = "wss://openws.work.weixin.qq.com"
dm_policy = "open"
group_policy = "open"
```

Start `milo`, then send the Bot a direct message or mention it in a group. It
authenticates with `aibot_subscribe`, receives `aibot_msg_callback` frames, and
returns incremental responses through `aibot_respond_msg` stream frames. Each
reply reuses one stream ID until its final frame sets `finish` to `true`.
Connection failures are retried with an exponential backoff capped at 30
seconds.

### HTTP SSE Gateway

The HTTP Gateway exposes `GET /health` and an authenticated
`POST /v1/chat/stream` endpoint. Use `curl -N` to avoid buffering the SSE
response:

```sh
curl -N http://127.0.0.1:3000/v1/chat/stream \
  -H 'Authorization: Bearer replace_with_a_long_random_token' \
  -H 'Content-Type: application/json' \
  -d '{"message":"你好","conversation_id":"demo"}'
```

It emits `delta`, `reset`, and `done` SSE events. The final `done` event
contains the complete response text.

The application uses Tokio for the Bot WebSocket, reconnect and heartbeat
tasks, and Rig's HTTP model requests.
