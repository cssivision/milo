use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use futures_util::StreamExt;
use rig::{
    agent::{AgentBuilder, MultiTurnStreamItem},
    providers::openai::OpenAIConfig,
    streaming::{Item, StreamEvent},
};
use tokio::sync::Mutex;

use crate::{
    config::{AgentConfig, ModelConfig},
    gateway::{Agent, AgentEvent, AgentStream, InboundMessage},
};

#[derive(Clone)]
pub struct RigAgent {
    model: ModelConfig,
    settings: AgentConfig,
    histories: Arc<Mutex<HashMap<String, VecDeque<ConversationTurn>>>>,
}

#[derive(Clone)]
struct ConversationTurn {
    user: String,
    assistant: String,
}

impl RigAgent {
    pub fn new(model: ModelConfig, settings: AgentConfig) -> Self {
        Self {
            model,
            settings,
            histories: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn context_for(&self, session_id: &str, message: &str) -> String {
        let histories = self.histories.lock().await;
        let Some(history) = histories.get(session_id) else {
            return message.to_owned();
        };

        let mut context = String::from("Conversation history:\n");
        for turn in history {
            context.push_str("User: ");
            context.push_str(&turn.user);
            context.push_str("\nAssistant: ");
            context.push_str(&turn.assistant);
            context.push('\n');
        }
        context.push_str("User: ");
        context.push_str(message);
        context
    }

    async fn save_turn(&self, session_id: &str, user: &str, assistant: &str) {
        let mut histories = self.histories.lock().await;
        let history = histories.entry(session_id.to_owned()).or_default();
        history.push_back(ConversationTurn {
            user: user.to_owned(),
            assistant: assistant.to_owned(),
        });
        while history.len() > self.settings.max_history_messages {
            history.pop_front();
        }
    }
}

impl Agent for RigAgent {
    fn respond_stream(&self, message: InboundMessage) -> AgentStream {
        let agent = self.clone();
        Box::pin(async_stream::try_stream! {
            let _metadata = (message.channel, &message.sender_id, &message.message_id);
            let context = agent.context_for(&message.conversation_id, &message.text).await;
            let client = OpenAIConfig::new(&agent.model.api_key)
                .with_base_url(&agent.model.base_url)
                .client();
            let rig_agent = AgentBuilder::new(client.chat(&agent.model.model))
                .preamble(&agent.settings.system_prompt)
                .build();
            let mut stream = rig_agent.prompt(&context).stream();
            let mut output = String::new();

            while let Some(item) = stream.next().await {
                match item? {
                    MultiTurnStreamItem::StreamAssistantItem(Item::Event(StreamEvent::Text { text, .. })) => {
                        output.push_str(&text);
                        yield AgentEvent::TextDelta(text);
                    }
                    MultiTurnStreamItem::ModelTurnRetried { .. } => {
                        output.clear();
                        yield AgentEvent::Reset;
                    }
                    MultiTurnStreamItem::FinalResponse(response) => {
                        let output = response.output();
                        agent.save_turn(&message.conversation_id, &message.text, &output).await;
                        yield AgentEvent::Finished(output);
                        return;
                    }
                    _ => {}
                }
            }
        })
    }
}
