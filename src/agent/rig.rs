use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use futures_util::StreamExt;
use rig::{
    agent::{AgentBuilder, MultiTurnStreamItem},
    providers::openai::{OpenAI, OpenAIConfig},
    streaming::{Item, StreamEvent},
};
use rig_memory::DirStore;

use crate::{
    agent::{
        profile::{Profile, ProfileStore},
        tools::ProfileTool,
    },
    config::{AgentConfig, ModelConfig},
    gateway::{Agent, AgentEvent, AgentStream, Channel, InboundMessage},
};

pub struct RigAgent<S: ProfileStore> {
    client: OpenAI,
    model_name: String,
    settings: AgentConfig,
    profile_store: Arc<S>,
    conversation_store: DirStore,
}

impl<S: ProfileStore> Clone for RigAgent<S> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            model_name: self.model_name.clone(),
            settings: self.settings.clone(),
            profile_store: self.profile_store.clone(),
            conversation_store: self.conversation_store.clone(),
        }
    }
}

fn session_key(channel: Channel, sender_id: &str, conversation_id: &str) -> String {
    format!("{channel:?}:{sender_id}:{conversation_id}")
}

fn profile_key(channel: Channel, sender_id: &str) -> String {
    format!("{channel:?}:{sender_id}")
}

impl<S: ProfileStore + 'static> RigAgent<S> {
    pub fn new(
        model: ModelConfig,
        settings: AgentConfig,
        profile_store: S,
        storage_dir: PathBuf,
    ) -> Result<Self> {
        let conversation_store = DirStore::open(storage_dir.join("sessions"))?;
        Ok(Self {
            client: OpenAIConfig::new(&model.api_key)
                .with_base_url(&model.base_url)
                .client(),
            model_name: model.model,
            settings,
            profile_store: Arc::new(profile_store),
            conversation_store,
        })
    }

    fn system_prompt(&self, profile: &Profile) -> String {
        let mut prompt = self.settings.system_prompt.clone();
        if !profile.soul_md.is_empty() {
            prompt.push_str("\n\nSoul:\n");
            prompt.push_str(&profile.soul_md);
        }
        if !profile.user_md.is_empty() {
            prompt.push_str("\n\nUser profile:\n");
            prompt.push_str(&profile.user_md);
        }
        prompt
    }

    async fn context_for(&self, profile_key: &str, message: &str) -> Result<(String, String)> {
        let profile = self.profile_store.load(profile_key).await?;
        Ok((message.to_owned(), self.system_prompt(&profile)))
    }
}

impl<S: ProfileStore + 'static> Agent for RigAgent<S> {
    fn respond_stream(&self, message: InboundMessage) -> AgentStream {
        let agent = self.clone();
        Box::pin(async_stream::try_stream! {
            let _metadata = (message.channel, &message.sender_id, &message.message_id);
            let session_key =
                session_key(message.channel, &message.sender_id, &message.conversation_id);
            let profile_key = profile_key(message.channel, &message.sender_id);
            let (context, system_prompt) = agent.context_for(&profile_key, &message.text).await?;
            let rig_agent = AgentBuilder::new(agent.client.chat(&agent.model_name))
                .name("milo")
                .preamble(system_prompt)
                .memory(agent.conversation_store.clone())
                .tool(ProfileTool::new(
                    agent.profile_store.clone(),
                    profile_key,
                ))
                .default_max_turns(2)
                .build();
            let mut stream = rig_agent.prompt(&context).conversation(session_key).stream();
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
                        yield AgentEvent::Finished(output);
                        return;
                    }
                    _ => {}
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_key_is_user_scoped() {
        assert_ne!(
            session_key(Channel::Http, "alice", "shared"),
            session_key(Channel::Http, "bob", "shared")
        );
    }
}
