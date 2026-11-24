use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use log::{info, warn};
use rig::{
    agent::{AgentBuilder, MultiTurnStreamItem},
    catalog::ModelSpec,
    providers::openai::{OpenAI, OpenAIConfig},
    streaming::{Item, StreamEvent},
};
use rig_memory::{
    DirStore, SummaryState, apply_summary, estimate_tokens, load, prepare_compaction, save,
    summary_request, summary_text, turn_prefix_request,
};

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
    compaction: Option<CompactionPolicy>,
}

#[derive(Clone, Copy)]
struct CompactionPolicy {
    context_length: u64,
    trigger_tokens: u64,
    keep_recent_tokens: u64,
    max_attempts: usize,
}

impl CompactionPolicy {
    fn from_config(model: &ModelConfig, settings: &AgentConfig) -> Option<Self> {
        let context_length = u64::try_from(model.context_length?).ok()?;
        let trigger_tokens =
            ((context_length as f64) * settings.compression_threshold_percent()) as u64;

        Some(Self {
            context_length,
            trigger_tokens: trigger_tokens.max(1),
            // Retaining half of the trigger budget leaves room for the generated summary and
            // prevents an immediate follow-up compaction for the usual 50% threshold.
            keep_recent_tokens: (trigger_tokens / 2).max(1),
            max_attempts: settings.max_compression_attempts,
        })
    }
}

impl<S: ProfileStore> Clone for RigAgent<S> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            model_name: self.model_name.clone(),
            settings: self.settings.clone(),
            profile_store: self.profile_store.clone(),
            conversation_store: self.conversation_store.clone(),
            compaction: self.compaction,
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
        let compaction = CompactionPolicy::from_config(&model, &settings);
        if compaction.is_none() {
            warn!("context compression is disabled because no model context length is configured");
        }
        Ok(Self {
            client: OpenAIConfig::new(&model.api_key)
                .with_base_url(&model.base_url)
                .client(),
            model_name: model.model,
            settings,
            profile_store: Arc::new(profile_store),
            conversation_store,
            compaction,
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
            let mut stream = rig_agent
                .prompt(&context)
                .conversation(session_key.clone())
                .stream();
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
                        agent.compact_conversation_async(session_key);
                        yield AgentEvent::Finished(output);
                        return;
                    }
                    _ => {}
                }
            }
        })
    }
}

impl<S: ProfileStore + 'static> RigAgent<S> {
    fn compact_conversation_async(&self, session_key: String) {
        let Some(policy) = self.compaction else {
            return;
        };
        let client = self.client.clone();
        let model_name = self.model_name.clone();
        let store = self.conversation_store.clone();

        tokio::spawn(async move {
            if let Err(error) =
                compact_conversation(client, model_name, store, session_key, policy).await
            {
                warn!("context compression failed: {error:#}");
            }
        });
    }
}

async fn compact_conversation(
    client: OpenAI,
    model_name: String,
    store: DirStore,
    session_key: String,
    policy: CompactionPolicy,
) -> Result<()> {
    let provider = rig::providers::registry::ProviderId::catalog("openai")
        .context("Rig does not recognize the OpenAI provider")?;
    let context_window = u32::try_from(policy.context_length).unwrap_or(u32::MAX);
    let model_spec = ModelSpec::new(provider, &model_name).with_context_window(context_window);

    for attempt in 1..=policy.max_attempts {
        let store_for_load = store.clone();
        let key_for_load = session_key.clone();
        let mut conversation =
            tokio::task::spawn_blocking(move || load(&store_for_load, &key_for_load))
                .await
                .context("join conversation load task")??;
        let tokens = estimate_tokens(conversation.messages());
        if tokens < policy.trigger_tokens {
            return Ok(());
        }
        let Some(prepared) = prepare_compaction(&conversation, policy.keep_recent_tokens, false)
        else {
            return Ok(());
        };

        let model = client.chat(&model_name);
        let request = summary_request(
            &prepared.to_summarize,
            (prepared.previous.as_ref(), ""),
            &model_spec,
            None,
        )?;
        let response = model.call(request).await?;
        let mut summary = summary_text(&response, "conversation summary")?;

        if !prepared.turn_prefix.is_empty() {
            let request = turn_prefix_request(&prepared.turn_prefix, "", &model_spec, None)?;
            let response = model.call(request).await?;
            let prefix = summary_text(&response, "split-turn summary")?;
            summary = rig_memory::merge_split_turn(&summary, &prefix);
        }

        let state = SummaryState {
            summary,
            read_files: prepared.files.read_files,
            modified_files: prepared.files.modified_files,
        };
        if apply_summary(&mut conversation, &prepared.replaces, &state).is_none() {
            continue;
        }

        let store_for_save = store.clone();
        let key_for_save = session_key.clone();
        tokio::task::spawn_blocking(move || {
            save(&store_for_save, &key_for_save, &mut conversation)
        })
        .await
        .context("join conversation save task")??;
        info!(
            "compacted conversation {session_key} after {attempt} attempt(s), estimated tokens: {tokens}"
        );
    }

    Ok(())
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

    #[test]
    fn compaction_policy_uses_the_configured_context_length() {
        let model = ModelConfig {
            api_key: "key".into(),
            base_url: "https://example.com/v1".into(),
            model: "test".into(),
            context_length: Some(1_000),
        };
        let settings = AgentConfig::default();

        let policy = CompactionPolicy::from_config(&model, &settings).unwrap();

        assert_eq!(policy.context_length, 1_000);
        assert_eq!(policy.trigger_tokens, 500);
        assert_eq!(policy.keep_recent_tokens, 250);
        assert_eq!(policy.max_attempts, 3);
    }

    #[test]
    fn compaction_policy_is_disabled_without_a_context_length() {
        let model = ModelConfig {
            api_key: "key".into(),
            base_url: "https://example.com/v1".into(),
            model: "test".into(),
            context_length: None,
        };

        assert!(CompactionPolicy::from_config(&model, &AgentConfig::default()).is_none());
    }
}
