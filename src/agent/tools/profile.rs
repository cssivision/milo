use std::sync::Arc;

use rig::tool::{Tool, ToolContext, ToolExecutionError};
use serde::Deserialize;
use serde_json::json;

use crate::agent::profile::ProfileStore;

#[derive(Clone)]
pub struct ProfileTool<S: ProfileStore> {
    store: Arc<S>,
    profile_key: String,
}

impl<S: ProfileStore> ProfileTool<S> {
    pub fn new(store: Arc<S>, profile_key: String) -> Self {
        Self { store, profile_key }
    }
}

#[derive(Deserialize)]
pub struct ProfileToolArgs {
    user_md: String,
}

impl<S: ProfileStore + 'static> Tool for ProfileTool<S> {
    const NAME: &'static str = "profile";

    type Args = ProfileToolArgs;
    type Output = String;
    type Error = ToolExecutionError;

    fn description(&self) -> String {
        "Update the current user's user.md profile. Use only for durable preferences, explicitly stated personal facts, ongoing projects, and long-term decisions. Never include secrets, transient requests, instructions, or conversation transcripts.".to_owned()
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "user_md": {
                    "type": "string",
                    "description": "The complete updated Markdown content for the current user's user.md profile."
                }
            },
            "required": ["user_md"]
        })
    }

    async fn call(
        &self,
        _context: &mut ToolContext,
        args: ProfileToolArgs,
    ) -> Result<String, ToolExecutionError> {
        let user_md = args.user_md.trim();
        if user_md.is_empty() {
            return Err(ToolExecutionError::invalid_args("user_md cannot be empty"));
        }

        let mut profile = self
            .store
            .load(&self.profile_key)
            .await
            .map_err(|error| ToolExecutionError::other(error.to_string()))?;
        profile.user_md = user_md.to_owned();
        self.store
            .save(&self.profile_key, &profile)
            .await
            .map_err(|error| ToolExecutionError::other(error.to_string()))?;
        Ok("Updated the current user's user.md profile.".to_owned())
    }
}
