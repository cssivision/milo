use std::{
    fs,
    io::{BufReader, Read},
    path::PathBuf,
};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Default)]
pub struct Profile {
    pub user_md: String,
    pub soul_md: String,
}

#[async_trait::async_trait]
pub trait ProfileStore: Send + Sync {
    async fn load(&self, user_id: &str) -> Result<Profile>;
    async fn save(&self, user_id: &str, profile: &Profile) -> Result<()>;
}

#[derive(Clone)]
pub struct FileProfileStore {
    root: PathBuf,
}

impl FileProfileStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn load(&self, user_id: &str) -> Result<Profile> {
        let directory = self.root.join(file_name(user_id));
        Ok(Profile {
            user_md: self.load_markdown(&directory, "user.md")?,
            soul_md: self.load_markdown(&directory, "soul.md")?,
        })
    }

    pub fn save(&self, user_id: &str, profile: &Profile) -> Result<()> {
        let directory = self.root.join(file_name(user_id));
        fs::create_dir_all(&directory)
            .with_context(|| format!("create profile directory {}", directory.display()))?;
        fs::write(directory.join("user.md"), &profile.user_md)
            .with_context(|| format!("write user profile {}", directory.display()))?;
        fs::write(directory.join("soul.md"), &profile.soul_md)
            .with_context(|| format!("write soul profile {}", directory.display()))
    }

    fn load_markdown(&self, directory: &std::path::Path, name: &str) -> Result<String> {
        fs::create_dir_all(directory)
            .with_context(|| format!("create profile directory {}", directory.display()))?;
        let path = directory.join(name);
        match fs::File::open(&path) {
            Ok(file) => {
                let mut content = String::new();
                BufReader::new(file)
                    .read_to_string(&mut content)
                    .with_context(|| format!("read profile file {}", path.display()))?;
                Ok(content)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::write(&path, "")
                    .with_context(|| format!("create profile file {}", path.display()))?;
                Ok(String::new())
            }
            Err(error) => {
                Err(error).with_context(|| format!("read profile file {}", path.display()))
            }
        }
    }
}

#[async_trait::async_trait]
impl ProfileStore for FileProfileStore {
    async fn load(&self, user_id: &str) -> Result<Profile> {
        FileProfileStore::load(self, user_id)
    }

    async fn save(&self, user_id: &str, profile: &Profile) -> Result<()> {
        FileProfileStore::save(self, user_id, profile)
    }
}

fn file_name(user_id: &str) -> String {
    Sha256::digest(user_id.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn creates_and_updates_user_profile() {
        let root = std::env::temp_dir().join(format!("milo-profile-test-{}", Uuid::new_v4()));
        let store = FileProfileStore::new(root.clone());
        let mut profile = store.load("http:alice").unwrap();
        profile.user_md = "- Prefers concise replies.".into();
        store.save("http:alice", &profile).unwrap();

        assert_eq!(
            store.load("http:alice").unwrap().user_md,
            "- Prefers concise replies."
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
