use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::secure;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub key_id: String,
    pub app_key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthCache {
    pub account_id: String,
    pub auth_token: String,
    pub api_url: String,
    pub download_url: String,
    pub s3_api_url: String,
    pub recommended_part_size: u64,
    pub absolute_minimum_part_size: u64,
}

fn config_dir() -> Result<PathBuf> {
    let dir = dirs::home_dir()
        .context("Cannot find home directory")?
        .join(".b2storage");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Writes `value` as DPAPI-encrypted `<name>.dat` and removes any plaintext
/// `<name>.json` left by an earlier version.
fn write_secret_in<T: Serialize>(dir: &Path, name: &str, value: &T) -> Result<()> {
    let sealed = secure::protect(&serde_json::to_vec(value)?)?;
    std::fs::write(dir.join(format!("{name}.dat")), sealed)?;
    let _ = std::fs::remove_file(dir.join(format!("{name}.json")));
    Ok(())
}

/// Reads `<name>.dat`. A plaintext `<name>.json` from an earlier version is accepted
/// once, then re-saved encrypted and deleted, so existing users are migrated on first
/// launch rather than asked for their key again.
fn read_secret_in<T: Serialize + DeserializeOwned>(dir: &Path, name: &str) -> Result<T> {
    if let Ok(sealed) = std::fs::read(dir.join(format!("{name}.dat"))) {
        let json = secure::unprotect(&sealed).with_context(|| {
            format!("Cannot decrypt {name}.dat. It was saved by a different Windows user.")
        })?;
        return serde_json::from_slice(&json).with_context(|| format!("Invalid {name}"));
    }

    let legacy = std::fs::read_to_string(dir.join(format!("{name}.json")))
        .context("Not authenticated. Run: b2storage auth")?;
    let value: T = serde_json::from_str(&legacy).with_context(|| format!("Invalid {name}"))?;
    write_secret_in(dir, name, &value)?;
    Ok(value)
}

pub fn save_config(config: &Config) -> Result<()> {
    write_secret_in(&config_dir()?, "config", config)
}

pub fn load_config() -> Result<Config> {
    read_secret_in(&config_dir()?, "config")
}

pub fn save_auth_cache(cache: &AuthCache) -> Result<()> {
    write_secret_in(&config_dir()?, "auth_cache", cache)
}

pub fn load_auth_cache() -> Result<AuthCache> {
    read_secret_in(&config_dir()?, "auth_cache")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("b2storage-test-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample() -> Config {
        Config { key_id: "keyid-abc".into(), app_key: "super-secret-app-key".into() }
    }

    #[test]
    fn saved_file_does_not_contain_the_key() {
        let dir = scratch_dir("plain");
        write_secret_in(&dir, "config", &sample()).unwrap();

        let raw = std::fs::read(dir.join("config.dat")).unwrap();
        let needle = b"super-secret-app-key";
        assert!(!raw.windows(needle.len()).any(|w| w == needle));
        assert!(!dir.join("config.json").exists());
    }

    #[test]
    fn round_trips() {
        let dir = scratch_dir("roundtrip");
        write_secret_in(&dir, "config", &sample()).unwrap();
        let back: Config = read_secret_in(&dir, "config").unwrap();
        assert_eq!(back.key_id, "keyid-abc");
        assert_eq!(back.app_key, "super-secret-app-key");
    }

    #[test]
    fn legacy_plaintext_is_migrated_and_deleted() {
        let dir = scratch_dir("migrate");
        std::fs::write(dir.join("config.json"), serde_json::to_string(&sample()).unwrap()).unwrap();

        let first: Config = read_secret_in(&dir, "config").unwrap();
        assert_eq!(first.app_key, "super-secret-app-key");
        assert!(!dir.join("config.json").exists(), "plaintext copy must be removed");
        assert!(dir.join("config.dat").exists());

        let second: Config = read_secret_in(&dir, "config").unwrap();
        assert_eq!(second.key_id, "keyid-abc");
    }

    #[test]
    fn missing_credentials_report_not_authenticated() {
        let dir = scratch_dir("missing");
        let err = read_secret_in::<Config>(&dir, "config").unwrap_err();
        assert!(err.to_string().contains("Not authenticated"));
    }
}
