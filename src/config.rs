use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

pub fn save_config(config: &Config) -> Result<()> {
    let path = config_dir()?.join("config.json");
    let data = serde_json::to_string_pretty(config)?;
    std::fs::write(&path, data)?;
    Ok(())
}

pub fn load_config() -> Result<Config> {
    let path = config_dir()?.join("config.json");
    let data = std::fs::read_to_string(&path)
        .context("Not authenticated. Run: b2storage auth")?;
    serde_json::from_str(&data).context("Invalid config file")
}

pub fn save_auth_cache(cache: &AuthCache) -> Result<()> {
    let path = config_dir()?.join("auth_cache.json");
    let data = serde_json::to_string_pretty(cache)?;
    std::fs::write(&path, data)?;
    Ok(())
}

pub fn load_auth_cache() -> Result<AuthCache> {
    let path = config_dir()?.join("auth_cache.json");
    let data = std::fs::read_to_string(&path)
        .context("Not authenticated. Run: b2storage auth")?;
    serde_json::from_str(&data).context("Invalid auth cache")
}
