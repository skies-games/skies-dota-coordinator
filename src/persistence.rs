//! On-disk JSON state for bot career stats and outdated lobbies.

use std::collections::{HashMap, VecDeque};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

pub(crate) const BOTS_STATISTICS_PATH: &str = "bots_statistics.json";
pub(crate) const BOTS_STATISTICS_TMP_PATH: &str = "bots_statistics.json.tmp";
pub(crate) const OUTDATED_LOBBIES_PATH: &str = "outdated_lobbies.json";
pub(crate) const OUTDATED_LOBBIES_TMP_PATH: &str = "outdated_lobbies.json.tmp";
/// FIFO cap for closed lobbies remembered across reconnect / coordinator restart.
pub(crate) const OUTDATED_LOBBIES_CAP: usize = 100;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OutdatedLobbyReason {
    Aborted,
    Ended,
}

/// Closed lobby_id → reason, FIFO-capped. `order` is only for eviction; lookup uses `lobbies`.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub(crate) struct OutdatedLobbies {
    #[serde(default)]
    pub(crate) lobbies: HashMap<String, OutdatedLobbyReason>,
    #[serde(default)]
    pub(crate) order: VecDeque<String>,
}

impl OutdatedLobbies {
    pub(crate) fn insert(&mut self, lobby_id: &str, reason: OutdatedLobbyReason) {
        // Duplicate abort/end reports: keep first FIFO position, do not re-push.
        if self.lobbies.contains_key(lobby_id) {
            return;
        }
        while self.order.len() >= OUTDATED_LOBBIES_CAP {
            if let Some(evicted) = self.order.pop_front() {
                self.lobbies.remove(&evicted);
            }
        }
        let lobby_id = lobby_id.to_string();
        self.lobbies.insert(lobby_id.clone(), reason);
        self.order.push_back(lobby_id);
    }

    pub(crate) fn get(&self, lobby_id: &str) -> Option<OutdatedLobbyReason> {
        self.lobbies.get(lobby_id).copied()
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub(crate) struct CareerRecord {
    #[serde(default)]
    pub(crate) wins: u64,
    #[serde(default)]
    pub(crate) losses: u64,
    /// Cumulative play minutes for the active Dota account (reset on account switch).
    #[serde(default)]
    pub(crate) play_time_minutes: f64,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub(crate) struct BotsStatistics {
    /// Last few results — used only for ongoing win-team streak scoring.
    #[serde(default)]
    pub(crate) bots: HashMap<String, Vec<MatchResult>>,
    /// Cumulative career W/L for metrics (survives the streak window trim).
    #[serde(default)]
    pub(crate) career: HashMap<String, CareerRecord>,
    /// Last known Steam/Dota account per bot_number. Stats are wiped on switch.
    #[serde(default)]
    pub(crate) active_dota_ids: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MatchResult {
    Win,
    Lose,
}
pub(crate) async fn load_json_state<T: DeserializeOwned + Default>(path: &str, tmp_path: &str, label: &str) -> T {
    if tokio::fs::try_exists(tmp_path).await.unwrap_or(false) {
        if let Err(e) = tokio::fs::remove_file(tmp_path).await {
            tracing::error!(?e, %label, "Failed to remove tmp file");
        }
    }
    match tokio::fs::read_to_string(path).await {
        Ok(json) => serde_json::from_str(&json).unwrap_or_else(|e| {
            tracing::error!(?e, %label, "Failed to parse JSON");
            T::default()
        }),
        Err(_) => T::default(),
    }
}

pub(crate) async fn save_json_state<T: Serialize>(state: &T, path: &str, tmp_path: &str, label: &str) {
    match serde_json::to_string_pretty(state) {
        Ok(json) => {
            match tokio::fs::File::create(tmp_path).await {
                Ok(mut file) => {
                    if let Err(e) = file.write_all(json.as_bytes()).await {
                        tracing::error!(?e, %label, "Failed to write to tmp file");
                    }
                    if let Err(e) = file.sync_all().await {
                        tracing::error!(?e, %label, "Failed to sync tmp file");
                    }
                    if let Err(e) = tokio::fs::rename(tmp_path, path).await {
                        tracing::error!(?e, %label, "Failed to rename tmp file");
                    }
                }
                Err(e) => {
                    tracing::error!(?e, %label, "Failed to create tmp file");
                }
            }
        }
        Err(e) => {
            tracing::error!(?e, %label, "Failed to convert to JSON");
        }
    }
}

pub(crate) async fn load_bots_statistics() -> BotsStatistics {
    load_json_state(BOTS_STATISTICS_PATH, BOTS_STATISTICS_TMP_PATH, "bots statistics").await
}

pub(crate) async fn load_outdated_lobbies() -> OutdatedLobbies {
    load_json_state(OUTDATED_LOBBIES_PATH, OUTDATED_LOBBIES_TMP_PATH, "outdated lobbies").await
}

pub(crate) async fn save_outdated_lobbies(state: &OutdatedLobbies) {
    save_json_state(state, OUTDATED_LOBBIES_PATH, OUTDATED_LOBBIES_TMP_PATH, "outdated lobbies").await;
}

pub(crate) async fn remember_outdated_lobby(
    outdated_lobbies: &Mutex<OutdatedLobbies>,
    lobby_id: &str,
    reason: OutdatedLobbyReason,
) {
    let mut guard = outdated_lobbies.lock().await;
    guard.insert(lobby_id, reason);
    save_outdated_lobbies(&*guard).await;
}
pub(crate) async fn save_bots_statistics(stats: &BotsStatistics) {
    save_json_state(stats, BOTS_STATISTICS_PATH, BOTS_STATISTICS_TMP_PATH, "bots statistics").await;
}
