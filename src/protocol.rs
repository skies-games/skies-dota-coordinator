//! Wire protocol: source/event codes and inbound message parsers.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

/*
-----------------------------------
Wire protocol — see PROTOCOL.md for the full reference.
-----------------------------------
*/
pub(crate) const INITIAL_BUF: usize = 4 * 1024;
pub(crate) const MAX_MESSAGE_LEN: usize = 8 * 1024;
pub(crate) enum Source {
    Bot,
    User,
    Unknown
}

impl Source {
    pub(crate) fn from_str(s: &str) -> Self {
        match s {
            "0" => Self::Bot,
            "1" => Self::User,
            _ => Self::Unknown
        }
    }
}

pub(crate) enum BotSourceEvent {
    ReconnectHandshake,
    LobbyFound,
    InGameParameters,
    GameEnded,
    GameAborted,
    InGameEvent,
    ConnectHandshake,
    Unknown
}

impl BotSourceEvent {
    pub(crate) fn from_str(s: &str) -> Self {
        match s {
            "0" => Self::ReconnectHandshake,
            "1" => Self::LobbyFound,
            "2" => Self::InGameParameters,
            "5" => Self::InGameEvent,
            "6" => Self::ConnectHandshake,
            "7" => Self::GameAborted,
            "8" => Self::GameEnded,
            _ => Self::Unknown
        }
    }
}

#[derive(Debug)]
pub(crate) enum ParseError {
    MissingField(&'static str),
    EmptyField(&'static str),
    InvalidField(&'static str),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(name) => write!(f, "missing field `{name}`"),
            Self::EmptyField(name) => write!(f, "empty field `{name}`"),
            Self::InvalidField(name) => write!(f, "invalid field `{name}`"),
        }
    }
}

pub(crate) fn require_field<'a>(
    parts: &mut impl Iterator<Item = &'a str>,
    name: &'static str,
) -> Result<&'a str, ParseError> {
    let value = parts.next().ok_or(ParseError::MissingField(name))?;
    if value.is_empty() {
        return Err(ParseError::EmptyField(name));
    }
    Ok(value)
}

pub(crate) struct ConnectHandshake {
    pub(crate) bot_number: String,
}

impl ConnectHandshake {
    /// Payload: `{bot_number}`
    pub(crate) fn try_parse(payload: &str) -> Result<Self, ParseError> {
        if payload.is_empty() {
            return Err(ParseError::EmptyField("bot_number"));
        }
        Ok(Self { bot_number: payload.to_string() })
    }
}

pub(crate) struct ReconnectHandshake {
    pub(crate) lobby_id: String,
    pub(crate) bot_number: String,
    pub(crate) side: String,
    pub(crate) tcp_writer: Arc<Mutex<OwnedWriteHalf>>
}

impl ReconnectHandshake {
    /// Payload: `{bot_number}:{lobby_id}:{side}`
    pub(crate) fn try_parse(payload: &str, tcp_writer: Arc<Mutex<OwnedWriteHalf>>) -> Result<Self, ParseError> {
        let mut parts = payload.split(':');
        let bot_number = require_field(&mut parts, "bot_number")?.to_string();
        let lobby_id = require_field(&mut parts, "lobby_id")?.to_string();
        let side = require_field(&mut parts, "side")?.to_string();
        if side != "0" && side != "1" {
            return Err(ParseError::InvalidField("side"));
        }
        Ok(Self { lobby_id, bot_number, side, tcp_writer })
    }
}

pub(crate) struct LobbyFound {
    pub(crate) lobby_id: String,
    pub(crate) bot_number: String,
    pub(crate) tcp_writer: Arc<Mutex<OwnedWriteHalf>>
}

impl LobbyFound {
    /// Payload: `{bot_number}:{lobby_id}`
    pub(crate) fn try_parse(payload: &str, tcp_writer: Arc<Mutex<OwnedWriteHalf>>) -> Result<Self, ParseError> {
        let mut parts = payload.split(':');
        let bot_number = require_field(&mut parts, "bot_number")?.to_string();
        let lobby_id = require_field(&mut parts, "lobby_id")?.to_string();
        Ok(Self { lobby_id, bot_number, tcp_writer })
    }
}
pub(crate) struct InGameParameters {
    pub(crate) lobby_id: String,
    pub(crate) bot_number: String,
    pub(crate) side: String,
    pub(crate) dota_id: String,
    pub(crate) tcp_writer: Arc<Mutex<OwnedWriteHalf>>
}

impl InGameParameters {
    /// Payload: `{bot_number}:{lobby_id}:{side}:{dota_id}`
    pub(crate) fn try_parse(payload: &str, tcp_writer: Arc<Mutex<OwnedWriteHalf>>) -> Result<Self, ParseError> {
        let mut parts = payload.split(':');
        let bot_number = require_field(&mut parts, "bot_number")?.to_string();
        let lobby_id = require_field(&mut parts, "lobby_id")?.to_string();
        let side = require_field(&mut parts, "side")?.to_string();
        if side != "0" && side != "1" {
            return Err(ParseError::InvalidField("side"));
        }
        let dota_id = require_field(&mut parts, "dota_id")?.to_string();
        if dota_id.is_empty() {
            return Err(ParseError::InvalidField("dota_id"));
        }
        Ok(Self { lobby_id, bot_number, side, dota_id, tcp_writer })
    }
}

pub(crate) struct GameEnded {
    pub(crate) lobby_id: String,
    pub(crate) bot_number: String,
    pub(crate) dota_id: String,
    pub(crate) match_duration_secs: u64,
}

impl GameEnded {
    /// Payload: `{bot_number}:{lobby_id}:{dota_id}:{match_duration_secs}`
    pub(crate) fn try_parse(payload: &str) -> Result<Self, ParseError> {
        let mut parts = payload.split(':');
        let bot_number = require_field(&mut parts, "bot_number")?.to_string();
        let lobby_id = require_field(&mut parts, "lobby_id")?.to_string();
        let dota_id = require_field(&mut parts, "dota_id")?.to_string();
        let match_duration_secs = require_field(&mut parts, "match_duration_secs")?
            .parse()
            .map_err(|_| ParseError::InvalidField("match_duration_secs"))?;
        Ok(Self { lobby_id, bot_number, dota_id, match_duration_secs })
    }
}

pub(crate) struct GameAborted {
    pub(crate) lobby_id: String,
    pub(crate) bot_number: String,
}

impl GameAborted {
    /// Payload: `{bot_number}:{lobby_id}`
    pub(crate) fn try_parse(payload: &str) -> Result<Self, ParseError> {
        let mut parts = payload.split(':');
        let bot_number = require_field(&mut parts, "bot_number")?.to_string();
        let lobby_id = require_field(&mut parts, "lobby_id")?.to_string();
        Ok(Self { lobby_id, bot_number })
    }
}

pub(crate) struct InGameEvent {
    pub(crate) lobby_id: String,
    pub(crate) bot_number: String,
    pub(crate) event: String
}

impl InGameEvent {
    /// Payload: `{bot_number}:{lobby_id}:{event}`
    pub(crate) fn try_parse(payload: &str) -> Result<Self, ParseError> {
        let mut parts = payload.split(':');
        let bot_number = require_field(&mut parts, "bot_number")?.to_string();
        let lobby_id = require_field(&mut parts, "lobby_id")?.to_string();
        let event = require_field(&mut parts, "event")?.to_string();
        Ok(Self { lobby_id, bot_number, event })
    }
}

pub(crate) enum UserSourceEvent {
    UserCommand,
    Unknown
}

impl UserSourceEvent {
    pub(crate) fn from_str(s: &str) -> Self {
        match s {
            "0" => Self::UserCommand,
            _ => Self::Unknown
        }
    }
}

pub(crate) struct UserCommand {
    pub(crate) command: String,
    pub(crate) affected_bots: Vec<String>,
    pub(crate) bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
}

impl UserCommand {
    /// Payload after event byte: `:{bot1},{bot2},...`
    pub(crate) fn try_parse(
        payload: &str,
        command: String,
        bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>,
    ) -> Result<Self, ParseError> {
        let bots_str = payload.strip_prefix(':').unwrap_or(payload);
        if bots_str.is_empty() {
            return Err(ParseError::EmptyField("affected_bots"));
        }
        let affected_bots: Vec<String> = bots_str
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        if affected_bots.is_empty() {
            return Err(ParseError::EmptyField("affected_bots"));
        }
        Ok(Self { command, affected_bots, bot_clients })
    }
}
